# PySpark Pipeline Design

## Why line-level

The original SQL pipeline keeps one Silver row per `InvoiceNo + StockCode`
(`ROW_NUMBER() ... rn = 1`). In the UCI Online Retail data that key is **not
unique**: 9,694 keys repeat across 20,378 rows, mostly with different
quantities or prices (the same item scanned several times in one basket).
Collapsing them removes 10,684 rows (net revenue 64,416.01), and when the
duplicates share an ingestion timestamp the surviving row is arbitrary.
Exact duplicate lines (5,268 rows) are interleaved through invoices in the same
way as legitimate repeats, and the source has no metadata that tells them
apart, so they are kept and flagged, not removed.
Details and numbers: [findings_and_parity.md](findings_and_parity.md).

## Row identity

| Column | Meaning |
|---|---|
| `file_content_hash` | SHA-256 of the file bytes |
| `source_row_number` | Physical line index in the file (header = 0, first data line = 1) |
| `bronze_row_id` | `sha2(file_content_hash || ':' || source_row_number)`; unique by construction |

`source_row_number` is computed **once, at ingestion**, from the file content:
the file is read as one binary value, split on line breaks and exploded with
`posexplode`, so the array position is the line index. It does not depend on
cluster size or partitioning, and works under Spark Connect / serverless (no RDD
API). `monotonically_increasing_id()` is not used: its values depend on
partition layout and are not row numbers (a test enforces this).
Multi-line quoted CSV fields are rejected rather than mis-numbered.

`bronze_row_id` is the **only** MERGE key used in Silver.

## Batches and manifests

Every CSV needs a sidecar manifest `<file>.csv.manifest.json`:

```json
{"batch_type": "CORRECTION", "batch_sequence": 6, "expected_row_count": 5, "source_system": "retail_sales"}
```

* `batch_type`: `NEW_DATA` or `CORRECTION`. Anything else (or a missing manifest) fails validation.
* `batch_sequence`: positive integer, unique across all batches. It defines version order,
  independently of arrival order.
* `expected_row_count`: must equal the parsed data rows (catches truncated files).

Bronze validates **all** landed files first and ingests nothing if any file fails:

| Situation | Result |
|---|---|
| Same name, same bytes, same manifest | Replay: skipped (no-op) |
| Same name, different bytes | Stop: immutable file changed, review required |
| Same name, different manifest | Stop: review required |
| Same bytes under a new name | Stop: would double count |
| `batch_sequence` already used | Stop |
| Bad/missing manifest, bad header, row-count mismatch, embedded newline | `VALIDATION_FAILED` recorded; file may be re-delivered after fixing |

## Silver rules

Let `s` be the batch sequence and `K = (InvoiceNo, StockCode)`.
"Newest applied correction of K" = highest sequence of a CORRECTION whose lines for K
are CURRENT or SUPERSEDED.

| Batch | Condition for key K | Lines of this batch | Existing CURRENT lines of K |
|---|---|---|---|
| NEW_DATA | no applied correction newer than `s` | CURRENT | unchanged (never superseded) |
| NEW_DATA | applied correction newer than `s` | STALE_ON_ARRIVAL | unchanged |
| CORRECTION | any invalid row for K | HELD_REJECTED_CORRECTION + OPEN rejection | unchanged |
| CORRECTION | applied correction newer than `s` | STALE_ON_ARRIVAL | unchanged |
| CORRECTION | otherwise | CURRENT | lines with sequence `< s` become SUPERSEDED |

* A CORRECTION must contain the **complete replacement set** for every key it touches.
* NEW_DATA lines newer than a correction are kept (the correction only restates K as of `s`).
* A correction row without InvoiceNo/StockCode cannot be attributed, so the whole batch is held.
* Nothing is deleted: superseded and held lines stay in Silver with their status.
* Invalid rows go to `silver_quarantine` with their reasons (see `quality.py`).
* Batches are applied in `batch_sequence` order; each batch is one atomic MERGE.

## Statuses and watermark

```
VALIDATION_FAILED   (never ingested)
INGESTED -> SILVER_APPLIED -> GOLD_PUBLISHED -> COMMITTED
                                            \-> BLOCKED_REVIEW (open correction rejection)
```

* Each stage reads the statuses of the previous one, so a rerun continues from where a failure stopped.
* Gold rebuilds from CURRENT Silver lines and validates everything before writing.
  Batches stay `SILVER_APPLIED` until all four Gold tables are written.
* The commit step runs last. `committed_through_sequence` = highest `S` such that every accepted
  batch with sequence `<= S` is COMMITTED. A BLOCKED_REVIEW batch therefore holds the watermark,
  and a late blocked batch moves it back (the previous value is kept for audit).
* While any rejection is OPEN, the commit step raises (the Databricks task fails) on every run.
  A rejection is resolved by a newer valid CORRECTION of the same key, or manually with
  `commit.resolve_rejection(ctx, rejection_id, note)` (a note is required).

## Replay and recovery

| Failure point | Why a rerun is safe |
|---|---|
| After Bronze write, before registry update | Bronze MERGE is insert-only on `bronze_row_id` |
| Inside Silver, before status update | Quarantine/rejections are insert-only; Silver MERGE inserts are no-ops and supersession only touches CURRENT lines with a lower sequence |
| Part-way through Gold writes | Batches still SILVER_APPLIED; rerun rewrites all four tables |
| Before/after commit | Watermark is recomputed from statuses |

## Tables (`workspace.retail_pipeline_pyspark`)

`batch_registry`, `bronze_sales`, `silver_sales`, `silver_quarantine`,
`correction_rejections`, `pipeline_watermark`, `gold_kpis`, `gold_monthly_sales`,
`gold_sales_by_country`, `gold_top_products`, `gold_top_merchandise`, `gold_revenue_by_class`.

The Delta store refuses `workspace.default` and `workspace.retail_pipeline_test`
and only writes table names it knows.

## Known limits

* A correction cannot delete a key (there is no "zero lines" representation in the CSV).
* Multi-line quoted CSV fields are rejected.
* Bronze reads one file into a single value before splitting: fine for files of tens of MB
  (the 48 MB UCI file runs end-to-end in ~100 s locally); very large files would need a
  different row-numbering approach.
