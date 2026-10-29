# Retail Pipeline: PySpark Implementation

A line-level PySpark version of the retail medallion pipeline. It runs alongside the
original SQL implementation (`../sql/`), which is unchanged, and writes only to its own
schema, `workspace.retail_pipeline_pyspark`.

* **Design** (row identity, correction batches, statuses, watermark): [docs/design.md](docs/design.md)
* **Findings and SQL parity** (what differs and why, validation status): [docs/findings_and_parity.md](docs/findings_and_parity.md)

## What it does differently from the SQL version

* Keeps **every source line**. The SQL key `InvoiceNo + StockCode` is not unique in the data;
  collapsing it drops 10,684 lines (64,416.01 net revenue).
* Gives each line a stable identity from the file's content hash and its physical line number.
* Accepts changes only through explicit **`CORRECTION`** batches with a version number, so an
  older correction can never overwrite a newer one.
* Quarantines invalid rows with reasons instead of turning them into NULLs.
* Tracks every batch's status and advances the watermark only after Gold succeeds, and never
  past a correction that is waiting for review.
* Uses Notebook 08's Gold rules (accounting adjustments `A...` excluded).

## Layout

```text
pyspark/
├── retail_pipeline/       # the pipeline (bronze, quality, silver, gold, commit, storage)
├── notebooks/             # Databricks tasks 10-14 (source format)
├── tests/                 # pytest suite (synthetic fixtures)
├── scripts/               # validate_full_dataset.py (local full-file run + SQL comparison)
├── examples/              # example manifests
├── docs/                  # design, findings, local run results
└── legacy_notebooks/      # exported notebooks 06-09, unchanged
```

## Input contract

Each CSV (same 8 columns as the SQL pipeline) needs a manifest next to it,
`<file>.csv.manifest.json`:

```json
{"batch_type": "NEW_DATA", "batch_sequence": 1, "expected_row_count": 541909, "source_system": "online_retail_uci"}
```

`batch_type` is `NEW_DATA` or `CORRECTION`. `batch_sequence` is a unique positive integer that
orders versions. A `CORRECTION` must contain the complete set of lines for every
`InvoiceNo + StockCode` it touches. Landed files are immutable. See [examples/](examples/).

## Testing

```bash
cd pyspark
pip install -r requirements-dev.txt
python -m pytest -m "not delta"          # local Parquet store, no downloads needed
RUN_DELTA_TESTS=1 python -m pytest -m delta   # same scenarios on Delta (downloads Delta jars)
```

GitHub Actions (`.github/workflows/pyspark-tests.yml`) runs both on every push and pull request.

The suite covers deterministic row numbering, manifest and immutability checks, replay,
every data-quality rule, line preservation, corrections (including out-of-order arrival,
held keys and resolution), Gold rules against hand-computed values, partial failures and
recovery, watermark blocking, write-safety guards, and parity with the original SQL text.
All test data is synthetic.

Full-file check (local, writes nothing to Databricks):

```bash
python scripts/validate_full_dataset.py --landing <dir with CSV + manifest> --work /tmp/run
```

## Running in Databricks

Validated on 2026-10-28 (Databricks Free Edition, serverless): 16/16 synthetic scenarios and 19/19 full-data checks passed. Results: [docs/findings_and_parity.md](docs/findings_and_parity.md#databricks-integration-results-level-d).

1. Add this repository as a Databricks Git folder on the `pyspark-implementation` branch.
2. Run `notebooks/10_pyspark_setup.py` once. It creates the schema `workspace.retail_pipeline_pyspark`,
   the Volume `landing`, and the empty tables. It never replaces an existing table.
3. Upload the CSV and its manifest to `/Volumes/workspace/retail_pipeline_pyspark/landing/incoming/`.
4. Create the Job (see "Databricks Job" below).
5. If task 14 fails with open correction rejections, review `correction_rejections`, then send a
   newer `CORRECTION` batch or resolve the rejection manually (see the notebook).

The existing tables in `workspace.default` and `workspace.retail_pipeline_test` are never written;
the Delta store refuses those schemas.

## Databricks Job

Definition: [jobs/retail_pyspark_pipeline.job.yml](jobs/retail_pyspark_pipeline.job.yml). To create it in the UI:

1. **Jobs & Pipelines → Create → Job**, name it `retail_pyspark_pipeline`.
2. Task `bronze`: type **Notebook**, source **Workspace**, path `.../retail-data-engineering-pipeline/pyspark/notebooks/11_pyspark_bronze`, compute **Serverless**.
3. Add tasks `silver` (12), `gold` (13) and `commit` (14) the same way, each **Depends on** the previous task.
4. Job **parameters**: `target_schema = workspace.retail_pipeline_pyspark`,
   `landing_dir = /Volumes/workspace/retail_pipeline_pyspark/landing/incoming`, `run_id = {{job.run_id}}`.
5. **Run now**. With no new files it is a replay: nothing changes and every task succeeds.
6. Optional: add a **File arrival** trigger on the landing path, after a manual run succeeds.

The Job runs whatever branch the Git folder is on.
