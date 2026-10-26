# Findings, SQL Parity and Validation Status

## Evidence levels

Results come from three different places. They are never mixed.

| Level | What it covers | Status |
|---|---|---|
| **A. Local automated tests** | `pytest` on **synthetic fixtures**, local Parquet store | **96 passed, 0 failed** (Parquet store; 2026-10-26, pyspark 4.0.1, Java 21) |
| **B. Local Delta tests** | Same scenarios on real local Delta tables | **Not run** (64 Delta variants skipped locally: Delta jars come from Maven, blocked in the build sandbox). Will run in GitHub Actions on push/PR |
| **C. Local full-dataset run** | Whole pipeline on a **public copy** of UCI Online Retail (541,909 rows, matches the notebook outputs), local Parquet store | Done: [`results/local_public_uci_run.json`](results/local_public_uci_run.json) |
| **D. Databricks integration** | Pipeline in `workspace.retail_pipeline_pyspark` on the real Volume file | **Not run.** Requires explicit approval |

The public copy was checked against the legacy notebook outputs before use: 541,909 rows,
4,879 exact-duplicate groups, prefix-5 revenue 10,655,622.48, C revenue −896,812.49,
25,900 invoices, 4,372 customers all match. One difference: 2,515 vs 2,519 zero-price
rows, explained by 4 prices of 0.001 that `decimal(10,2)` rounds to 0.00 in notebook 07.

## Audit of the legacy notebooks (kept unchanged in `../legacy_notebooks/`)

| Notebook | Finding |
|---|---|
| 06 bronze | Appends before its "already processed?" check, so a top-to-bottom rerun duplicates 541,909 rows. Single hard-coded file. `source_file` holds the full path, `batch_id` is constant `initial_load_001`, `source_system` differs from SQL. |
| 07 silver | Append-only: no watermark, no dedup, no MERGE (Silver = Bronze). One date format only. Silent NULL casts. Outputs are from out-of-order execution (`17850.0` still visible after the `.0` strip; the `UPDATE` regex `\\\\\\\\.0$` is over-escaped and matches nothing), so the current state of `retail_pipeline_test.silver_sales_v2` is unknown. |
| 08 gold | Correct and validated, but KPIs only. Its rules became the official PySpark rules. |
| 09 gold final | `SyntaxError` (missing `)` on line 13). Would overwrite the validated `gold_kpis_v2` with `overwriteSchema`. Fails when a batch has no returns (NULL `ReturnsValue`). Top products have no Description. Month stored as a string. |

## Duplicate-key investigation (public UCI copy)

| Measure | Value |
|---|---|
| `InvoiceNo + StockCode` keys | 531,225 |
| Keys appearing more than once | 9,694 (20,378 rows) |
| Rows the SQL `rn = 1` dedup removes | **10,684**, net revenue **64,416.01** (positive revenue 79,668.81) |
| Repeated keys whose lines differ only in Quantity | 4,664 |
| ... in UnitPrice (with or without Quantity) | 526 |
| ... identical in every column | 4,495 |
| Exact duplicate lines (copies of an earlier line) | 5,268, revenue 21,740.98 |
| Exact duplicates adjacent in the file | 731 of 5,268 (median gap 9 lines) |

Example, invoice 536412: StockCode 22077 appears with quantities 1, 2 and 7 at different
positions of the basket. Exact duplicates follow the same interleaved pattern, and no source
column distinguishes a re-scan from an accidental copy, so they are kept and flagged
(`is_exact_repeat`).

## SQL vs PySpark rule differences

| Area | Original SQL | PySpark (official) |
|---|---|---|
| Silver grain | one row per `InvoiceNo + StockCode` | one row per source line (`bronze_row_id`) |
| Updates | implicit: same key in a later batch overwrites | explicit `CORRECTION` batches with sequence versioning |
| Invalid values | `CAST` turns them into NULL silently | quarantined with reasons |
| `A...` invoices (bad-debt adjustments) | included | excluded from Gold, value reported in `AdjustmentsExcludedValue` |
| Gross revenue | lines with `Quantity > 0` | non-cancellation lines |
| Returns | lines with `Quantity < 0` | cancellation (`C...`) lines |
| Total orders / AOV | all distinct invoices (incl. C and A) | distinct non-cancellation invoices with a positive line |
| Customers | `CustomerID > 0` on a STRING | normalised ID (`17850.0` → `17850`), NULL / -1 / non-numeric excluded |
| Unit price precision | `DECIMAL(10,2)` | `DECIMAL(18,4)`; Gold rounds to 2 dp |
| Top products | `StockCode, Description` group | `StockCode` identity, deterministic representative Description |
| Monthly Month | timestamp | timestamp (same) |

These differences are asserted, using the original SQL text, in `tests/test_parity.py`.

### Full-dataset numbers (level C, local, public UCI copy)

| KPI | PySpark (official rules) | SQL rules on PySpark line-level Silver | SQL rules on SQL Silver (dedup) |
|---|---|---|---|
| GrossRevenue | **10,655,622.48** | 10,644,560.42 | 10,564,891.61 |
| ReturnsValue | **896,812.49** | 896,812.49 | 881,559.69 |
| NetRevenue | **9,758,809.99** | 9,747,747.93 | 9,683,331.92 |
| TotalCustomers | **4,372** | 4,372 | 4,372 |
| TotalOrders | **20,725** | 25,900 | 25,900 |
| AOV | **470.87** | 376.36 | 373.87 |

* PySpark column = Notebook 08's validated values exactly.
* Column 1 vs 2: rule differences only (A adjustments −11,062.06 net; orders definition).
* Column 2 vs 3: SQL dedup loss: 64,416.01 net revenue. Because the SQL dedup picks an
  arbitrary row among ties, column 3 may differ slightly between runs.
* To run the SQL Gold on this data, `17850.0`-style IDs were normalised first (next section).
* Silver: 541,909 Bronze rows → 541,909 unique `bronze_row_id`s → 541,909 CURRENT lines; quarantine 0.

## Other findings

1. **Original SQL fails on UCI-formatted customer IDs.** `CustomerID > 0` on a STRING raises
   `CAST_INVALID_INPUT` under ANSI (default in Spark 4 / Databricks SQL) for `'17850.0'` or
   any non-numeric value. The SQL project worked because its base data used integer IDs.
   Covered by `test_original_sql_customer_filter_fails_on_uci_formatted_ids`. SQL not changed.
2. **`batch_006` keys do not exist in the UCI data.** `600002/71053` and `600004/84029G` match
   the separate base data in `workspace.default` (UCI invoices end at 581587). On a UCI base
   all five rows are inserts. The update path is tested with a synthetic base.
3. **Top products include service codes**: `DOT` (postage) is #1, `M` (manual) #8, `POST` #9.
   Neither SQL nor Notebook 08 excludes them. Open business question.
4. **StockCode 23843** ranks #3 from a single 80,995-unit order that was later cancelled
   (gross uses sales lines only, by definition). Open business question.
5. **Sub-cent prices**: 4 rows at 0.001 become 0.00 under `DECIMAL(10,2)`; kept at 4 dp here.

## Open items

* Run level B (CI) and level D (Databricks) after approval.
* Decide on service codes and cancelled-order treatment in Top Products.
* `batch_001`–`batch_005` (the SQL base data) would allow testing `batch_006` on real data.
