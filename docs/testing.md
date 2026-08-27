# Testing

The pipeline was tested end-to-end in Databricks using an incremental batch
(`data/batch_006.csv`) containing 5 rows: 2 designed to update existing
Silver records and 3 designed to be new inserts, applied against a base
retail dataset already loaded into `silver_sales_v2` at the time of the test.

No screenshots have been added to this repository yet. The **Evidence**
column below is left as "Not yet captured" rather than referencing images
that don't exist. If screenshots are added later under `docs/screenshots/`,
this table will be updated to link to them.

| # | Test | Expected Result | Observed Result | Evidence |
|---|---|---|---|---|
| 1 | File-arrival trigger | Uploading a new CSV to the Volume path automatically starts the Databricks Job. | Job started automatically on upload. | Not yet captured |
| 2 | Five-row incremental batch (`batch_006.csv`) | Bronze contains the 5 new rows; Silver applies the 2 updates and 3 inserts; Gold refreshes its aggregates to reflect the resulting Silver changes. | Batch processed successfully end-to-end. | Not yet captured |
| 3 | Two updates + three inserts | The 2 rows matching existing `InvoiceNo + StockCode` keys update Silver in place; the 3 new-key rows are inserted. | Silver row count and updated fields matched expectations. | Not yet captured |
| 4 | Bronze ingestion validation | `bronze_sales_v2` gains exactly the 5 new rows, with correct `source_file`, `batch_id`, and `ingestion_timestamp`. | Confirmed by row count and metadata check. | Not yet captured |
| 5 | Silver `MERGE` validation | `silver_sales_v2` reflects updated values for the 2 matched keys and contains the 3 new keys. | Confirmed. | Not yet captured |
| 6 | Gold refresh validation | All four Gold tables (`gold_kpis_v2`, `gold_monthly_sales_v2`, `gold_sales_by_country_v2`, `gold_top_products_v2`) reflect the updated Silver data. | Confirmed. | Not yet captured |
| 7 | Watermark advancement after success | `pipeline_watermark.last_processed_timestamp` advances to the new batch's ingestion timestamp only after Bronze, Silver, and Gold all succeed. | Watermark advanced as expected. | Not yet captured |
| 8 | Idempotency (reprocessing test) | Re-running `01_bronze_ingestion.sql` against the same, already-ingested file does not create duplicate Bronze rows. | `COPY INTO` skipped the already-processed file; no duplicates. | Not yet captured |
| 9 | Intentional Gold failure | Deliberately breaking the `gold_tables` task causes it to fail without affecting Bronze/Silver. | Gold task failed as intended. | Not yet captured |
| 10 | Watermark blocked after failure | Because `watermark_update` depends on `gold_tables`, it does not run when Gold fails, and the watermark does not advance. | `watermark_update` task did not execute; watermark unchanged. | Not yet captured |
| 11 | Repair Run | After fixing the Gold task, a Databricks Repair Run reruns only the failed `gold_tables` task and its dependent `watermark_update` task, not Bronze or Silver. | Repair Run completed successfully, rerunning only the two affected tasks. | Not yet captured |
| 12 | Final successful end-to-end run | A separate, subsequent run of all four tasks (`bronze_ingestion` → `silver_processing` → `gold_tables` → `watermark_update`), performed after the failure/Repair Run test, completes with all four tasks succeeding and the watermark reflecting the batch. | Full four-task Job run succeeded end-to-end. | Not yet captured |
| 13 | Setup script validation (`00_setup_tables.sql`, run in `workspace.retail_pipeline_test`) | Running `sql/00_setup_tables.sql` in an empty schema creates `bronze_sales_v2`, `silver_sales_v2`, and `pipeline_watermark`; seeds exactly one `bronze_to_silver_sales` row at `2026-01-01 00:00:00`; rerunning the seed `MERGE` creates no duplicate. | All three tables were created; the seed was correct; rerunning kept the row count at 1. | Not yet captured |

## Notes

- These results are based on manual testing performed directly in the Databricks workspace UI (Job runs, task status, and table queries), not an automated test suite.
- "Observed Result" reflects what was seen during testing at the time; it is not re-verified automatically on every commit.
