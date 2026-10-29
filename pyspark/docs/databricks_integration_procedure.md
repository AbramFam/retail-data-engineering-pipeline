# Databricks Integration Procedure

Every phase lists whether it writes. **Phases marked WRITE need a separate, explicit approval
before they run.** Nothing in this procedure touches `workspace.default` or
`workspace.retail_pipeline_test`; Phase 1 records their state so Phase 7 can prove it.

Prerequisite: GitHub Actions green on `pyspark-implementation` (Parquet **and** Delta jobs).

**Status (2026-10-29):** Phases 1, 2, 3, 4, 5 and 7 done and passed.
Results: [findings_and_parity.md](findings_and_parity.md#databricks-integration-results-level-d).
Phase 3 ran [`scripts/integration_scenario.py`](../scripts/integration_scenario.py); Phases 4 and 7 ran the equivalent
checks from one notebook cell (same expected values as the tables below).

---

## Phase 1: read-only checks (no writes)

### 1a. Baseline of existing tables (SQL editor)

```sql
SHOW SCHEMAS IN workspace;                                  -- expect: no retail_pipeline_pyspark yet
SHOW TABLES IN workspace.default;
SHOW TABLES IN workspace.retail_pipeline_test;

-- Record version + row count of every existing table you care about, e.g.:
SELECT 'default.silver_sales_v2' t, COUNT(*) n FROM workspace.default.silver_sales_v2
UNION ALL SELECT 'default.gold_kpis_v2', COUNT(*) FROM workspace.default.gold_kpis_v2
UNION ALL SELECT 'test.bronze_sales_v2', COUNT(*) FROM workspace.retail_pipeline_test.bronze_sales_v2
UNION ALL SELECT 'test.silver_sales_v2', COUNT(*) FROM workspace.retail_pipeline_test.silver_sales_v2
UNION ALL SELECT 'test.gold_kpis_v2', COUNT(*) FROM workspace.retail_pipeline_test.gold_kpis_v2;

DESCRIBE HISTORY workspace.retail_pipeline_test.gold_kpis_v2 LIMIT 1;   -- note the version number
DESCRIBE HISTORY workspace.default.silver_sales_v2 LIMIT 1;
```

Save the output (screenshot or copy). Phase 7 compares against it.

### 1b. Findings to confirm on the real tables

```sql
-- Rows the SQL key dedup would remove (local public copy: 10684)
SELECT SUM(cnt - 1) FROM (SELECT COUNT(*) cnt FROM workspace.retail_pipeline_test.bronze_sales_v2
                          GROUP BY InvoiceNo, StockCode HAVING COUNT(*) > 1);
-- State of the legacy PySpark Silver (unknown after out-of-order runs)
SELECT COUNT(*) total, SUM(CASE WHEN CustomerID LIKE '%.0' THEN 1 ELSE 0 END) with_dot0
FROM workspace.retail_pipeline_test.silver_sales_v2;
-- Do batch_006 update keys exist in the SQL base data?
SELECT InvoiceNo, StockCode, COUNT(*) FROM workspace.default.silver_sales_v2
WHERE (InvoiceNo, StockCode) IN (('600002','71053'), ('600004','84029G')) GROUP BY ALL;
```

### 1c. Code and environment check (notebook in the Git folder, read-only)

Add the repo as a Git folder on branch `pyspark-implementation`, then in a scratch notebook
next to `pyspark/notebooks/`:

```python
import os, sys; sys.path.insert(0, os.path.abspath(".."))
import pyspark, retail_pipeline
from delta.tables import DeltaTable                       # must import
print(pyspark.__version__, spark.conf.get("spark.sql.ansi.enabled"))

from retail_pipeline.storage import DeltaStore, StorageSafetyError
for s in ["workspace.default", "workspace.retail_pipeline_test"]:
    try: DeltaStore(spark, s); print("GUARD FAILED", s)
    except StorageSafetyError: print("guard ok:", s)

# Deterministic row numbering on the REAL file (reads only; no table is created)
from retail_pipeline import bronze
from retail_pipeline.context import PipelineContext
ctx = PipelineContext(spark, DeltaStore(spark, "workspace.retail_pipeline_pyspark"))  # constructing writes nothing
path = "/Volumes/workspace/retail_pipeline_test/retail_raw_files/online_retail.csv"
src = bronze.read_source_file(ctx, path)
print(src.row_count, src.file_hash)                        # expect 541909
r = src.rows.agg({"source_row_number": "max"}).first()[0]; print("max row number", r)
src2 = bronze.read_source_file(ctx, path)
print("same hash on re-read:", src2.file_hash == src.file_hash)
```

**Stop here and report:** versions, guard output, row count, hash.

---

## Phase 2: WRITE (approval #1): create the isolated schemas

Run `pyspark/notebooks/10_pyspark_setup.py` twice:

| Run | widget `target_schema` | widget `landing_dir` |
|---|---|---|
| 1 | `workspace.retail_pipeline_pyspark` | `/Volumes/workspace/retail_pipeline_pyspark/landing/incoming` |
| 2 | `workspace.retail_pipeline_pyspark_it` | `/Volumes/workspace/retail_pipeline_pyspark_it/landing/incoming` |

Check: each schema has 10 empty tables and a `landing` Volume. Nothing else changed.

## Phase 3: WRITE (approval #2): synthetic integration scenarios in `_it`

All in `workspace.retail_pipeline_pyspark_it`, run tasks 11 → 14 manually after each step.
Upload files with the Catalog UI into `.../retail_pipeline_pyspark_it/landing/incoming/`.

| Step | Files to land | Expected after 14 |
|---|---|---|
| 1 | `b1.csv` NEW_DATA seq 1 (3 lines, two with the same key) | 3 CURRENT lines, COMMITTED, watermark 1 |
| 2 | nothing new, rerun 11→14 | no change anywhere (replay) |
| 3 | `c2.csv` CORRECTION seq 2 restating one key | old lines SUPERSEDED, watermark 2 |
| 4 | `c3.csv` CORRECTION seq 3 with one invalid row | task 14 **fails**, `c3` BLOCKED_REVIEW, watermark stays 2 |
| 5 | `c4.csv` CORRECTION seq 4 fixing that key | rejection RESOLVED, all COMMITTED, watermark 4 |
| 6 | `b1.csv` again with edited content | task 11 **fails** (immutable file), nothing ingested |

I will provide the six small files and manifests for this phase.

## Phase 4: WRITE (approval #3): real data in `workspace.retail_pipeline_pyspark`

1. Copy (not move) the source file; the original stays where it is:
   ```python
   dbutils.fs.cp("/Volumes/workspace/retail_pipeline_test/retail_raw_files/online_retail.csv",
                 "/Volumes/workspace/retail_pipeline_pyspark/landing/incoming/online_retail.csv")
   ```
2. Upload `pyspark/examples/online_retail.csv.manifest.json` next to it (check `expected_row_count` = Phase 1c count).
3. Run 11 → 14 one at a time and check:

| After | Expected (from the local public-copy run) |
|---|---|
| 11 | registry: 1 row INGESTED; `bronze_sales` 541,909 rows, 541,909 distinct `bronze_row_id` |
| 12 | `silver_sales` 541,909 CURRENT; `silver_quarantine` 0; `is_exact_repeat` 5,268 |
| 13 | `gold_kpis`: 10,655,622.48 / 896,812.49 / 9,758,809.99 / 4,372 / 20,725 / 470.87 (= Notebook 08) |
| 14 | status COMMITTED; watermark 1; task succeeds |

Any difference is a finding to investigate before going further.

## Phase 5: WRITE (approval #4): Job

Create Job `retail_pyspark_pipeline`: tasks 11 → 14 (each depends on the previous succeeding),
parameter `run_id = {{job.run_id}}`, widgets pointing at the main schema. Trigger: manual first;
file-arrival trigger on the landing path only after a manual Job run succeeds.

## Phase 6: CI-to-Databricks record

Fill in the "Databricks integration" level in `findings_and_parity.md` with dates, run ids and
the measured numbers (separately from local results).

## Phase 7: prove existing work is untouched (read-only)

Rerun the Phase 1a queries. Row counts and `DESCRIBE HISTORY` versions of every table in
`workspace.default` and `workspace.retail_pipeline_test` must equal the Phase 1 baseline.

## Rollback (WRITE, only on request)

Affects only the new schemas:
```sql
DROP SCHEMA workspace.retail_pipeline_pyspark_it CASCADE;
DROP SCHEMA workspace.retail_pipeline_pyspark CASCADE;
```
