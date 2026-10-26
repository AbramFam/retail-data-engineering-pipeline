# Databricks notebook source
# MAGIC %md
# MAGIC # 14 - Commit and watermark
# MAGIC Marks batches COMMITTED or BLOCKED_REVIEW and advances the watermark. Fails the task while correction rejections are open.
# MAGIC
# MAGIC Writes only to the isolated schema (default `workspace.retail_pipeline_pyspark`).
# MAGIC The Delta store refuses `workspace.default` and `workspace.retail_pipeline_test`.

# COMMAND ----------

import os
import sys

# This notebook lives in pyspark/notebooks/; the package is in pyspark/retail_pipeline/.
sys.path.insert(0, os.path.abspath(".."))

from retail_pipeline import databricks as dbx

params = dbx.widgets(dbutils)
ctx = dbx.context(spark, params)
print(params, "run_id:", ctx.run_id)

# COMMAND ----------

from retail_pipeline import commit

display(ctx.store.read_or_empty("correction_rejections").orderBy("created_at"))
print(commit.run(ctx))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manual review (only after investigating a rejection)
# MAGIC `commit.resolve_rejection(ctx, "<rejection_id>", "<why this is safe>")`, then re-run this notebook.
