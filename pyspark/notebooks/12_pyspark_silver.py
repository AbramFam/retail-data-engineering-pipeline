# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Silver (line-level, corrections, quarantine)
# MAGIC Applies every INGESTED batch in batch_sequence order.
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

from retail_pipeline import silver

print("applied:", silver.run(ctx))
display(ctx.store.read("silver_sales").groupBy("batch_id", "line_status").count().orderBy("batch_id"))
display(ctx.store.read_or_empty("silver_quarantine").groupBy("batch_id", "dq_reasons").count())
