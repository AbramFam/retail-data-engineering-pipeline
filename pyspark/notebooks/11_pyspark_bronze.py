# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Bronze ingestion
# MAGIC Validates every landed CSV and its manifest; ingests only if all files are acceptable.
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

from retail_pipeline import bronze

result = bronze.run(ctx, params["landing_dir"])
print(result)
display(ctx.store.read("batch_registry").orderBy("batch_sequence"))
