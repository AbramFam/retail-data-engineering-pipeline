# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - PySpark pipeline setup (run once)
# MAGIC Creates the isolated schema, the landing Volume and empty pipeline tables. **Requires approval before the first run.**
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

schema = params["target_schema"]
assert schema == "workspace.retail_pipeline_pyspark", f"Unexpected target schema {schema}; edit deliberately if intended"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema}.landing")
os.makedirs(params["landing_dir"], exist_ok=True)
ctx.store.ensure_all()   # CREATE only when missing; never replaces an existing table
display(spark.sql(f"SHOW TABLES IN {schema}"))
