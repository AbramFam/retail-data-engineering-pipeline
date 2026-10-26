# Databricks notebook source
# MAGIC %md
# MAGIC # 13 - Gold (Notebook 08 rules)
# MAGIC Rebuilds the four Gold tables from CURRENT Silver lines after validating them.
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

from retail_pipeline import gold

print("published:", gold.run(ctx))
display(ctx.store.read("gold_kpis"))
display(ctx.store.read("gold_top_products").orderBy("Rank"))
