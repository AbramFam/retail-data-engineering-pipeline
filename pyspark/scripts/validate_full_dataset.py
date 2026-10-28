"""Run the PySpark pipeline locally on a full CSV and compare with the original SQL logic.

Usage:
    python pyspark/scripts/validate_full_dataset.py --landing <dir with CSV + manifest> --work <scratch dir>

Local only: uses ParquetStore and writes nothing to Databricks. The original
SQL files are read (never modified) and executed with Spark SQL on temporary
views. Note: the SQL Gold filter `CustomerID > 0` raises under ANSI for IDs
like '17850.0', so the SQL comparison normalises those IDs first (reported).
"""
import argparse
import json
import os
import re
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
REPO = os.path.dirname(os.path.dirname(HERE))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from retail_pipeline import pipeline  # noqa: E402
from retail_pipeline.context import PipelineContext  # noqa: E402
from retail_pipeline.storage import ParquetStore  # noqa: E402


def sql_text(name):
    with open(os.path.join(REPO, "sql", name)) as fh:
        return fh.read()


def sql_gold_queries():
    out = {}
    for stmt in sql_text("03_gold_tables.sql").split(";"):
        m = re.search(r"CREATE\s+OR\s+REPLACE\s+TABLE\s+(\w+)\s+AS(.*)", stmt, re.S | re.I)
        if m:
            out[m.group(1)] = m.group(2)
    return out


def revenue(df):
    return df.agg(F.sum(F.col("Quantity") * F.col("UnitPrice"))).first()[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--json-out")
    args = ap.parse_args()

    spark = (SparkSession.builder.master("local[*]").appName("full-dataset-validation")
             .config("spark.driver.memory", "6g").config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    store = ParquetStore(spark, os.path.join(args.work, "tables"))
    ctx = PipelineContext(spark, store, fail_on_open_issues=False)

    t0 = time.time()
    run = pipeline.run_all(ctx, args.landing)
    elapsed = round(time.time() - t0, 1)

    bronze = store.read("bronze_sales")
    silver = store.read("silver_sales")
    quarantine = store.read("silver_quarantine")
    k = store.read("gold_kpis").first().asDict()
    report = {
        "run": run, "elapsed_seconds": elapsed,
        "bronze_rows": bronze.count(),
        "distinct_bronze_row_ids": bronze.select("bronze_row_id").distinct().count(),
        "silver_rows_by_status": {r["line_status"]: r["count"] for r in silver.groupBy("line_status").count().collect()},
        "quarantine_by_reason": {r["dq_reasons"]: r["count"] for r in quarantine.groupBy("dq_reasons").count().collect()},
        "exact_repeat_lines": silver.filter("is_exact_repeat").count(),
        "exact_repeat_revenue": str(revenue(silver.filter("is_exact_repeat"))),
        "zero_price_lines": silver.filter("is_zero_price").count(),
        "invalid_customer_lines": silver.filter("NOT is_valid_customer").count(),
        "kpis": {c: str(v) for c, v in k.items() if c not in ("gold_run_id", "published_at")},
        "top_products": [r.asDict() for r in store.read("gold_top_products").orderBy("Rank").collect()],
        "top_merchandise": [{k2: str(v) for k2, v in r.asDict().items()}
                            for r in store.read("gold_top_merchandise").orderBy("Rank").collect()],
        "revenue_by_class": {r["ProductClass"]: str(r["GrossRevenue"]) for r in store.read("gold_revenue_by_class").collect()},
        "monthly_rows": store.read("gold_monthly_sales").count(),
        "country_rows": store.read("gold_sales_by_country").count(),
    }
    report["top_products"] = [{k2: str(v) for k2, v in r.items()} for r in report["top_products"]]

    # ---- Original SQL logic on the same Bronze rows ----
    (bronze.select("InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice",
                   "CustomerID", "Country", "source_file", "ingestion_timestamp", "batch_id", "source_system")
     .createOrReplaceTempView("bronze_sales_v2"))
    spark.createDataFrame([("bronze_to_silver_sales", datetime(2026, 1, 1))],
                          "pipeline_name string, last_processed_timestamp timestamp").createOrReplaceTempView("pipeline_watermark")
    src = re.search(r"USING\s*\((.*)\)\s*AS\s+source", sql_text("02_silver_processing.sql"), re.S | re.I).group(1)
    sql_silver = spark.sql(src)
    sql_silver_rows = sql_silver.count()
    report["sql_silver_rows"] = sql_silver_rows
    report["sql_silver_revenue"] = str(revenue(sql_silver))
    report["pyspark_current_revenue_incl_adjustments"] = str(revenue(silver.filter("line_status = 'CURRENT'")))

    sql_silver.withColumn("CustomerID", F.regexp_replace("CustomerID", r"\.0+$", "")).createOrReplaceTempView("silver_sales_v2")
    report["sql_gold_on_sql_silver"] = {c: str(v) for c, v in spark.sql(sql_gold_queries()["gold_kpis_v2"]).first().asDict().items()}

    (silver.filter("line_status = 'CURRENT'")
     .withColumn("CustomerID", F.when(F.col("is_valid_customer"), F.col("CustomerID")))
     .createOrReplaceTempView("silver_sales_v2"))
    report["sql_gold_on_pyspark_silver"] = {c: str(v) for c, v in spark.sql(sql_gold_queries()["gold_kpis_v2"]).first().asDict().items()}

    text = json.dumps(report, indent=2, default=str)
    print(text)
    if args.json_out:
        with open(args.json_out, "w") as fh:
            fh.write(text)


if __name__ == "__main__":
    main()
