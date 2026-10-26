"""SQL vs PySpark parity on a SYNTHETIC fixture.

The original SQL is read from sql/02_silver_processing.sql and
sql/03_gold_tables.sql (read-only, unchanged) and executed with Spark SQL on
temporary views. Every difference asserted here is a documented, intended rule
difference (see pyspark/docs/findings_and_parity.md), not an unexplained gap.
"""
import os
import re
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import functions as F

from conftest import REPO_ROOT, line
from retail_pipeline import bronze, gold, silver

SQL_DIR = os.path.join(REPO_ROOT, "sql")

# Integer-formatted CustomerIDs: the original SQL errors on '17850.0' under ANSI (see last test).
FIXTURE = [
    line("536412", "22077", 1, "1.65", "17920", "6 RIBBONS RUSTIC CHARM", "2010-12-01 11:49:00", "United Kingdom"),
    line("536412", "22077", 2, "1.65", "17920", "6 RIBBONS RUSTIC CHARM", "2010-12-01 11:49:00", "United Kingdom"),
    line("536412", "22077", 7, "1.65", "17920", "6 RIBBONS RUSTIC CHARM", "2010-12-01 11:49:00", "United Kingdom"),
    line("536412", "21448", 2, "1.65", "17920", "12 DAISY PEGS IN WOOD BOX", "2010-12-01 11:49:00", "United Kingdom"),
    line("536412", "21448", 2, "1.65", "17920", "12 DAISY PEGS IN WOOD BOX", "2010-12-01 11:49:00", "United Kingdom"),
    line("536500", "85123A", 6, "2.55", "", "WHITE HANGING HEART", "2011-01-04 10:00:00", "France"),
    line("C536501", "85123A", -2, "2.55", "-1", "WHITE HANGING HEART", "2011-01-05 10:00:00", "France"),
    line("A563185", "B", 1, "11062.06", "", "Adjust bad debt", "2011-08-12 14:50:00", "United Kingdom"),
    line("A563186", "B", 1, "-11062.06", "", "Adjust bad debt", "2011-08-12 14:51:00", "United Kingdom"),
    line("536600", "POST", 1, "18.00", "12583", "POSTAGE", "2011-01-06 10:00:00", "France"),
]


def _read(name):
    with open(os.path.join(SQL_DIR, name)) as fh:
        return fh.read()


def _sql_silver_query():
    text = _read("02_silver_processing.sql")
    m = re.search(r"USING\s*\((.*)\)\s*AS\s+source", text, re.S | re.I)
    assert m, "could not locate the MERGE source query in 02_silver_processing.sql"
    return m.group(1)


def _sql_gold_queries():
    out = {}
    for stmt in _read("03_gold_tables.sql").split(";"):
        m = re.search(r"CREATE\s+OR\s+REPLACE\s+TABLE\s+(\w+)\s+AS(.*)", stmt, re.S | re.I)
        if m:
            out[m.group(1)] = m.group(2)
    assert set(out) == {"gold_kpis_v2", "gold_monthly_sales_v2", "gold_sales_by_country_v2", "gold_top_products_v2"}
    return out


def _run_sql_pipeline(spark, rows):
    cols = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    data = [tuple(None if v == "" else v for v in r) + ("batch_001.csv", datetime(2026, 10, 26), "batch_001", "retail_sales")
            for r in rows]
    spark.createDataFrame(data, ", ".join(f"{c} string" for c in cols)
                          + ", source_file string, ingestion_timestamp timestamp, batch_id string, source_system string"
                          ).createOrReplaceTempView("bronze_sales_v2")
    spark.createDataFrame([("bronze_to_silver_sales", datetime(2026, 1, 1))],
                          "pipeline_name string, last_processed_timestamp timestamp").createOrReplaceTempView("pipeline_watermark")
    sql_silver = spark.sql(_sql_silver_query())
    sql_silver.createOrReplaceTempView("silver_sales_v2")
    return sql_silver, {name: spark.sql(q) for name, q in _sql_gold_queries().items()}


def _run_pyspark(ctx, landing, rows):
    landing.batch("batch_001.csv", "NEW_DATA", 1, rows)
    bronze.run(ctx, landing.dir)
    silver.run(ctx)
    return ctx.store.read("silver_sales"), gold.build(ctx, ctx.store.read("silver_sales"))


def test_silver_row_identity_difference(ctx, spark, landing):
    sql_silver, _ = _run_sql_pipeline(spark, FIXTURE)
    py_silver, _ = _run_pyspark(ctx, landing, FIXTURE)
    assert py_silver.count() == 10                       # every source line kept
    assert sql_silver.count() == 7                       # one row per InvoiceNo+StockCode
    rev = lambda df: float(df.agg(F.sum(F.col("Quantity") * F.col("UnitPrice"))).first()[0])  # noqa: E731
    # 22077 keeps 1 of (1,2,7); 21448 keeps 1 of (2,2): at least 6 units (9.90) lost, depending on which row survives.
    assert rev(py_silver) - rev(sql_silver) >= 1.65 * 6 - 1e-9


def test_gold_definition_differences_are_exactly_the_documented_ones(ctx, spark, landing):
    py_silver, py = _run_pyspark(ctx, landing, FIXTURE)
    # Feed the SAME line-level data to the original SQL Gold to isolate rule differences from dedup.
    (py_silver.filter("line_status = 'CURRENT'")
     .withColumn("CustomerID", F.when(F.col("is_valid_customer"), F.col("CustomerID")))
     .createOrReplaceTempView("silver_sales_v2"))
    sql = {n: spark.sql(q) for n, q in _sql_gold_queries().items()}
    s, p = sql["gold_kpis_v2"].first(), py.kpis.first()

    adj_positive_qty = Decimal("11062.06") + Decimal("-11062.06")   # SQL GrossRevenue includes A lines with Quantity > 0
    adjustments = Decimal("0.00")                                   # A563185 + A563186 net to zero here
    assert s["GrossRevenue"] == p["GrossRevenue"] + adj_positive_qty
    assert s["NetRevenue"] == p["NetRevenue"] + adjustments
    assert s["ReturnsValue"] == p["ReturnsValue"]                    # C lines are exactly the negative-quantity lines here
    assert s["TotalOrders"] == 6 and p["TotalOrders"] == 3           # SQL counts C/A invoices as orders
    assert s["TotalCustomers"] == p["TotalCustomers"] == 2
    assert s["AOV"] != p["AOV"]                                      # different denominators

    sql_top = [r["StockCode"] for r in sql["gold_top_products_v2"].collect()]
    py_top = [r["StockCode"] for r in py.top_products.orderBy("Rank").collect()]
    assert "B" in sql_top and "B" not in py_top                      # adjustment rows rank as a 'product' in SQL
    assert "POST" in sql_top and "POST" in py_top                    # neither excludes service codes (open question)


def test_original_sql_customer_filter_fails_on_uci_formatted_ids(spark):
    """Finding: `CustomerID > 0` on STRING raises under ANSI for '17850.0' (UCI export format)."""
    rows = [line("536365", "85123A", 6, "2.55", "17850.0")]
    _, sql = _run_sql_pipeline(spark, rows)
    with pytest.raises(PySparkException, match="CAST_INVALID_INPUT"):
        sql["gold_kpis_v2"].collect()
