"""Gold: business tables from CURRENT Silver lines (Notebook 08 rules).

Official PySpark rules (differences from sql/03_gold_tables.sql are listed in
pyspark/docs/findings_and_parity.md):
  * Invoices starting with 'A' are accounting adjustments -> excluded from all
    Gold figures (their value is reported separately as AdjustmentsExcludedValue).
  * GrossRevenue  = revenue of non-cancellation lines (InvoiceNo not 'C...').
  * ReturnsValue  = -(revenue of cancellation lines 'C...').
  * NetRevenue    = revenue of all non-adjustment lines.
  * TotalOrders   = distinct non-cancellation invoices with a positive quantity line.
  * TotalCustomers= distinct valid CustomerID (NULL, -1, non-numeric excluded).
  * AOV           = NetRevenue / TotalOrders (NULL when there are no orders).
  * Top products  = StockCode identity; gross revenue of positive sales lines;
                    Description = most frequent non-'Unknown Product' description
                    on those lines, ties -> latest InvoiceDate, then alphabetical.
All checks run before any table is written.
"""
from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import registry
from .config import UNKNOWN_PRODUCT, BatchStatus, LineStatus
from .context import PipelineContext

GOLD_TABLES = ["gold_kpis", "gold_monthly_sales", "gold_sales_by_country", "gold_top_products"]


class GoldValidationError(RuntimeError):
    pass


@dataclass
class GoldResult:
    kpis: DataFrame
    monthly: DataFrame
    by_country: DataFrame
    top_products: DataFrame
    totals: dict
    warnings: List[str]


def _rev():
    return F.col("Quantity") * F.col("UnitPrice")


def build(ctx: PipelineContext, silver: DataFrame) -> GoldResult:
    current = silver.filter(F.col("line_status") == LineStatus.CURRENT).withColumn("Revenue", _rev())
    retail = current.filter(~F.col("is_adjustment"))
    zero = F.lit(0).cast("decimal(38,4)")

    t = retail.agg(
        F.coalesce(F.sum(F.when(~F.col("is_cancellation"), F.col("Revenue"))), zero).alias("gross"),
        F.coalesce(F.sum(F.when(F.col("is_cancellation"), F.col("Revenue"))), zero).alias("returns_signed"),
        F.coalesce(F.sum("Revenue"), zero).alias("net"),
        F.countDistinct(F.when(F.col("is_valid_customer"), F.col("CustomerID"))).alias("customers"),
        F.countDistinct(F.when(~F.col("is_cancellation") & (F.col("Quantity") > 0), F.col("InvoiceNo"))).alias("orders"),
        F.sum(F.when(~F.col("is_cancellation") & (F.col("Revenue") < 0), 1).otherwise(0)).alias("neg_sales_lines"),
        F.sum(F.when(F.col("is_cancellation") & (F.col("Revenue") > 0), 1).otherwise(0)).alias("pos_return_lines"),
    ).first().asDict()
    a = current.agg(
        F.coalesce(F.sum(F.when(F.col("is_adjustment"), F.col("Revenue"))), zero).alias("adjustments"),
        F.coalesce(F.sum("Revenue"), zero).alias("all_current"),
    ).first().asDict()
    totals = {**t, **a}

    raw = ctx.spark.createDataFrame([(
        totals["gross"], -totals["returns_signed"], totals["net"], totals["customers"], totals["orders"],
        totals["adjustments"], ctx.run_id, ctx.now,
    )], "GrossRevenue decimal(38,4), ReturnsValue decimal(38,4), NetRevenue decimal(38,4), TotalCustomers long, "
        "TotalOrders long, AdjustmentsExcludedValue decimal(38,4), gold_run_id string, published_at timestamp")
    # Rounded in Spark (HALF_UP, same as Notebook 08 / SQL ROUND), not Python's half-even round().
    kpis = raw.select(
        F.round("GrossRevenue", 2).alias("GrossRevenue"), F.round("ReturnsValue", 2).alias("ReturnsValue"),
        F.round("NetRevenue", 2).alias("NetRevenue"), "TotalCustomers", "TotalOrders",
        F.when(F.col("TotalOrders") > 0, F.round(F.col("NetRevenue") / F.col("TotalOrders"), 2)).alias("AOV"),
        F.round("AdjustmentsExcludedValue", 2).alias("AdjustmentsExcludedValue"), "gold_run_id", "published_at")

    monthly_raw = retail.groupBy(F.date_trunc("month", "InvoiceDate").alias("Month")).agg(F.sum("Revenue").alias("NetRevenue"))
    country_raw = (retail.withColumn("Country", F.coalesce(F.col("Country"), F.lit("Unknown")))
                         .groupBy("Country").agg(F.sum("Revenue").alias("NetRevenue")))
    totals["monthly_sum"] = monthly_raw.agg(F.coalesce(F.sum("NetRevenue"), zero)).first()[0]
    totals["country_sum"] = country_raw.agg(F.coalesce(F.sum("NetRevenue"), zero)).first()[0]
    totals["null_months"] = monthly_raw.filter(F.col("Month").isNull()).count()

    sales = retail.filter(~F.col("is_cancellation") & (F.col("Quantity") > 0) & (F.col("Revenue") > 0))
    revenue = sales.groupBy("StockCode").agg(F.sum("Revenue").alias("GrossRevenue"),
                                            F.sum("Quantity").cast("long").alias("UnitsSold"))
    desc_rank = Window.partitionBy("StockCode").orderBy(F.desc("n"), F.desc("last_seen"), F.asc("Description"))
    descriptions = (sales.filter(F.col("Description").isNotNull() & (F.col("Description") != UNKNOWN_PRODUCT))
                    .groupBy("StockCode", "Description")
                    .agg(F.count("*").alias("n"), F.max("InvoiceDate").alias("last_seen"))
                    .withColumn("r", F.row_number().over(desc_rank)).filter("r = 1")
                    .select("StockCode", "Description"))
    top = (revenue.join(descriptions, "StockCode", "left")
           .withColumn("Description", F.coalesce("Description", F.lit(UNKNOWN_PRODUCT)))
           .withColumn("Rank", F.row_number().over(Window.orderBy(F.desc("GrossRevenue"), F.asc("StockCode"))))
           .filter("Rank <= 10")
           .select("Rank", "StockCode", "Description", F.round("GrossRevenue", 2).alias("GrossRevenue"), "UnitsSold"))

    warnings = []
    if totals["neg_sales_lines"]:
        warnings.append(f"{totals['neg_sales_lines']} non-cancellation lines have negative revenue")
    if totals["pos_return_lines"]:
        warnings.append(f"{totals['pos_return_lines']} cancellation lines have positive revenue")

    return GoldResult(
        kpis=kpis,
        monthly=monthly_raw.select("Month", F.round("NetRevenue", 2).alias("NetRevenue")),
        by_country=country_raw.select("Country", F.round("NetRevenue", 2).alias("NetRevenue")),
        top_products=top, totals=totals, warnings=warnings,
    )


def validate(result: GoldResult) -> None:
    t = result.totals
    errors = []
    gross, returns_value, net = t["gross"], -t["returns_signed"], t["net"]
    if gross - returns_value != net:
        errors.append(f"reconciliation: Gross {gross} - Returns {returns_value} != Net {net}")
    if t["monthly_sum"] != net:
        errors.append(f"monthly totals {t['monthly_sum']} != Net {net}")
    if t["country_sum"] != net:
        errors.append(f"country totals {t['country_sum']} != Net {net}")
    if net + t["adjustments"] != t["all_current"]:
        errors.append(f"Net {net} + adjustments {t['adjustments']} != all CURRENT Silver revenue {t['all_current']}")
    if t["null_months"]:
        errors.append(f"{t['null_months']} monthly rows have a NULL month")
    rows = result.kpis.collect()
    if len(rows) != 1:
        errors.append(f"expected exactly 1 KPI row, got {len(rows)}")
    else:
        k = rows[0]
        for c in ["GrossRevenue", "ReturnsValue", "NetRevenue", "TotalCustomers", "TotalOrders"]:
            if k[c] is None:
                errors.append(f"KPI {c} is NULL")
        if (k["AOV"] is None) != (k["TotalOrders"] == 0):
            errors.append("AOV must be NULL exactly when TotalOrders is 0")
    top = result.top_products
    if top.count() > 10:
        errors.append("more than 10 top products")
    if top.select("StockCode").distinct().count() != top.count():
        errors.append("duplicate StockCode in top products")
    if top.filter(F.col("Description").isNull()).count():
        errors.append("top product without Description")
    if errors:
        raise GoldValidationError("Gold validation failed; no Gold table was written:\n  - " + "\n  - ".join(errors))


def run(ctx: PipelineContext, force: bool = False) -> List[str]:
    pending = [b["batch_id"] for b in registry.batches_with_status(ctx, BatchStatus.SILVER_APPLIED)]
    gold_missing = not all(ctx.store.exists(t) for t in GOLD_TABLES)
    if not pending and not gold_missing and not force:
        return []
    result = build(ctx, ctx.store.read_or_empty("silver_sales"))
    validate(result)
    for w in result.warnings:
        print(f"[gold warning] {w}")
    # Batches stay SILVER_APPLIED until every table is written, so a failure
    # part-way through is repaired by simply re-running this step.
    ctx.store.overwrite("gold_monthly_sales", result.monthly)
    ctx.store.overwrite("gold_sales_by_country", result.by_country)
    ctx.store.overwrite("gold_top_products", result.top_products)
    ctx.store.overwrite("gold_kpis", result.kpis)
    registry.set_status(ctx, pending, BatchStatus.GOLD_PUBLISHED)
    return pending
