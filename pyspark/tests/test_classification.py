"""StockCode classification (rule v1) and the two additional Gold tables. SYNTHETIC data."""
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from conftest import line
from retail_pipeline import bronze, classification, gold, silver

D = Decimal


@pytest.mark.parametrize("code, cls, review", [
    ("85123A", "PRODUCT", False), ("22423", "PRODUCT", False), (" 22423 ", "PRODUCT", False),
    ("DCGSSBOY", "PRODUCT", True), ("PADS", "PRODUCT", True),
    ("POST", "SHIPPING", False), ("DOT", "SHIPPING", False), ("C2", "SHIPPING", False),
    ("AMAZONFEE", "FEE_OR_CHARGE", False), ("BANK CHARGES", "FEE_OR_CHARGE", False), ("CRUK", "FEE_OR_CHARGE", False),
    ("M", "MANUAL_ADJUSTMENT", False), ("m", "MANUAL_ADJUSTMENT", False), ("D", "MANUAL_ADJUSTMENT", False),
    ("S", "MANUAL_ADJUSTMENT", False), ("B", "ACCOUNTING_ADJUSTMENT", False),
    ("gift_0001_20", "GIFT_VOUCHER", False), ("post", "PRODUCT", True),
])
def test_classification_rule_v1(spark, code, cls, review):
    row = spark.createDataFrame([(code,)], "StockCode string").select(
        classification.product_class().alias("c"), classification.needs_review().alias("r")).first()
    assert (row["c"], row["r"]) == (cls, review)


FIXTURE = [
    line("1001", "DOT", 1, "100.00", desc="DOTCOM POSTAGE"),
    line("1001", "22423", 2, "12.75", desc="REGENCY CAKESTAND 3 TIER"),
    line("1002", "POST", 1, "18.00", desc="POSTAGE"),
    line("1002", "85123A", 4, "2.55", desc="WHITE HANGING HEART"),
    line("1003", "DCGSSBOY", 1, "3.29", desc="BOYS PARTY BAG"),
    line("1004", "M", 1, "40.00", desc="Manual"),
    line("1005", "gift_0001_20", 1, "16.67", desc="Gift Voucher"),
    line("C1006", "22423", -1, "12.75", desc="REGENCY CAKESTAND 3 TIER"),
    line("A1007", "B", 1, "-50.00", desc="Adjust bad debt"),
]


def _build(ctx, landing):
    landing.batch("b1.csv", "NEW_DATA", 1, FIXTURE)
    bronze.run(ctx, landing.dir)
    silver.run(ctx)
    return gold.build(ctx, ctx.store.read("silver_sales"))


def test_top_merchandise_excludes_services_and_flags_review(ctx, landing):
    res = _build(ctx, landing)
    gold.validate(res)
    merch = [(r["Rank"], r["StockCode"], r["GrossRevenue"], r["NeedsReview"])
             for r in res.top_merchandise.orderBy("Rank").collect()]
    assert merch == [(1, "22423", D("25.50"), False), (2, "85123A", D("10.20"), False),
                     (3, "DCGSSBOY", D("3.29"), True)]


def test_existing_top_products_unchanged(ctx, landing):
    res = _build(ctx, landing)
    top = [r["StockCode"] for r in res.top_products.orderBy("Rank").collect()]
    assert top == ["DOT", "M", "22423", "POST", "gift_0001_20", "85123A", "DCGSSBOY"]
    assert "NeedsReview" not in res.top_products.columns


def test_revenue_by_class_reconciles_to_gross(ctx, landing):
    res = _build(ctx, landing)
    by_class = {r["ProductClass"]: (r["GrossRevenue"], r["SalesLines"], r["rule_version"])
                for r in res.revenue_by_class.collect()}
    assert by_class == {
        "PRODUCT": (D("38.99"), 3, "v1"), "SHIPPING": (D("118.00"), 2, "v1"),
        "MANUAL_ADJUSTMENT": (D("40.00"), 1, "v1"), "GIFT_VOUCHER": (D("16.67"), 1, "v1"),
    }
    gross = res.kpis.first()["GrossRevenue"]
    assert sum(v[0] for v in by_class.values()) == gross == D("213.66")


def test_class_mismatch_blocks_gold_write(ctx, landing):
    res = _build(ctx, landing)
    res.totals["class_sum"] = D("0")
    with pytest.raises(gold.GoldValidationError, match="revenue by class"):
        gold.validate(res)


def test_gold_run_writes_new_tables(ctx, landing):
    landing.batch("b1.csv", "NEW_DATA", 1, FIXTURE)
    bronze.run(ctx, landing.dir)
    silver.run(ctx)
    gold.run(ctx)
    assert ctx.store.read("gold_top_merchandise").count() == 3
    assert ctx.store.read("gold_revenue_by_class").agg(F.sum("GrossRevenue")).first()[0] == D("213.66")


def test_gold_backfills_new_tables_when_missing(ctx, landing):
    """An existing deployment (tables created before rule v1) gets the new tables on the next run."""
    from retail_pipeline import registry
    landing.batch("b1.csv", "NEW_DATA", 1, FIXTURE)
    bronze.run(ctx, landing.dir)
    silver.run(ctx)
    for t in ["gold_kpis", "gold_monthly_sales", "gold_sales_by_country", "gold_top_products"]:
        ctx.store.ensure(t)                               # the four tables of the earlier deployment
    registry.set_status(ctx, ["b1.csv"], "COMMITTED")     # nothing pending
    assert not ctx.store.exists("gold_top_merchandise")
    gold.run(ctx.new_run())
    assert ctx.store.read("gold_top_merchandise").count() == 3
    assert ctx.store.read("gold_revenue_by_class").count() == 4
    assert gold.run(ctx.new_run()) == []                  # all tables exist, nothing pending -> skip
