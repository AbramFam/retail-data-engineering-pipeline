"""Gold rules (Notebook 08 definitions) on a hand-computed SYNTHETIC fixture."""
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from conftest import line
from retail_pipeline import bronze, gold, registry, silver
from retail_pipeline.config import BatchStatus
from retail_pipeline.gold import GoldValidationError

D = Decimal
FIXTURE = [
    # inv     stock qty  price     cust      desc          date                  country
    line("1001", "X", 2, "10.00", "100", "MUG", "2026-01-05 10:00:00", "United Kingdom"),
    line("1001", "Y", 1, "5.46", "100", "PLATE", "2026-01-05 10:00:00", "United Kingdom"),
    line("1002", "X", 1, "10.00", "101.0", "MUG BIG", "2026-02-03 10:00:00", "Cyprus"),
    line("C1003", "X", -1, "10.00", "100", "MUG", "2026-02-10 10:00:00", "United Kingdom"),   # return
    line("1004", "Z", 3, "0.00", "-1", "", "2026-02-11 10:00:00", "Cyprus"),                  # zero price
    line("A9001", "B", 1, "-50.00", "", "Adjust bad debt", "2026-02-12 10:00:00", "United Kingdom"),
    line("1005", "X", 1, "10.00", "ABC", "MUG", "2026-02-15 10:00:00", "Cyprus"),             # invalid customer
    line("1006", "Z", -2, "0.00", "", "", "2026-02-16 10:00:00", "United Kingdom"),           # stock adjustment
]


def _build(ctx, landing, rows=FIXTURE):
    landing.batch("b1.csv", "NEW_DATA", 1, rows)
    bronze.run(ctx, landing.dir)
    silver.run(ctx)
    return gold.build(ctx, ctx.store.read("silver_sales"))


def test_kpis_match_hand_computed_values(ctx, landing):
    res = _build(ctx, landing)
    gold.validate(res)
    k = res.kpis.first()
    assert k["GrossRevenue"] == D("45.46")       # 20 + 5.46 + 10 + 0 + 10 + 0 (A excluded)
    assert k["ReturnsValue"] == D("10.00")
    assert k["NetRevenue"] == D("35.46")
    assert k["GrossRevenue"] - k["ReturnsValue"] == k["NetRevenue"]
    assert k["TotalCustomers"] == 2              # 100, 101 ; -1 / ABC / NULL excluded
    assert k["TotalOrders"] == 4                 # 1001, 1002, 1004, 1005
    assert k["AOV"] == D("8.87")                 # 35.46 / 4 = 8.865 -> HALF_UP
    assert k["AdjustmentsExcludedValue"] == D("-50.00")


def test_monthly_and_country(ctx, landing):
    res = _build(ctx, landing)
    monthly = {r["Month"]: r["NetRevenue"] for r in res.monthly.collect()}
    assert monthly == {datetime(2026, 1, 1): D("25.46"), datetime(2026, 2, 1): D("10.00")}
    country = {r["Country"]: r["NetRevenue"] for r in res.by_country.collect()}
    assert country == {"United Kingdom": D("15.46"), "Cyprus": D("20.00")}


def test_top_products_identity_and_description_rule(ctx, landing):
    res = _build(ctx, landing)
    top = [(r["Rank"], r["StockCode"], r["Description"], r["GrossRevenue"], r["UnitsSold"])
           for r in res.top_products.orderBy("Rank").collect()]
    # X has descriptions MUG (2 sales lines) and MUG BIG (1) -> one row, most frequent wins.
    assert top == [(1, "X", "MUG", D("40.00"), 4), (2, "Y", "PLATE", D("5.46"), 1)]


def test_description_ties_break_by_latest_then_alphabetical(ctx, landing):
    rows = [line("1", "W", 1, "1.00", desc="OLD NAME", date="2026-01-01 10:00:00"),
            line("2", "W", 1, "1.00", desc="NEW NAME", date="2026-03-01 10:00:00"),
            line("3", "V", 1, "1.00", desc="BETA", date="2026-01-01 10:00:00"),
            line("4", "V", 1, "1.00", desc="ALPHA", date="2026-01-01 10:00:00"),
            line("5", "U", 1, "1.00", desc="", date="2026-01-01 10:00:00")]
    res = _build(ctx, landing, rows)
    desc = {r["StockCode"]: r["Description"] for r in res.top_products.collect()}
    assert desc == {"W": "NEW NAME", "V": "ALPHA", "U": "Unknown Product"}


def test_batch_without_returns_gives_zero_not_null(ctx, landing):
    res = _build(ctx, landing, [line("1", "X", 2, "3.00")])
    gold.validate(res)
    k = res.kpis.first()
    assert (k["ReturnsValue"], k["GrossRevenue"], k["NetRevenue"]) == (D("0.00"), D("6.00"), D("6.00"))


def test_empty_silver_has_null_aov_and_passes(ctx, landing):
    res = gold.build(ctx, ctx.store.read_or_empty("silver_sales"))
    gold.validate(res)
    k = res.kpis.first()
    assert k["TotalOrders"] == 0 and k["AOV"] is None and k["NetRevenue"] == D("0.00")


@pytest.mark.parametrize("key, bad", [("monthly_sum", D("1")), ("country_sum", D("1")),
                                      ("all_current", D("1")), ("null_months", 1)])
def test_validation_detects_inconsistency(ctx, landing, key, bad):
    res = _build(ctx, landing)
    res.totals[key] = bad
    with pytest.raises(GoldValidationError):
        gold.validate(res)


def test_failed_validation_writes_nothing(ctx, landing, monkeypatch):
    landing.batch("b1.csv", "NEW_DATA", 1, FIXTURE)
    bronze.run(ctx, landing.dir)
    silver.run(ctx)
    real_build = gold.build

    def broken(*a, **k):
        r = real_build(*a, **k)
        r.totals["monthly_sum"] = D("0")
        return r
    monkeypatch.setattr(gold, "build", broken)
    with pytest.raises(GoldValidationError):
        gold.run(ctx)
    assert not any(ctx.store.exists(t) for t in gold.GOLD_TABLES)
    assert registry.read(ctx).first()["status"] == BatchStatus.SILVER_APPLIED


def test_only_current_lines_count(ctx, landing):
    landing.batch("b1.csv", "NEW_DATA", 1, [line("1", "X", 5, "2.00")])
    landing.batch("c2.csv", "CORRECTION", 2, [line("1", "X", 1, "2.00")])
    bronze.run(ctx, landing.dir)
    silver.run(ctx)
    res = gold.build(ctx, ctx.store.read("silver_sales"))
    assert res.kpis.first()["NetRevenue"] == D("2.00")
    assert ctx.store.read("silver_sales").filter(F.col("line_status") == "SUPERSEDED").count() == 1
