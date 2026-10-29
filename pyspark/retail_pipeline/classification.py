"""StockCode classification: merchandise vs. services and adjustments (rule v1).

Approved 2026-10-29 (see docs/proposal_product_classification.md). It only feeds the
additional tables gold_top_merchandise and gold_revenue_by_class; no existing KPI or
table definition uses it.

* Explicit reference list (case-sensitive, trimmed code).
* Gift vouchers by pattern.
* Everything else is PRODUCT (revenue is never hidden by default). A PRODUCT whose code
  does not look like a standard merchandise code is flagged NeedsReview, not excluded.
"""
from pyspark.sql import functions as F

RULE_VERSION = "v1"

PRODUCT = "PRODUCT"
REFERENCE = {
    "POST": "SHIPPING",
    "DOT": "SHIPPING",
    "C2": "SHIPPING",
    "AMAZONFEE": "FEE_OR_CHARGE",
    "BANK CHARGES": "FEE_OR_CHARGE",
    "CRUK": "FEE_OR_CHARGE",
    "M": "MANUAL_ADJUSTMENT",
    "m": "MANUAL_ADJUSTMENT",
    "D": "MANUAL_ADJUSTMENT",
    "S": "MANUAL_ADJUSTMENT",
    "B": "ACCOUNTING_ADJUSTMENT",
}
GIFT_VOUCHER = "GIFT_VOUCHER"
GIFT_PATTERN = r"^gift_[0-9]{4}_[0-9]+$"
STANDARD_CODE_PATTERN = r"^[0-9]{5}[A-Za-z]{0,2}$"


def product_class(col="StockCode"):
    code = F.trim(F.col(col))
    expr = None
    for value, cls in REFERENCE.items():
        cond = code == F.lit(value)
        expr = F.when(cond, F.lit(cls)) if expr is None else expr.when(cond, F.lit(cls))
    return expr.when(code.rlike(GIFT_PATTERN), F.lit(GIFT_VOUCHER)).otherwise(F.lit(PRODUCT))


def needs_review(col="StockCode"):
    return (product_class(col) == PRODUCT) & ~F.trim(F.col(col)).rlike(STANDARD_CODE_PATTERN)
