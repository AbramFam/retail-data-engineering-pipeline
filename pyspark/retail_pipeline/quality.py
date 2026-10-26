"""Data-quality rules: type every Bronze row and record why a row is invalid.

Invalid rows are never dropped silently; Silver writes them to
``silver_quarantine`` with their reasons.

Hard rules (row goes to quarantine):
  MALFORMED_ROW        more fields than the 8 expected columns
  MISSING_INVOICENO    InvoiceNo empty
  MISSING_STOCKCODE    StockCode empty
  INVALID_QUANTITY     not a whole number (or out of INT range)
  ZERO_QUANTITY        Quantity = 0 (no business event)
  INVALID_UNITPRICE    not a decimal with at most 4 decimal places
  INVALID_INVOICEDATE  not parseable with either accepted format

Soft flags (row kept, flagged): invalid customer, cancellation (C...),
accounting adjustment (A...), zero price, negative price, exact repeat.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import INVOICE_DATE_FORMATS, SOURCE_COLUMNS, UNKNOWN_PRODUCT

_QTY_RE = r"^-?[0-9]{1,9}$"
_PRICE_RE = r"^-?[0-9]{1,14}(\.[0-9]{1,4})?$"
_CUSTOMER_RE = r"^[1-9][0-9]*$"


def _blank(c):
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def normalized_customer_id(col="CustomerID"):
    """'17850.0' -> '17850'. Whitespace trimmed."""
    return F.regexp_replace(F.trim(F.col(col)), r"\.0+$", "")


def parse_invoice_date(col="InvoiceDate"):
    value = F.trim(F.col(col))
    return F.coalesce(*[F.try_to_timestamp(value, F.lit(fmt)) for fmt in INVOICE_DATE_FORMATS])


def evaluate(bronze: DataFrame) -> DataFrame:
    """Return Bronze rows with typed columns, flags and a ``dq_reasons`` array."""
    qty_txt = F.trim(F.col("Quantity"))
    price_txt = F.trim(F.col("UnitPrice"))
    qty_ok = qty_txt.rlike(_QTY_RE) & qty_txt.isNotNull()
    price_ok = price_txt.rlike(_PRICE_RE) & price_txt.isNotNull()
    cust = normalized_customer_id()
    cust_ok = F.coalesce(cust.rlike(_CUSTOMER_RE), F.lit(False))
    inv = F.upper(F.trim(F.col("InvoiceNo")))

    typed = (bronze
             .withColumn("_qty", F.when(qty_ok, qty_txt.cast("long")))
             .withColumn("_price", F.when(price_ok, price_txt.cast("decimal(18,4)")))
             .withColumn("_date", parse_invoice_date()))

    rules = [
        ("MALFORMED_ROW", F.col("malformed_extra_field").isNotNull()),
        ("MISSING_INVOICENO", _blank("InvoiceNo")),
        ("MISSING_STOCKCODE", _blank("StockCode")),
        ("INVALID_QUANTITY", ~F.coalesce(qty_ok, F.lit(False))),  # at most 9 digits: always fits INT
        ("ZERO_QUANTITY", F.col("_qty") == 0),
        ("INVALID_UNITPRICE", ~F.coalesce(price_ok, F.lit(False))),
        ("INVALID_INVOICEDATE", F.col("_date").isNull()),
    ]
    reasons = F.array_compact(F.array(*[F.when(F.coalesce(cond, F.lit(False)), F.lit(code)) for code, cond in rules]))

    repeat_window = Window.partitionBy("batch_id", *SOURCE_COLUMNS).orderBy("source_row_number")
    return (typed
            .withColumn("dq_reasons", reasons)
            .withColumn("Quantity_typed", F.col("_qty").cast("int"))
            .withColumn("UnitPrice_typed", F.col("_price"))
            .withColumn("InvoiceDate_typed", F.col("_date"))
            .withColumn("CustomerID_normalized",
                        F.when(cust_ok, cust).otherwise(F.nullif(F.trim(F.col("CustomerID")), F.lit(""))))
            .withColumn("Description_clean", F.coalesce(F.col("Description"), F.lit(UNKNOWN_PRODUCT)))
            .withColumn("is_valid_customer", cust_ok)
            .withColumn("is_cancellation", F.coalesce(inv.startswith("C"), F.lit(False)))
            .withColumn("is_adjustment", F.coalesce(inv.startswith("A"), F.lit(False)))
            .withColumn("is_zero_price", F.coalesce(F.col("_price") == 0, F.lit(False)))
            .withColumn("is_negative_price", F.coalesce(F.col("_price") < 0, F.lit(False)))
            .withColumn("is_exact_repeat", F.row_number().over(repeat_window) > 1)
            .drop("_qty", "_price", "_date"))
