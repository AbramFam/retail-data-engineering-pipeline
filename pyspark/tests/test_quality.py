"""DQ rules: every invalid row gets an explicit reason; valid rows are typed correctly."""
from datetime import datetime
from decimal import Decimal

import pytest

from retail_pipeline import quality
from retail_pipeline.config import BRONZE_SALES


def _eval(spark, rows):
    base = {"malformed_extra_field": None, "bronze_row_id": "x", "batch_id": "b", "batch_type": "NEW_DATA",
            "batch_sequence": 1, "source_file": "b", "source_file_path": None, "file_content_hash": "h",
            "source_system": "t", "ingestion_timestamp": datetime(2026, 1, 1), "ingestion_run_id": "r"}
    full = []
    for i, r in enumerate(rows, start=1):
        rec = dict(base, InvoiceNo="900001", StockCode="X", Description="MUG", Quantity="1",
                   InvoiceDate="2026-08-01 10:00:00", UnitPrice="2.50", CustomerID="17850", Country="Cyprus",
                   source_row_number=i, bronze_row_id=f"id{i}")
        rec.update(r)
        full.append(rec)
    df = spark.createDataFrame(full, BRONZE_SALES)
    return [r.asDict() for r in quality.evaluate(df).orderBy("source_row_number").collect()]


@pytest.mark.parametrize("override, reason", [
    ({"Quantity": "abc"}, "INVALID_QUANTITY"),
    ({"Quantity": "1.5"}, "INVALID_QUANTITY"),
    ({"Quantity": None}, "INVALID_QUANTITY"),
    ({"Quantity": "0"}, "ZERO_QUANTITY"),
    ({"UnitPrice": "two"}, "INVALID_UNITPRICE"),
    ({"UnitPrice": "1.23456"}, "INVALID_UNITPRICE"),
    ({"UnitPrice": None}, "INVALID_UNITPRICE"),
    ({"InvoiceDate": "31/31/2026"}, "INVALID_INVOICEDATE"),
    ({"InvoiceDate": None}, "INVALID_INVOICEDATE"),
    ({"InvoiceNo": None}, "MISSING_INVOICENO"),
    ({"InvoiceNo": "  "}, "MISSING_INVOICENO"),
    ({"StockCode": None}, "MISSING_STOCKCODE"),
    ({"malformed_extra_field": "EXTRA"}, "MALFORMED_ROW"),
])
def test_each_rule_reports_its_reason(spark, override, reason):
    row = _eval(spark, [override])[0]
    assert reason in row["dq_reasons"]


def test_multiple_reasons_are_all_reported(spark):
    row = _eval(spark, [{"Quantity": "x", "UnitPrice": "y", "InvoiceDate": "z"}])[0]
    assert set(row["dq_reasons"]) == {"INVALID_QUANTITY", "INVALID_UNITPRICE", "INVALID_INVOICEDATE"}


def test_valid_row_types_and_both_date_formats(spark):
    rows = _eval(spark, [{}, {"InvoiceDate": "12/1/2010 8:26", "Quantity": "-3", "UnitPrice": "0.001"}])
    assert rows[0]["dq_reasons"] == [] and rows[1]["dq_reasons"] == []
    assert rows[0]["InvoiceDate_typed"] == datetime(2026, 8, 1, 10, 0)
    assert rows[1]["InvoiceDate_typed"] == datetime(2010, 12, 1, 8, 26)
    assert rows[1]["Quantity_typed"] == -3
    assert rows[1]["UnitPrice_typed"] == Decimal("0.0010")  # sub-cent price is not rounded away


@pytest.mark.parametrize("raw, normalized, valid", [
    ("17850", "17850", True),
    ("17850.0", "17850", True),
    (" 12346 ", "12346", True),
    (None, None, False),
    ("-1", "-1", False),
    ("0", "0", False),
    ("ABC12", "ABC12", False),
    ("", None, False),
])
def test_customer_id_rules(spark, raw, normalized, valid):
    row = _eval(spark, [{"CustomerID": raw}])[0]
    assert row["dq_reasons"] == []            # customer problems never quarantine a sale
    assert row["is_valid_customer"] is valid
    assert row["CustomerID_normalized"] == normalized


def test_flags(spark):
    rows = _eval(spark, [{"InvoiceNo": "C900001", "Quantity": "-1"}, {"InvoiceNo": "A563186", "UnitPrice": "-11062.06"},
                         {"UnitPrice": "0"}, {"Description": None}])
    assert rows[0]["is_cancellation"] and not rows[0]["is_adjustment"]
    assert rows[1]["is_adjustment"] and rows[1]["is_negative_price"]
    assert rows[2]["is_zero_price"]
    assert rows[3]["Description_clean"] == "Unknown Product"


def test_exact_repeats_are_flagged_not_removed(spark):
    rows = _eval(spark, [{}, {"Quantity": "2"}, {}, {}])
    assert [r["is_exact_repeat"] for r in rows] == [False, False, True, True]
    assert all(r["dq_reasons"] == [] for r in rows)
