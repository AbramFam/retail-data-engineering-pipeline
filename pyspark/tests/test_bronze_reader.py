"""Deterministic source_row_number extraction from CSV files."""
import csv
import os

import pytest

from conftest import PYSPARK_DIR, REPO_ROOT, line
from retail_pipeline import bronze
from retail_pipeline.bronze import BatchValidationError


def _rows(src):
    return [r.asDict() for r in src.rows.orderBy("source_row_number").collect()]


def test_row_numbers_follow_physical_file_order(ctx, landing):
    data = [line(f"9000{i:02d}", "X", i + 1, "1.00") for i in range(25)]
    path = landing.csv("f.csv", data)
    src = bronze.read_source_file(ctx, path)
    got = _rows(src)
    # Reference: Python's csv module, reading the file sequentially.
    with open(path, newline="") as fh:
        expected = list(csv.reader(fh))[1:]
    assert src.row_count == 25
    assert [r["source_row_number"] for r in got] == list(range(1, 26))
    assert [[r[c] for c in ["InvoiceNo", "StockCode", "Description", "Quantity",
                             "InvoiceDate", "UnitPrice", "CustomerID", "Country"]] for r in got] == expected


def test_same_bytes_give_same_numbers_regardless_of_partitioning(ctx, spark, landing):
    data = [line(f"9100{i:03d}", "Y", 1, "2.00") for i in range(300)]
    path = landing.csv("g.csv", data)
    first = {(r["source_row_number"], r["InvoiceNo"]) for r in _rows(bronze.read_source_file(ctx, path))}
    old = spark.conf.get("spark.sql.shuffle.partitions")
    try:
        spark.conf.set("spark.sql.shuffle.partitions", "7")
        repartitioned = bronze.read_source_file(ctx, path).rows.repartition(5)
        second = {(r["source_row_number"], r["InvoiceNo"]) for r in repartitioned.collect()}
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", old)
    assert first == second


def test_quoted_commas_crlf_bom_and_blank_lines(ctx, landing):
    data = [line("900001", "X", 1, "1.00", desc="MUG, LARGE"), line("900002", "Y", 2, "3.50")]
    path = landing.csv("h.csv", data, newline="\r\n", bom=True)
    with open(path, "a", newline="") as fh:
        fh.write("\r\n\r\n")  # trailing blank lines are ignored
    rows = _rows(bronze.read_source_file(ctx, path))
    assert [r["source_row_number"] for r in rows] == [1, 2]
    assert rows[0]["Description"] == "MUG, LARGE"
    assert rows[1]["Country"] == "Cyprus"  # no stray \r


def test_blank_line_in_middle_keeps_physical_numbering(ctx, landing):
    path = landing.path("i.csv")
    with open(path, "w") as fh:
        fh.write(",".join(["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
                           "UnitPrice", "CustomerID", "Country"]) + "\n")
        fh.write("900001,X,A,1,2026-08-01 10:00:00,1.00,1,Cyprus\n\n")
        fh.write("900002,Y,B,1,2026-08-01 10:00:00,1.00,1,Cyprus\n")
    assert [r["source_row_number"] for r in _rows(bronze.read_source_file(ctx, path))] == [1, 3]


def test_extra_field_is_captured_not_lost(ctx, landing):
    path = landing.path("j.csv")
    with open(path, "w") as fh:
        fh.write("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n")
        fh.write("900001,X,A,1,2026-08-01 10:00:00,1.00,1,Cyprus,EXTRA\n")
    assert _rows(bronze.read_source_file(ctx, path))[0]["malformed_extra_field"] == "EXTRA"


def test_header_mismatch_is_rejected(ctx, landing):
    path = landing.csv("k.csv", [line("900001", "X", 1, "1.00")],
                       header=["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate",
                               "UnitPrice", "CustomerID", "Country"])
    with pytest.raises(BatchValidationError, match="header"):
        bronze.read_source_file(ctx, path)


def test_embedded_newline_is_rejected_not_misnumbered(ctx, landing):
    path = landing.path("l.csv")
    with open(path, "w") as fh:
        fh.write("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n")
        fh.write('900001,X,"TWO\nLINES",1,2026-08-01 10:00:00,1.00,1,Cyprus\n')
    with pytest.raises(BatchValidationError, match="unbalanced quotes"):
        bronze.read_source_file(ctx, path)


def test_repository_batch_006_reads_as_five_rows(ctx):
    src = bronze.read_source_file(ctx, os.path.join(REPO_ROOT, "data", "batch_006.csv"))
    assert src.row_count == 5
    assert [r["InvoiceNo"] for r in _rows(src)] == ["600002", "600004", "600301", "600302", "600303"]


def test_pipeline_code_never_uses_monotonically_increasing_id():
    pkg = os.path.join(PYSPARK_DIR, "retail_pipeline")
    for name in os.listdir(pkg):
        if name.endswith(".py"):
            with open(os.path.join(pkg, name)) as fh:
                code = "\n".join(l for l in fh.read().splitlines() if not l.strip().startswith(("#", '"', "`")))
            assert "monotonically_increasing_id(" not in code, name
