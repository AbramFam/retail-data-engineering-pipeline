"""Merge semantics shared by ParquetStore and DeltaStore, and write-safety guards."""
from datetime import datetime

import pytest

from retail_pipeline.config import DEFAULT_TARGET_SCHEMA, PIPELINE_WATERMARK
from retail_pipeline.storage import DeltaStore, ParquetStore, StorageSafetyError

T = "pipeline_watermark"


def _rows(store):
    return sorted((r["pipeline_name"], r["committed_through_sequence"]) for r in store.read(T).collect())


def _df(spark, rows):
    return spark.createDataFrame([(n, s, None, "r", datetime(2026, 1, 1)) for n, s in rows], PIPELINE_WATERMARK)


def test_merge_insert_update_and_conditions(spark, store):
    store.merge(T, _df(spark, [("a", 1), ("b", 2)]), ["pipeline_name"])
    assert _rows(store) == [("a", 1), ("b", 2)]
    store.merge(T, _df(spark, [("a", 10), ("c", 3)]), ["pipeline_name"],
                update_set={"committed_through_sequence": "s.committed_through_sequence"},
                update_condition="t.committed_through_sequence < s.committed_through_sequence",
                insert=True, insert_condition="s.committed_through_sequence > 100")
    assert _rows(store) == [("a", 10), ("b", 2)]      # c filtered by insert_condition


def test_merge_rejects_duplicate_source_keys(spark, store):
    with pytest.raises(ValueError, match="duplicate keys"):
        store.merge(T, _df(spark, [("a", 1), ("a", 2)]), ["pipeline_name"])


def test_unknown_tables_are_refused(spark, store):
    with pytest.raises(StorageSafetyError):
        store.overwrite("silver_sales_v2", _df(spark, []))


@pytest.mark.parametrize("schema", ["workspace.default", "workspace.retail_pipeline_test", " WORKSPACE.DEFAULT "])
def test_delta_store_refuses_protected_schemas(spark, schema):
    with pytest.raises(StorageSafetyError):
        DeltaStore(spark, schema)


def test_default_target_schema_is_the_isolated_one():
    assert DEFAULT_TARGET_SCHEMA == "workspace.retail_pipeline_pyspark"


def test_parquet_store_keeps_previous_version_if_write_fails(spark, tmp_path):
    store = ParquetStore(spark, str(tmp_path))
    store.overwrite(T, _df(spark, [("a", 1)]))
    bad = spark.createDataFrame([("x",)], "pipeline_name string").selectExpr(
        "pipeline_name", "raise_error('boom') AS committed_through_sequence")
    with pytest.raises(Exception):
        store.overwrite(T, bad)
    assert _rows(store) == [("a", 1)]
