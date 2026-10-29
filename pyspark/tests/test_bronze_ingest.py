"""Manifest validation, file immutability, replay safety."""
import json

import pytest
from pyspark.sql import functions as F

from conftest import line
from retail_pipeline import bronze, registry
from retail_pipeline.bronze import BatchValidationError
from retail_pipeline.config import BatchStatus


def _statuses(ctx):
    return {r["batch_id"]: r["status"] for r in registry.read(ctx).collect()}


def _bronze_count(ctx):
    return ctx.store.read_or_empty("bronze_sales").count()


def test_new_batch_is_ingested_with_metadata(ctx, landing):
    landing.batch("batch_001.csv", "NEW_DATA", 1, [line("900001", "X", 1, "2.00"), line("900001", "X", 1, "2.00")])
    out = bronze.run(ctx, landing.dir)
    assert out["ingested"] == ["batch_001.csv"]
    rows = ctx.store.read("bronze_sales").orderBy("source_row_number").collect()
    assert [r["source_row_number"] for r in rows] == [1, 2]
    assert len({r["bronze_row_id"] for r in rows}) == 2  # identical lines, distinct identities
    assert {r["batch_type"] for r in rows} == {"NEW_DATA"} and {r["batch_sequence"] for r in rows} == {1}
    assert rows[0]["Quantity"] == "1"  # Bronze keeps raw strings
    reg = registry.read(ctx).first()
    assert (reg["status"], reg["ingested_row_count"], reg["expected_row_count"]) == ("INGESTED", 2, 2)


@pytest.mark.parametrize("manifest, error", [
    (None, "missing manifest"),
    ({"batch_type": "UPSERT", "batch_sequence": 1, "expected_row_count": 1}, "unknown batch_type"),
    ({"batch_sequence": 1, "expected_row_count": 1}, "unknown batch_type"),
    ({"batch_type": "NEW_DATA", "batch_sequence": 0, "expected_row_count": 1}, "batch_sequence"),
    ({"batch_type": "NEW_DATA", "batch_sequence": "3", "expected_row_count": 1}, "batch_sequence"),
    ({"batch_type": "NEW_DATA", "batch_sequence": 1, "expected_row_count": 2}, "row count 1 != manifest"),
])
def test_invalid_manifest_fails_validation_and_nothing_is_ingested(ctx, landing, manifest, error):
    landing.batch("good.csv", "NEW_DATA", 1, [line("900001", "X", 1, "2.00")])
    landing.csv("bad.csv", [line("900002", "Y", 1, "2.00")])
    if manifest is not None:
        manifest = dict(manifest)
        if manifest.get("batch_sequence") == 1:
            manifest["batch_sequence"] = 2
        with open(landing.path("bad.csv") + ".manifest.json", "w") as fh:
            json.dump(manifest, fh)
    with pytest.raises(BatchValidationError, match=error):
        bronze.run(ctx, landing.dir)
    assert _bronze_count(ctx) == 0                      # the good file was not ingested either
    assert _statuses(ctx) == {"bad.csv": BatchStatus.VALIDATION_FAILED}


def test_failed_file_can_be_redelivered_after_fix(ctx, landing):
    landing.csv("b.csv", [line("900001", "X", 1, "2.00")])
    landing.manifest("b.csv", "SOMETHING", 1, expected=1)
    with pytest.raises(BatchValidationError):
        bronze.run(ctx, landing.dir)
    landing.manifest("b.csv", "NEW_DATA", 1, expected=1)
    bronze.run(ctx.new_run(), landing.dir)
    assert _statuses(ctx) == {"b.csv": BatchStatus.INGESTED}


def test_replaying_the_same_file_is_a_noop(ctx, landing):
    landing.batch("batch_001.csv", "NEW_DATA", 1, [line("900001", "X", 1, "2.00")])
    bronze.run(ctx, landing.dir)
    out = bronze.run(ctx.new_run(), landing.dir)
    assert out == {"ingested": [], "skipped": ["batch_001.csv"]}
    assert _bronze_count(ctx) == 1


def test_same_name_different_content_stops_for_review(ctx, landing):
    landing.batch("batch_001.csv", "NEW_DATA", 1, [line("900001", "X", 1, "2.00")])
    bronze.run(ctx, landing.dir)
    before = registry.read(ctx).collect()
    landing.batch("batch_001.csv", "NEW_DATA", 1, [line("900001", "X", 5, "2.00")])  # edited in place
    landing.batch("batch_002.csv", "NEW_DATA", 2, [line("900002", "Y", 1, "1.00")])
    with pytest.raises(BatchValidationError, match="immutable file changed"):
        bronze.run(ctx.new_run(), landing.dir)
    assert _bronze_count(ctx) == 1                       # batch_002 was not ingested either
    assert registry.read(ctx).collect() == before        # original registry row untouched


def test_changed_manifest_for_ingested_file_stops(ctx, landing):
    landing.batch("batch_001.csv", "NEW_DATA", 1, [line("900001", "X", 1, "2.00")])
    bronze.run(ctx, landing.dir)
    landing.manifest("batch_001.csv", "CORRECTION", 1, expected=1)
    with pytest.raises(BatchValidationError, match="manifest differs"):
        bronze.run(ctx.new_run(), landing.dir)


def test_same_content_under_new_name_stops(ctx, landing):
    rows = [line("900001", "X", 1, "2.00")]
    landing.batch("batch_001.csv", "NEW_DATA", 1, rows)
    bronze.run(ctx, landing.dir)
    landing.batch("batch_001_copy.csv", "NEW_DATA", 2, rows)
    with pytest.raises(BatchValidationError, match="identical content already ingested"):
        bronze.run(ctx.new_run(), landing.dir)


def test_duplicate_batch_sequence_stops(ctx, landing):
    landing.batch("a.csv", "NEW_DATA", 7, [line("900001", "X", 1, "2.00")])
    landing.batch("b.csv", "CORRECTION", 7, [line("900002", "Y", 1, "2.00")])
    with pytest.raises(BatchValidationError, match="batch_sequence 7 already used"):
        bronze.run(ctx, landing.dir)
    assert _bronze_count(ctx) == 0


def test_crash_between_bronze_write_and_registry_is_replay_safe(ctx, landing, monkeypatch):
    landing.batch("batch_001.csv", "NEW_DATA", 1, [line("900001", "X", 1, "2.00"), line("900001", "X", 1, "2.00")])
    real_upsert = registry.upsert

    def crash(*a, **k):
        raise RuntimeError("simulated crash after Bronze write")
    monkeypatch.setattr(registry, "upsert", crash)
    with pytest.raises(RuntimeError, match="simulated crash"):
        bronze.run(ctx, landing.dir)
    assert _bronze_count(ctx) == 2 and registry.read(ctx).count() == 0
    monkeypatch.setattr(registry, "upsert", real_upsert)
    bronze.run(ctx.new_run(), landing.dir)
    assert _bronze_count(ctx) == 2                        # no duplicates after replay
    assert _statuses(ctx) == {"batch_001.csv": BatchStatus.INGESTED}
    assert ctx.store.read("bronze_sales").select(F.countDistinct("bronze_row_id")).first()[0] == 2
