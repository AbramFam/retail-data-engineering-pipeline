"""End-to-end: watermark only after success, partial failures, recovery, review blocking.

SYNTHETIC fixtures, local stores. Not a Databricks integration test.
"""
import pytest
from pyspark.sql import functions as F

from conftest import line
from retail_pipeline import commit, gold, pipeline, registry, silver
from retail_pipeline.bronze import BatchValidationError
from retail_pipeline.commit import OpenReviewItemsError
from retail_pipeline.config import BatchStatus

N, C = "NEW_DATA", "CORRECTION"


def statuses(ctx):
    return {r["batch_id"]: r["status"] for r in registry.read(ctx).collect()}


def watermark(ctx):
    row = ctx.store.read_or_empty("pipeline_watermark").first()
    return None if row is None else row["committed_through_sequence"]


def net(ctx):
    return float(ctx.store.read("gold_kpis").first()["NetRevenue"])


def test_happy_path_commits_and_advances_watermark(ctx, landing):
    landing.batch("b1.csv", N, 1, [line("1", "X", 2, "5.00")])
    landing.batch("b2.csv", N, 2, [line("2", "Y", 1, "3.00")])
    out = pipeline.run_all(ctx, landing.dir)
    assert out["commit"]["watermark"] == 2
    assert statuses(ctx) == {"b1.csv": "COMMITTED", "b2.csv": "COMMITTED"}
    assert net(ctx) == 13.0


def test_gold_failure_part_way_keeps_watermark_and_recovers(ctx, landing, monkeypatch):
    landing.batch("b1.csv", N, 1, [line("1", "X", 2, "5.00")])
    pipeline.run_all(ctx, landing.dir)
    assert watermark(ctx) == 1

    landing.batch("b2.csv", N, 2, [line("2", "Y", 1, "3.00")])
    real = ctx.store.overwrite

    def fail_on_kpis(name, df):
        if name == "gold_kpis":   # three Gold tables already rewritten at this point
            raise RuntimeError("simulated Gold failure")
        return real(name, df)
    run = ctx.new_run()
    monkeypatch.setattr(run.store, "overwrite", fail_on_kpis)
    with pytest.raises(RuntimeError, match="simulated Gold failure"):
        pipeline.run_all(run, landing.dir)
    monkeypatch.setattr(run.store, "overwrite", real)

    assert statuses(ctx)["b2.csv"] == BatchStatus.SILVER_APPLIED   # not published, not committed
    assert watermark(ctx) == 1                                      # did not advance
    assert net(ctx) == 10.0                                         # KPI table still the old one

    pipeline.run_all(ctx.new_run(), landing.dir)                    # recovery = plain rerun
    assert statuses(ctx) == {"b1.csv": "COMMITTED", "b2.csv": "COMMITTED"}
    assert watermark(ctx) == 2 and net(ctx) == 13.0
    monthly_total = ctx.store.read("gold_monthly_sales").agg(F.sum("NetRevenue")).first()[0]
    assert float(monthly_total) == 13.0                             # all Gold tables consistent again


def test_silver_failure_mid_run_recovers_without_duplicates(ctx, landing, monkeypatch):
    landing.batch("b1.csv", N, 1, [line("1", "X", 2, "5.00")])
    landing.batch("b2.csv", N, 2, [line("2", "Y", 1, "3.00")])
    real = silver.process_batch

    def fail_second(run_ctx, batch):
        if batch["batch_id"] == "b2.csv":
            raise RuntimeError("simulated Silver failure")
        return real(run_ctx, batch)
    monkeypatch.setattr(silver, "process_batch", fail_second)
    with pytest.raises(RuntimeError):
        pipeline.run_all(ctx, landing.dir)
    monkeypatch.setattr(silver, "process_batch", real)
    assert statuses(ctx) == {"b1.csv": "SILVER_APPLIED", "b2.csv": "INGESTED"}
    assert watermark(ctx) is None and not ctx.store.exists("gold_kpis")

    pipeline.run_all(ctx.new_run(), landing.dir)
    assert watermark(ctx) == 2 and net(ctx) == 13.0
    assert ctx.store.read("silver_sales").count() == 2 and ctx.store.read("bronze_sales").count() == 2


def test_bronze_validation_failure_stops_run_and_watermark(ctx, landing):
    landing.batch("b1.csv", N, 1, [line("1", "X", 2, "5.00")])
    pipeline.run_all(ctx, landing.dir)
    landing.csv("b2.csv", [line("2", "Y", 1, "3.00")])
    landing.manifest("b2.csv", "MERGE", 2, expected=1)
    with pytest.raises(BatchValidationError):
        pipeline.run_all(ctx.new_run(), landing.dir)
    assert watermark(ctx) == 1 and statuses(ctx)["b2.csv"] == BatchStatus.VALIDATION_FAILED


def test_rejected_correction_blocks_watermark_until_resolved_by_newer_correction(ctx, landing):
    landing.batch("b1.csv", N, 1, [line("1", "X", 2, "5.00")])
    pipeline.run_all(ctx, landing.dir)

    landing.batch("c2.csv", C, 2, [line("1", "X", "two", "5.00")])
    landing.batch("b3.csv", N, 3, [line("3", "Z", 1, "1.00")])
    with pytest.raises(OpenReviewItemsError):
        pipeline.run_all(ctx.new_run(), landing.dir)
    assert statuses(ctx) == {"b1.csv": "COMMITTED", "c2.csv": "BLOCKED_REVIEW", "b3.csv": "COMMITTED"}
    assert watermark(ctx) == 1                       # cannot pass the blocked correction
    assert net(ctx) == 11.0                          # Gold still published: 10 (old X) + 1 (b3)

    # Re-running without fixing anything keeps failing: it cannot be silently skipped.
    with pytest.raises(OpenReviewItemsError):
        pipeline.run_all(ctx.new_run(), landing.dir)
    assert watermark(ctx) == 1

    landing.batch("c4.csv", C, 4, [line("1", "X", 1, "5.00")])
    out = pipeline.run_all(ctx.new_run(), landing.dir)
    assert set(out["commit"]["committed"]) == {"c2.csv", "c4.csv"}
    assert watermark(ctx) == 4 and net(ctx) == 6.0


def test_manual_resolution_unblocks(ctx, landing):
    landing.batch("c1.csv", C, 1, [line("1", "X", "bad", "5.00")])
    with pytest.raises(OpenReviewItemsError):
        pipeline.run_all(ctx, landing.dir)
    rid = commit.open_rejections(ctx).first()["rejection_id"]
    with pytest.raises(ValueError, match="note is required"):
        commit.resolve_rejection(ctx, rid, " ")
    commit.resolve_rejection(ctx.new_run(), rid, "supplier withdrew the correction")
    out = pipeline.run_all(ctx.new_run(), landing.dir)
    assert out["commit"]["committed"] == ["c1.csv"] and watermark(ctx) == 1
    rej = ctx.store.read("correction_rejections").first()
    assert rej["status"] == "RESOLVED" and rej["resolution_note"].startswith("MANUAL:")


def test_late_blocked_batch_below_watermark_moves_watermark_back(ctx, landing):
    for name, seq in [("b1.csv", 1), ("b2.csv", 2), ("b5.csv", 5)]:
        landing.batch(name, N, seq, [line(str(seq), "X", 1, "1.00")])
    pipeline.run_all(ctx, landing.dir)
    assert watermark(ctx) == 5
    landing.batch("c3.csv", C, 3, [line("1", "X", "bad", "1.00")])   # late and broken
    with pytest.raises(OpenReviewItemsError):
        pipeline.run_all(ctx.new_run(), landing.dir)
    row = ctx.store.read("pipeline_watermark").first()
    assert (row["committed_through_sequence"], row["previous_committed_through_sequence"]) == (2, 5)


def test_late_valid_batch_below_watermark_commits(ctx, landing):
    landing.batch("b1.csv", N, 1, [line("1", "X", 1, "1.00")])
    landing.batch("b5.csv", N, 5, [line("5", "X", 1, "1.00")])
    pipeline.run_all(ctx, landing.dir)
    landing.batch("b3.csv", N, 3, [line("3", "X", 1, "1.00")])
    pipeline.run_all(ctx.new_run(), landing.dir)
    assert watermark(ctx) == 5 and statuses(ctx)["b3.csv"] == "COMMITTED"


def test_open_issue_mode_can_be_reported_without_raising(ctx, landing):
    landing.batch("c1.csv", C, 1, [line("1", "X", "bad", "5.00")])
    ctx.fail_on_open_issues = False
    out = pipeline.run_all(ctx, landing.dir)
    assert out["commit"]["blocked"] == ["c1.csv"] and out["commit"]["watermark"] is None


def test_gold_skips_when_nothing_new(ctx, landing):
    landing.batch("b1.csv", N, 1, [line("1", "X", 2, "5.00")])
    pipeline.run_all(ctx, landing.dir)
    run_id = ctx.store.read("gold_kpis").first()["gold_run_id"]
    assert gold.run(ctx.new_run()) == []
    assert ctx.store.read("gold_kpis").first()["gold_run_id"] == run_id
