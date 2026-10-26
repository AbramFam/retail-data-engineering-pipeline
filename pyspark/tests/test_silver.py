"""Line-level Silver: repeats preserved, explicit corrections, versioning, holds, replay.

All data here is SYNTHETIC (fixtures), not the Databricks dataset.
"""
import os

from pyspark.sql import functions as F

from conftest import REPO_ROOT, line
from retail_pipeline import bronze, registry, silver
from retail_pipeline.config import BatchStatus, LineStatus

N, C = "NEW_DATA", "CORRECTION"


def load(ctx, landing, name, btype, seq, rows):
    landing.batch(name, btype, seq, rows)
    run = ctx.new_run()
    bronze.run(run, landing.dir)
    silver.run(run)
    return run


def lines_for(ctx, inv, stock, status=None):
    df = ctx.store.read("silver_sales").filter((F.col("InvoiceNo") == inv) & (F.col("StockCode") == stock))
    if status:
        df = df.filter(F.col("line_status") == status)
    return sorted((r["batch_id"], r["source_row_number"], r["Quantity"], r["line_status"]) for r in df.collect())


def current_qty(ctx, inv, stock):
    return sorted(q for _, _, q, _ in lines_for(ctx, inv, stock, LineStatus.CURRENT))


def test_legitimate_repeated_lines_are_all_preserved(ctx, landing):
    # Shape taken from UCI invoice 536412: same StockCode scanned several times.
    rows = [line("536412", "22077", 1, "1.65"), line("536412", "21448", 2, "1.65"),
            line("536412", "22077", 2, "1.65"), line("536412", "21448", 2, "1.65"),
            line("536412", "22077", 7, "1.65"), line("536412", "21448", 2, "1.65")]
    load(ctx, landing, "base.csv", N, 1, rows)
    s = ctx.store.read("silver_sales")
    assert s.count() == 6
    assert current_qty(ctx, "536412", "22077") == [1, 2, 7]
    assert current_qty(ctx, "536412", "21448") == [2, 2, 2]
    assert s.filter("is_exact_repeat").count() == 2       # flagged, not removed
    revenue = s.agg(F.sum(F.col("Quantity") * F.col("UnitPrice"))).first()[0]
    assert float(revenue) == 1.65 * 16


def test_new_data_never_supersedes_even_with_matching_key(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00")])
    load(ctx, landing, "b2.csv", N, 2, [line("900001", "X", 3, "2.00")])
    assert current_qty(ctx, "900001", "X") == [1, 3]


def test_correction_replaces_complete_key_and_keeps_history(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00"), line("900001", "X", 7, "2.00"),
                                        line("900001", "Y", 2, "5.00")])
    load(ctx, landing, "c2.csv", C, 2, [line("900001", "X", 3, "2.00")])
    assert current_qty(ctx, "900001", "X") == [3]
    assert current_qty(ctx, "900001", "Y") == [2]          # untouched key
    old = lines_for(ctx, "900001", "X", LineStatus.SUPERSEDED)
    assert [(b, q) for b, _, q, _ in old] == [("b1.csv", 1), ("b1.csv", 7)]
    sup = ctx.store.read("silver_sales").filter(F.col("line_status") == "SUPERSEDED").first()
    assert sup["superseded_by_sequence"] == 2


def test_batch_006_as_correction_two_replacements_three_inserts(ctx, landing):
    """SYNTHETIC base (batch_001-005 are not available); batch_006.csv is the real repo file, unchanged."""
    base = [line("600002", "71053", 1, "3.39", desc="WHITE METAL LANTERN"),
            line("600002", "71053", 2, "3.39", desc="WHITE METAL LANTERN"),   # two lines for one key
            line("600004", "84029G", 6, "3.39", desc="KNITTED UNION FLAG HOT WATER BOTTLE"),
            line("600099", "22423", 1, "12.75")]
    load(ctx, landing, "batch_005.csv", N, 5, base)
    landing.copy(os.path.join(REPO_ROOT, "data", "batch_006.csv"))
    landing.manifest("batch_006.csv", C, 6, expected=5)
    run = ctx.new_run()
    bronze.run(run, landing.dir)
    silver.run(run)
    assert current_qty(ctx, "600002", "71053") == [1]        # both old lines replaced by the one restated line
    assert current_qty(ctx, "600004", "84029G") == [4]
    for inv, sc in [("600301", "NEW301"), ("600302", "NEW302"), ("600303", "NEW303")]:
        assert len(current_qty(ctx, inv, sc)) == 1           # new keys inserted
    assert current_qty(ctx, "600099", "22423") == [1]        # not in the correction -> untouched
    cur = ctx.store.read("silver_sales").filter(F.col("line_status") == "CURRENT")
    assert cur.count() == 6 and ctx.store.read("silver_sales").count() == 9


def test_out_of_order_correction_never_overwrites_newer(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00")])
    load(ctx, landing, "c5.csv", C, 5, [line("900001", "X", 5, "2.00")])
    load(ctx, landing, "c4.csv", C, 4, [line("900001", "X", 4, "2.00")])  # arrives late
    assert current_qty(ctx, "900001", "X") == [5]
    stale = lines_for(ctx, "900001", "X", LineStatus.STALE_ON_ARRIVAL)
    assert [(b, q) for b, _, q, _ in stale] == [("c4.csv", 4)]
    assert ctx.store.read("silver_sales").filter("batch_id = 'c4.csv'").first()["superseded_by_sequence"] == 5


def test_late_new_data_older_than_applied_correction_is_stale(ctx, landing):
    load(ctx, landing, "c5.csv", C, 5, [line("900001", "X", 5, "2.00")])
    load(ctx, landing, "b3.csv", N, 3, [line("900001", "X", 3, "2.00"), line("900002", "Z", 1, "1.00")])
    assert current_qty(ctx, "900001", "X") == [5]
    assert current_qty(ctx, "900002", "Z") == [1]          # other key from the same late batch is fine


def test_correction_only_replaces_lines_older_than_itself(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00")])
    load(ctx, landing, "b8.csv", N, 8, [line("900001", "X", 8, "2.00")])   # newer new-data line
    load(ctx, landing, "c7.csv", C, 7, [line("900001", "X", 7, "2.00")])   # correction authored before b8
    assert current_qty(ctx, "900001", "X") == [7, 8]


def test_corrections_in_one_run_apply_in_sequence_order(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00")])
    landing.batch("c9.csv", C, 9, [line("900001", "X", 9, "2.00")])
    landing.batch("c3.csv", C, 3, [line("900001", "X", 3, "2.00")])
    run = ctx.new_run()
    bronze.run(run, landing.dir)
    silver.run(run)
    assert current_qty(ctx, "900001", "X") == [9]
    assert {b: s for b, _, _, s in lines_for(ctx, "900001", "X")} == {
        "b1.csv": "SUPERSEDED", "c3.csv": "SUPERSEDED", "c9.csv": "CURRENT"}


def test_invalid_row_in_correction_holds_only_that_key(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00"), line("900001", "Y", 2, "5.00")])
    load(ctx, landing, "c2.csv", C, 2, [line("900001", "X", 3, "2.00"),
                                        line("900001", "X", "three", "2.00"),   # invalid quantity
                                        line("900001", "Y", 4, "5.00")])
    assert current_qty(ctx, "900001", "X") == [1]          # NOT superseded
    assert lines_for(ctx, "900001", "X", LineStatus.HELD)[0][2] == 3
    assert current_qty(ctx, "900001", "Y") == [4]          # valid key in the same batch applied
    q = ctx.store.read("silver_quarantine").collect()
    assert len(q) == 1 and q[0]["dq_reasons"] == "INVALID_QUANTITY" and q[0]["Quantity"] == "three"
    rej = ctx.store.read("correction_rejections").collect()
    assert [(r["InvoiceNo"], r["StockCode"], r["status"], r["reason"]) for r in rej] == [
        ("900001", "X", "OPEN", "INVALID_QUANTITY")]
    reg = {r["batch_id"]: r for r in registry.read(ctx).collect()}
    assert reg["c2.csv"]["held_line_count"] == 1 and reg["c2.csv"]["quarantined_row_count"] == 1


def test_newer_valid_correction_resolves_rejection_older_does_not(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00")])
    load(ctx, landing, "c5.csv", C, 5, [line("900001", "X", "bad", "2.00")])
    load(ctx, landing, "c4.csv", C, 4, [line("900001", "X", 4, "2.00")])   # older: must not resolve c5
    rej = ctx.store.read("correction_rejections").first()
    assert rej["status"] == "OPEN"
    assert current_qty(ctx, "900001", "X") == [4]
    load(ctx, landing, "c6.csv", C, 6, [line("900001", "X", 6, "2.00")])
    rej = ctx.store.read("correction_rejections").first()
    assert (rej["status"], rej["resolved_by_batch"]) == ("RESOLVED", "c6.csv")
    assert current_qty(ctx, "900001", "X") == [6]


def test_rejection_already_overtaken_is_recorded_resolved(ctx, landing):
    load(ctx, landing, "c5.csv", C, 5, [line("900001", "X", 5, "2.00")])
    load(ctx, landing, "c4.csv", C, 4, [line("900001", "X", "bad", "2.00")])
    rej = ctx.store.read("correction_rejections").first()
    assert rej["status"] == "RESOLVED" and "sequence 5" in rej["resolution_note"]


def test_unattributable_invalid_row_holds_whole_correction(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00"), line("900001", "Y", 1, "2.00")])
    load(ctx, landing, "c2.csv", C, 2, [line("900001", "X", 2, "2.00"), line("", "Y", 2, "2.00")])
    assert current_qty(ctx, "900001", "X") == [1] and current_qty(ctx, "900001", "Y") == [1]
    rej = ctx.store.read("correction_rejections").filter(F.col("InvoiceNo").isNull()).first()
    assert rej["status"] == "OPEN" and rej["reason"].startswith("UNATTRIBUTABLE")


def test_invalid_rows_in_new_data_are_quarantined_not_dropped(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00"), line("900001", "X", 0, "2.00"),
                                        line("900002", "Y", 1, "2.00", date="not a date")])
    assert ctx.store.read("silver_sales").count() == 1
    reasons = sorted(r["dq_reasons"] for r in ctx.store.read("silver_quarantine").collect())
    assert reasons == ["INVALID_INVOICEDATE", "ZERO_QUANTITY"]
    assert ctx.store.read_or_empty("correction_rejections").count() == 0
    bronze_n = ctx.store.read("bronze_sales").count()
    assert bronze_n == ctx.store.read("silver_sales").count() + ctx.store.read("silver_quarantine").count()


def _snapshot(ctx):
    return sorted(tuple(r) for r in ctx.store.read("silver_sales").drop("status_updated_at").collect())


def test_replay_of_silver_is_idempotent(ctx, landing):
    load(ctx, landing, "b1.csv", N, 1, [line("900001", "X", 1, "2.00"), line("900001", "X", 1, "2.00")])
    load(ctx, landing, "c2.csv", C, 2, [line("900001", "X", 3, "2.00"), line("900001", "Y", "bad", "1.00")])
    load(ctx, landing, "b3.csv", N, 3, [line("900002", "Z", 1, "9.50")])
    before = _snapshot(ctx)
    q_before = ctx.store.read("silver_quarantine").count()
    rej_before = ctx.store.read("correction_rejections").collect()
    # Pretend every batch was never marked as applied and replay all of them.
    ids = [r["batch_id"] for r in registry.read(ctx).collect()]
    registry.set_status(ctx, ids, BatchStatus.INGESTED)
    silver.run(ctx.new_run())
    assert _snapshot(ctx) == before
    assert ctx.store.read("silver_quarantine").count() == q_before
    assert ctx.store.read("correction_rejections").collect() == rej_before


def test_crash_between_silver_merge_and_status_update_is_safe(ctx, landing, monkeypatch):
    landing.batch("b1.csv", N, 1, [line("900001", "X", 1, "2.00")])
    landing.batch("c2.csv", C, 2, [line("900001", "X", 3, "2.00")])
    run = ctx.new_run()
    bronze.run(run, landing.dir)
    real = registry.set_status
    calls = {"n": 0}

    def crash_on_second(*a, **k):
        calls["n"] += 1
        if calls["n"] == 2:   # b1 marked, c2 merged but not marked
            raise RuntimeError("simulated crash")
        return real(*a, **k)
    monkeypatch.setattr(registry, "set_status", crash_on_second)
    try:
        silver.run(run)
    except RuntimeError:
        pass
    monkeypatch.setattr(registry, "set_status", real)
    assert {r["batch_id"]: r["status"] for r in registry.read(ctx).collect()} == {
        "b1.csv": "SILVER_APPLIED", "c2.csv": "INGESTED"}
    silver.run(ctx.new_run())
    assert current_qty(ctx, "900001", "X") == [3]
    assert ctx.store.read("silver_sales").count() == 2
