"""Synthetic integration scenarios (Phase 3 of docs/databricks_integration_procedure.md).

Run in Databricks against the isolated test schema ONLY, with fresh (empty) tables:

    import sys; sys.path.insert(0, "<git folder>/pyspark")
    from retail_pipeline.storage import DeltaStore
    from scripts.integration_scenario import run_scenario
    run_scenario(spark, DeltaStore(spark, "workspace.retail_pipeline_pyspark_it"),
                 "/Volumes/workspace/retail_pipeline_pyspark_it/landing/incoming")

It refuses to run if batch_registry is not empty. Result on 2026-10-28: 16/16 passed.
"""


def run_scenario(spark, store, landing):
    """Synthetic integration scenarios. Writes ONLY to the given store/landing dir."""
    import csv, json, os
    from pyspark.sql import functions as F
    from retail_pipeline import pipeline
    from retail_pipeline.bronze import BatchValidationError
    from retail_pipeline.commit import OpenReviewItemsError
    from retail_pipeline.context import PipelineContext

    if store.read_or_empty("batch_registry").count() > 0:
        raise RuntimeError("batch_registry is not empty: this scenario expects fresh tables. Stopping.")
    os.makedirs(landing, exist_ok=True)
    H = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    results = []

    def land(name, btype, seq, rows):
        with open(os.path.join(landing, name), "w", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n"); w.writerow(H); w.writerows(rows)
        with open(os.path.join(landing, name + ".manifest.json"), "w") as fh:
            json.dump({"batch_type": btype, "batch_sequence": seq, "expected_row_count": len(rows),
                       "source_system": "it_synthetic"}, fh)

    def r(inv, sc, q, p="2.00", d="IT ITEM"):
        return [inv, sc, d, str(q), "2026-08-01 10:00:00", p, "17850", "Cyprus"]

    def run(expect=None):
        ctx = PipelineContext(spark, store)
        try:
            pipeline.run_all(ctx, landing); return None
        except Exception as e:
            if expect and isinstance(e, expect):
                return e
            raise

    def check(step, name, ok, detail=""):
        results.append((step, name, bool(ok)))
        print(f"[{'PASS' if ok else 'FAIL'}] step {step}: {name} {detail}")

    def status():
        return {x["batch_id"]: x["status"] for x in store.read("batch_registry").collect()}

    def wm():
        row = store.read_or_empty("pipeline_watermark").first()
        return None if row is None else row["committed_through_sequence"]

    def cur(inv, sc):
        return sorted(x["Quantity"] for x in store.read("silver_sales").filter(
            (F.col("InvoiceNo") == inv) & (F.col("StockCode") == sc) & (F.col("line_status") == "CURRENT")).collect())

    B1 = [r("900001", "X", 1), r("900001", "Y", 2, "5.00"), r("900001", "X", 1), r("900001", "X", 7)]

    # 1 NEW_DATA with legitimate repeated lines
    land("it_b1.csv", "NEW_DATA", 1, B1); run()
    check(1, "repeated lines kept", cur("900001", "X") == [1, 1, 7], cur("900001", "X"))
    check(1, "committed + watermark 1", status() == {"it_b1.csv": "COMMITTED"} and wm() == 1)
    check(1, "gold net revenue 28.00", float(store.read("gold_kpis").first()["NetRevenue"]) == 28.0)

    # 2 replay: nothing new
    n_b, n_s = store.read("bronze_sales").count(), store.read("silver_sales").count()
    run()
    check(2, "replay is a no-op", (store.read("bronze_sales").count(), store.read("silver_sales").count()) == (n_b, n_s))

    # 3 CORRECTION restates key X
    land("it_c2.csv", "CORRECTION", 2, [r("900001", "X", 3)]); run()
    check(3, "correction replaced X", cur("900001", "X") == [3], cur("900001", "X"))
    check(3, "history kept (3 SUPERSEDED)", store.read("silver_sales").filter("line_status = 'SUPERSEDED'").count() == 3)
    check(3, "watermark 2", wm() == 2)

    # 4 CORRECTION with an invalid row -> held, blocks watermark
    land("it_c3.csv", "CORRECTION", 3, [r("900001", "Y", "bad", "5.00")])
    e = run(expect=OpenReviewItemsError)
    check(4, "commit fails with open review item", e is not None)
    check(4, "Y not superseded", cur("900001", "Y") == [2], cur("900001", "Y"))
    check(4, "c3 BLOCKED_REVIEW, watermark stays 2", status().get("it_c3.csv") == "BLOCKED_REVIEW" and wm() == 2)

    # 5 newer valid CORRECTION resolves it
    land("it_c4.csv", "CORRECTION", 4, [r("900001", "Y", 4, "5.00")]); run()
    check(5, "rejection resolved", store.read("correction_rejections").filter("status = 'OPEN'").count() == 0)
    check(5, "all committed, watermark 4", set(status().values()) == {"COMMITTED"} and wm() == 4)
    check(5, "gold net revenue 26.00", float(store.read("gold_kpis").first()["NetRevenue"]) == 26.0)

    # 6 immutable file changed -> stop, nothing ingested
    n_b = store.read("bronze_sales").count()
    land("it_b1.csv", "NEW_DATA", 1, B1[:3])
    e = run(expect=BatchValidationError)
    check(6, "changed file is refused", e is not None and "immutable" in str(e))
    check(6, "nothing ingested", store.read("bronze_sales").count() == n_b)
    land("it_b1.csv", "NEW_DATA", 1, B1)  # restore original bytes
    run()
    check(6, "restored file replays cleanly", wm() == 4)

    passed = sum(ok for _, _, ok in results)
    print(f"\n==== {passed}/{len(results)} checks passed ====")
    return results
