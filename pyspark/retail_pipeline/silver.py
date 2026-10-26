"""Silver: line-level sales with explicit, versioned correction handling.

Grain: one row per valid source line, identified by ``bronze_row_id``.
The only MERGE key is ``bronze_row_id`` (unique by construction); business
keys are never used as a MERGE key.

Batch types (declared in the file's manifest, never inferred):

NEW_DATA (sequence s)
    Lines are appended. Nothing is superseded, even when an
    (InvoiceNo, StockCode) already exists. If a CORRECTION with a higher
    sequence was already applied to the key, the line is STALE_ON_ARRIVAL
    (that correction is the complete state of the key as of its sequence).

CORRECTION (sequence s) - the file restates the complete set of lines for
every (InvoiceNo, StockCode) it contains.
    For each key in the file:
      * any invalid row for the key   -> key HELD: its lines are stored as
        HELD_REJECTED_CORRECTION, existing lines stay CURRENT, and an OPEN
        rejection is recorded (blocks the watermark until resolved);
      * an applied correction with a higher sequence exists -> the lines
        are STALE_ON_ARRIVAL (an older correction never overwrites a newer one);
      * otherwise the lines become CURRENT and every CURRENT line of the key
        with a lower sequence becomes SUPERSEDED (history kept, nothing deleted).
    A correction row with no InvoiceNo/StockCode cannot be attributed to a
    key, so the whole batch is held.

Replays are no-ops: inserts match existing ids, and supersession only touches
CURRENT lines with a lower sequence.
"""
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import quality, registry
from .config import SOURCE_COLUMNS, BatchStatus, BatchType, LineStatus, RejectionStatus
from .context import PipelineContext

TABLE = "silver_sales"
KEY = ["InvoiceNo", "StockCode"]


def _applied_correction_versions(silver: DataFrame) -> DataFrame:
    return (silver.filter((F.col("batch_type") == BatchType.CORRECTION)
                          & F.col("line_status").isin(LineStatus.CURRENT, LineStatus.SUPERSEDED))
                  .groupBy(*KEY).agg(F.max("batch_sequence").alias("_newest_correction")))


def _silver_rows(valid: DataFrame, ctx: PipelineContext) -> DataFrame:
    return valid.select(
        "bronze_row_id", "batch_id", "batch_type", "batch_sequence", "source_file", "source_row_number",
        "InvoiceNo", "StockCode",
        F.col("Description_clean").alias("Description"),
        F.col("Quantity_typed").alias("Quantity"),
        F.col("InvoiceDate_typed").alias("InvoiceDate"),
        F.col("UnitPrice_typed").alias("UnitPrice"),
        F.col("CustomerID_normalized").alias("CustomerID"),
        "Country", "is_valid_customer", "is_cancellation", "is_adjustment", "is_zero_price",
        "is_negative_price", "is_exact_repeat", "ingestion_timestamp",
        F.lit(ctx.now).alias("silver_processed_at"), F.lit(ctx.run_id).alias("silver_run_id"),
        F.lit(ctx.now).alias("status_updated_at"),
    )


def process_batch(ctx: PipelineContext, batch) -> Dict[str, int]:
    store = ctx.store
    batch_id, seq, btype = batch["batch_id"], batch["batch_sequence"], batch["batch_type"]
    if btype not in BatchType.ALL:
        raise ValueError(f"{batch_id}: unknown batch_type {btype!r}")

    bronze = store.read("bronze_sales").filter(F.col("batch_id") == batch_id)
    evaluated = quality.evaluate(bronze)
    invalid = evaluated.filter(F.size("dq_reasons") > 0)
    valid = evaluated.filter(F.size("dq_reasons") == 0)

    # 1. Quarantine (insert-only, keyed on the row id -> replay safe).
    store.merge("silver_quarantine", invalid.select(
        "bronze_row_id", "batch_id", "batch_type", "batch_sequence", "source_file", "source_row_number",
        *SOURCE_COLUMNS, "malformed_extra_field",
        F.concat_ws(";", "dq_reasons").alias("dq_reasons"),
        F.lit(ctx.now).alias("quarantined_at"), F.lit(ctx.run_id).alias("silver_run_id"),
    ), ["bronze_row_id"], insert=True)

    existing = store.read_or_empty(TABLE)
    versions = _applied_correction_versions(existing)
    lines = _silver_rows(valid, ctx).join(versions, KEY, "left")
    newer_correction = F.coalesce(F.col("_newest_correction") > F.lit(seq), F.lit(False))

    rejected_keys = None
    if btype == BatchType.CORRECTION:
        unattributable = invalid.filter(F.col("InvoiceNo").isNull() | (F.trim("InvoiceNo") == "")
                                        | F.col("StockCode").isNull() | (F.trim("StockCode") == ""))
        hold_whole_batch = unattributable.limit(1).count() > 0
        rejected_keys = (invalid.groupBy(*KEY)
                         .agg(F.concat_ws(";", F.array_sort(F.array_distinct(F.flatten(F.collect_list("dq_reasons")))))
                              .alias("reason"))
                         .withColumn("_rejected", F.lit(True)))
        lines = lines.join(rejected_keys.select(*KEY, "_rejected"), KEY, "left")
        held = F.lit(True) if hold_whole_batch else F.coalesce(F.col("_rejected"), F.lit(False))
        status = (F.when(held, LineStatus.HELD)
                   .when(newer_correction, LineStatus.STALE_ON_ARRIVAL)
                   .otherwise(LineStatus.CURRENT))
        _record_rejections(ctx, batch_id, seq, rejected_keys, versions, hold_whole_batch)
    else:
        status = F.when(newer_correction, LineStatus.STALE_ON_ARRIVAL).otherwise(LineStatus.CURRENT)

    lines = (lines.withColumn("line_status", status)
                  .withColumn("superseded_by_sequence",
                              F.when(F.col("line_status") == LineStatus.STALE_ON_ARRIVAL, F.col("_newest_correction")))
                  .drop("_newest_correction", "_rejected"))
    inserts = lines.withColumn("__action", F.lit("INSERT"))

    plan = inserts
    applied_keys = None
    if btype == BatchType.CORRECTION:
        applied_keys = lines.filter(F.col("line_status") == LineStatus.CURRENT).select(*KEY).distinct()
        supersede = (existing.filter((F.col("line_status") == LineStatus.CURRENT)
                                     & (F.col("batch_sequence") < F.lit(seq))
                                     & (F.col("batch_id") != F.lit(batch_id)))
                     .join(applied_keys, KEY, "left_semi")
                     .select("bronze_row_id",
                             F.lit(seq).cast("long").alias("superseded_by_sequence"),
                             F.lit(ctx.now).alias("status_updated_at"),
                             F.lit("SUPERSEDE").alias("__action")))
        plan = inserts.unionByName(supersede, allowMissingColumns=True)

    # 2. One atomic MERGE keyed on bronze_row_id.
    store.merge(TABLE, plan, ["bronze_row_id"],
                update_set={"line_status": f"'{LineStatus.SUPERSEDED}'",
                            "superseded_by_sequence": "s.superseded_by_sequence",
                            "status_updated_at": "s.status_updated_at"},
                update_condition=f"s.__action = 'SUPERSEDE' AND t.line_status = '{LineStatus.CURRENT}'",
                insert=True, insert_condition="s.__action = 'INSERT'")

    # 3. A successful correction of a key resolves older open rejections of that key.
    if applied_keys is not None:
        _resolve_by_correction(ctx, batch_id, seq, applied_keys)

    counts = {r["line_status"]: r["count"] for r in
              store.read(TABLE).filter(F.col("batch_id") == batch_id).groupBy("line_status").count().collect()}
    return {
        "quarantined_row_count": invalid.count(),
        "held_line_count": counts.get(LineStatus.HELD, 0),
        "stale_line_count": counts.get(LineStatus.STALE_ON_ARRIVAL, 0),
    }


def _record_rejections(ctx, batch_id, seq, rejected_keys, versions, hold_whole_batch):
    rows = (rejected_keys.filter(F.col("InvoiceNo").isNotNull() & F.col("StockCode").isNotNull()
                                 & (F.trim("InvoiceNo") != "") & (F.trim("StockCode") != ""))
            .join(versions, KEY, "left")
            .select(
                F.sha2(F.concat_ws("|", F.lit(batch_id), "InvoiceNo", "StockCode"), 256).alias("rejection_id"),
                F.lit(batch_id).alias("batch_id"), F.lit(seq).cast("long").alias("batch_sequence"),
                "InvoiceNo", "StockCode", "reason",
                # Already overtaken by a newer applied correction: nothing to repair.
                F.when(F.col("_newest_correction") > F.lit(seq), RejectionStatus.RESOLVED)
                 .otherwise(RejectionStatus.OPEN).alias("status"),
                F.lit(None).cast("string").alias("resolved_by_batch"),
                F.when(F.col("_newest_correction") > F.lit(seq),
                       F.concat(F.lit("overtaken by newer applied correction sequence "),
                                F.col("_newest_correction").cast("string"))).alias("resolution_note"),
                F.lit(ctx.now).alias("created_at"),
                F.when(F.col("_newest_correction") > F.lit(seq), F.lit(ctx.now)).alias("resolved_at")))
    if hold_whole_batch:
        whole = ctx.spark.createDataFrame([(batch_id,)], "batch_id string").select(
            F.sha2(F.concat_ws("|", F.lit(batch_id), F.lit("*")), 256).alias("rejection_id"),
            "batch_id", F.lit(seq).cast("long").alias("batch_sequence"),
            F.lit(None).cast("string").alias("InvoiceNo"), F.lit(None).cast("string").alias("StockCode"),
            F.lit("UNATTRIBUTABLE_INVALID_ROW: whole correction batch held").alias("reason"),
            F.lit(RejectionStatus.OPEN).alias("status"), F.lit(None).cast("string").alias("resolved_by_batch"),
            F.lit(None).cast("string").alias("resolution_note"), F.lit(ctx.now).alias("created_at"),
            F.lit(None).cast("timestamp").alias("resolved_at"))
        rows = rows.unionByName(whole)
    # Insert-only: a replay never re-opens or rewrites an existing rejection.
    ctx.store.merge("correction_rejections", rows, ["rejection_id"], insert=True)


def _resolve_by_correction(ctx, batch_id, seq, applied_keys):
    open_older = (ctx.store.read_or_empty("correction_rejections")
                  .filter((F.col("status") == RejectionStatus.OPEN) & (F.col("batch_sequence") < F.lit(seq)))
                  .join(applied_keys, KEY, "left_semi")
                  .select("rejection_id",
                          F.lit(RejectionStatus.RESOLVED).alias("status"),
                          F.lit(batch_id).alias("resolved_by_batch"),
                          F.lit(f"replaced by correction {batch_id} (sequence {seq})").alias("resolution_note"),
                          F.lit(ctx.now).alias("resolved_at")))
    ctx.store.merge("correction_rejections", open_older, ["rejection_id"],
                    update_set={c: f"s.{c}" for c in ["status", "resolved_by_batch", "resolution_note", "resolved_at"]},
                    update_condition=f"t.status = '{RejectionStatus.OPEN}'", insert=False)


def run(ctx: PipelineContext) -> List[str]:
    """Apply every INGESTED batch in ascending batch_sequence order."""
    done = []
    for batch in registry.batches_with_status(ctx, BatchStatus.INGESTED):
        counts = process_batch(ctx, batch)
        registry.set_status(ctx, [batch["batch_id"]], BatchStatus.SILVER_APPLIED,
                            extra={batch["batch_id"]: counts})
        done.append(batch["batch_id"])
    return done
