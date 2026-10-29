"""Commit step: final batch statuses + watermark. Runs last, after Gold succeeded.

Watermark definition
--------------------
``committed_through_sequence`` = the highest batch_sequence S such that every
accepted batch with sequence <= S is COMMITTED. A batch with an open
correction rejection is BLOCKED_REVIEW, so the watermark cannot move past it,
even if later batches succeed. If a late batch arrives with a sequence below
the current watermark and is blocked, the watermark moves *back* to expose it
(recorded in previous_committed_through_sequence), instead of hiding it.
"""
from typing import Dict, List, Optional

from pyspark.sql import functions as F

from . import registry
from .config import PIPELINE_NAME, BatchStatus, RejectionStatus
from .context import PipelineContext


class OpenReviewItemsError(RuntimeError):
    """Raised after committing when correction rejections still need review."""


def open_rejections(ctx: PipelineContext):
    return ctx.store.read_or_empty("correction_rejections").filter(F.col("status") == RejectionStatus.OPEN)


def compute_watermark(batches: List) -> Optional[int]:
    through = None
    for b in sorted(batches, key=lambda r: r["batch_sequence"]):
        if b["status"] != BatchStatus.COMMITTED:
            break
        through = b["batch_sequence"]
    return through


def run(ctx: PipelineContext) -> Dict:
    candidates = registry.batches_with_status(ctx, BatchStatus.GOLD_PUBLISHED, BatchStatus.BLOCKED_REVIEW)
    open_by_batch = {r["batch_id"]: r["count"] for r in open_rejections(ctx).groupBy("batch_id").count().collect()}

    committed = [b["batch_id"] for b in candidates if b["batch_id"] not in open_by_batch]
    blocked = [b["batch_id"] for b in candidates if b["batch_id"] in open_by_batch]
    if committed:
        registry.set_status(ctx, committed, BatchStatus.COMMITTED)
    for b in blocked:
        registry.set_status(ctx, [b], BatchStatus.BLOCKED_REVIEW,
                            detail=f"{open_by_batch[b]} open correction rejection(s); see correction_rejections")

    previous_row = ctx.store.read_or_empty("pipeline_watermark").filter(
        F.col("pipeline_name") == PIPELINE_NAME).first()
    previous = previous_row["committed_through_sequence"] if previous_row else None
    through = compute_watermark(registry.active(ctx).collect())
    row = ctx.spark.createDataFrame(
        [(PIPELINE_NAME, through, previous, ctx.run_id, ctx.now)],
        "pipeline_name string, committed_through_sequence long, previous_committed_through_sequence long, "
        "last_committed_run_id string, updated_at timestamp")
    ctx.store.merge("pipeline_watermark", row, ["pipeline_name"],
                    update_set={c: f"s.{c}" for c in row.columns if c != "pipeline_name"})

    summary = {"committed": committed, "blocked": blocked, "watermark": through, "previous_watermark": previous}
    if open_by_batch and ctx.fail_on_open_issues:
        raise OpenReviewItemsError(
            f"Watermark held at {through}: open correction rejections in {sorted(open_by_batch)}. "
            "Resolve with a newer CORRECTION batch or retail_pipeline.commit.resolve_rejection().")
    return summary


def resolve_rejection(ctx: PipelineContext, rejection_id: str, note: str) -> None:
    """Manual resolution after review (e.g. the rejected correction is withdrawn)."""
    if not note or not note.strip():
        raise ValueError("A resolution note is required")
    row = ctx.spark.createDataFrame(
        [(rejection_id, RejectionStatus.RESOLVED, None, f"MANUAL: {note.strip()}", ctx.now)],
        "rejection_id string, status string, resolved_by_batch string, resolution_note string, resolved_at timestamp")
    exists = open_rejections(ctx).filter(F.col("rejection_id") == rejection_id).count()
    if not exists:
        raise ValueError(f"No OPEN rejection with id {rejection_id}")
    ctx.store.merge("correction_rejections", row, ["rejection_id"],
                    update_set={c: f"s.{c}" for c in ["status", "resolved_by_batch", "resolution_note", "resolved_at"]},
                    update_condition=f"t.status = '{RejectionStatus.OPEN}'", insert=False)
