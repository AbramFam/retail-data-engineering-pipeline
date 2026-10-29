"""Batch-level processing status (one row per source file)."""
from typing import Dict, Iterable, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import BATCH_REGISTRY, BatchStatus
from .context import PipelineContext

TABLE = "batch_registry"


def read(ctx: PipelineContext) -> DataFrame:
    return ctx.store.read_or_empty(TABLE)


def upsert(ctx: PipelineContext, rows: List[Dict]) -> None:
    """Insert or update registry rows. Only the keys present in the dicts are updated."""
    if not rows:
        return
    provided = sorted({k for r in rows for k in r})
    for r in rows:
        missing = set(provided) - set(r)
        if missing:
            raise ValueError(f"All rows in one upsert must provide the same columns; missing {missing}")
    full = [{f.name: r.get(f.name) for f in BATCH_REGISTRY.fields} for r in rows]
    for r in full:
        r["updated_at"] = r["updated_at"] or ctx.now
        r["last_run_id"] = r["last_run_id"] or ctx.run_id
    df = ctx.spark.createDataFrame(full, BATCH_REGISTRY)
    update_cols = set(provided) | {"updated_at", "last_run_id"}
    update_cols.discard("batch_id")
    update_cols.discard("registered_at")  # first registration time is never overwritten
    ctx.store.merge(TABLE, df, ["batch_id"],
                    update_set={c: f"s.`{c}`" for c in sorted(update_cols)})


def set_status(ctx: PipelineContext, batch_ids: Iterable[str], status: str, detail: str = None,
               extra: Dict[str, Dict] = None) -> None:
    rows = []
    for b in batch_ids:
        row = {"batch_id": b, "status": status, "status_detail": detail}
        row.update((extra or {}).get(b, {}))
        rows.append(row)
    # Group by provided column set so upsert receives homogeneous rows.
    groups: Dict[tuple, List[Dict]] = {}
    for r in rows:
        groups.setdefault(tuple(sorted(r)), []).append(r)
    for group in groups.values():
        upsert(ctx, group)


def batches_with_status(ctx: PipelineContext, *statuses: str) -> List:
    return (read(ctx).filter(F.col("status").isin(list(statuses)))
            .orderBy("batch_sequence", "batch_id").collect())


def active(ctx: PipelineContext) -> DataFrame:
    """Every batch that was actually accepted (anything but VALIDATION_FAILED)."""
    return read(ctx).filter(F.col("status") != BatchStatus.VALIDATION_FAILED)
