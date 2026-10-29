"""Bronze: validate each landed file + manifest, then append its rows untouched.

Row identity
------------
``source_row_number`` is the physical line index in the file (header = 0,
first data line = 1). It is derived from the file *content*, not from Spark
task/partition layout: the whole file is read as one binary value, split on
line breaks and exploded with ``posexplode`` (array position = line index).
The same bytes always produce the same numbers, on any cluster size, and it
works under Spark Connect / Databricks serverless (no RDD API).
``monotonically_increasing_id`` is deliberately not used: its values depend
on partitioning and are not a file row number.

``bronze_row_id = sha2(file_content_hash || ':' || source_row_number)`` is
unique by construction and stable across replays.

Limit: CSV fields containing embedded line breaks are rejected (detected by
an odd number of quote characters on a line) rather than mis-numbered.
"""
import csv
import io
import json
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import registry
from .config import MANIFEST_SUFFIX, SOURCE_COLUMNS, BatchStatus, BatchType
from .context import PipelineContext

TABLE = "bronze_sales"
_CSV_DDL = ", ".join(f"`{c}` STRING" for c in SOURCE_COLUMNS) + ", `malformed_extra_field` STRING"


class BatchValidationError(RuntimeError):
    """A landed file failed validation. Nothing from this run was ingested."""


class ImmutabilityError(BatchValidationError):
    """A file that was already ingested arrived again with different content or manifest."""


@dataclass
class SourceFile:
    batch_id: str
    path: str
    file_hash: str
    size: int
    modification_time: object
    header: List[str]
    rows: DataFrame           # source_row_number + raw columns
    row_count: int


@dataclass
class Manifest:
    batch_type: str
    batch_sequence: int
    expected_row_count: int
    source_system: Optional[str]


def read_manifest(csv_path: str) -> Manifest:
    path = csv_path + MANIFEST_SUFFIX
    if not os.path.exists(path):
        raise BatchValidationError(f"missing manifest {os.path.basename(path)}")
    try:
        with open(path) as fh:
            m = json.load(fh)
    except json.JSONDecodeError as exc:
        raise BatchValidationError(f"manifest is not valid JSON: {exc}") from exc
    batch_type = m.get("batch_type")
    if batch_type not in BatchType.ALL:
        raise BatchValidationError(f"unknown batch_type {batch_type!r}; expected one of {BatchType.ALL}")
    seq = m.get("batch_sequence")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq <= 0:
        raise BatchValidationError(f"batch_sequence must be a positive integer, got {seq!r}")
    expected = m.get("expected_row_count")
    if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
        raise BatchValidationError(f"expected_row_count must be a non-negative integer, got {expected!r}")
    return Manifest(batch_type, seq, expected, m.get("source_system"))


def read_source_file(ctx: PipelineContext, path: str) -> SourceFile:
    """Read one CSV deterministically. Raises BatchValidationError on structural problems."""
    raw = ctx.spark.read.format("binaryFile").load(path)
    meta = raw.select("length", "modificationTime", F.sha2("content", 256).alias("h")).first()
    if meta is None:
        raise BatchValidationError("file could not be read")
    text = F.regexp_replace(F.decode("content", "UTF-8"), "^﻿", "")  # drop UTF-8 BOM
    lines = (raw.select(F.posexplode(F.split(text, "\r?\n")).alias("pos", "line"))
                .select(F.col("pos").cast("long").alias("source_row_number"), "line"))
    header_row = lines.filter("source_row_number = 0").first()
    header = next(csv.reader(io.StringIO(header_row["line"]))) if header_row and header_row["line"] else []
    if [h.strip() for h in header] != SOURCE_COLUMNS:
        raise BatchValidationError(f"header {header} does not match expected columns {SOURCE_COLUMNS}")
    data = lines.filter((F.col("source_row_number") > 0) & (F.trim("line") != ""))
    quote_count = F.length("line") - F.length(F.regexp_replace("line", '"', ""))
    broken = data.filter(quote_count % 2 == 1).select("source_row_number").limit(5).collect()
    if broken:
        raise BatchValidationError(
            "unbalanced quotes (embedded line break?) on line(s) "
            f"{[r[0] for r in broken]}; multi-line CSV fields are not supported")
    parsed = (data.select("source_row_number", F.from_csv("line", _CSV_DDL, {"mode": "PERMISSIVE"}).alias("r"))
                  .select("source_row_number", "r.*"))
    return SourceFile(
        batch_id=os.path.basename(path), path=path, file_hash=meta["h"], size=meta["length"],
        modification_time=meta["modificationTime"], header=header, rows=parsed,
        row_count=parsed.count(),
    )


def discover(landing_dir: str) -> List[str]:
    return sorted(os.path.join(landing_dir, f) for f in os.listdir(landing_dir)
                  if f.lower().endswith(".csv") and os.path.isfile(os.path.join(landing_dir, f)))


def run(ctx: PipelineContext, landing_dir: str) -> Dict[str, List[str]]:
    """Validate every landed file first; ingest only if all of them are acceptable."""
    known = {r["batch_id"]: r for r in registry.active(ctx).collect()}
    by_hash = {r["file_content_hash"]: r["batch_id"] for r in known.values()}
    by_seq = {r["batch_sequence"]: r["batch_id"] for r in known.values()}

    to_ingest, skipped, failures, conflicts = [], [], [], []
    seen_seq: Dict[int, str] = {}
    for path in discover(landing_dir):
        name = os.path.basename(path)
        try:
            manifest = read_manifest(path)
            src = read_source_file(ctx, path)
        except BatchValidationError as exc:
            failures.append((name, path, str(exc)))
            continue

        prior = known.get(name)
        if prior is not None:
            if prior["file_content_hash"] != src.file_hash:
                conflicts.append(f"{name}: already ingested with different content (immutable file changed)")
            elif (prior["batch_type"], prior["batch_sequence"], prior["expected_row_count"]) != (
                    manifest.batch_type, manifest.batch_sequence, manifest.expected_row_count):
                conflicts.append(f"{name}: manifest differs from the one it was ingested with")
            else:
                skipped.append(name)  # replay of an already-ingested file: no-op
            continue
        if src.file_hash in by_hash:
            conflicts.append(f"{name}: identical content already ingested as {by_hash[src.file_hash]}")
            continue
        owner = by_seq.get(manifest.batch_sequence) or seen_seq.get(manifest.batch_sequence)
        if owner:
            conflicts.append(f"{name}: batch_sequence {manifest.batch_sequence} already used by {owner}")
            continue
        if src.row_count != manifest.expected_row_count:
            failures.append((name, path, f"row count {src.row_count} != manifest expected_row_count "
                                         f"{manifest.expected_row_count}"))
            continue
        seen_seq[manifest.batch_sequence] = name
        to_ingest.append((src, manifest))

    if failures:
        registry.upsert(ctx, [{
            "batch_id": name, "source_file": name, "source_file_path": path,
            "status": BatchStatus.VALIDATION_FAILED, "status_detail": reason, "registered_at": ctx.now,
        } for name, path, reason in failures])
    if failures or conflicts:
        problems = conflicts + [f"{n}: {r}" for n, _, r in failures]
        raise BatchValidationError("Bronze stopped; nothing ingested. Review required:\n  - "
                                   + "\n  - ".join(problems))

    for src, manifest in to_ingest:
        _ingest(ctx, src, manifest)
    return {"ingested": [s.batch_id for s, _ in to_ingest], "skipped": skipped}


def _ingest(ctx: PipelineContext, src: SourceFile, manifest: Manifest) -> None:
    rows = (src.rows
            .withColumn("file_content_hash", F.lit(src.file_hash))
            .withColumn("bronze_row_id",
                        F.sha2(F.concat_ws(":", F.col("file_content_hash"), F.col("source_row_number").cast("string")), 256))
            .withColumn("batch_id", F.lit(src.batch_id))
            .withColumn("batch_type", F.lit(manifest.batch_type))
            .withColumn("batch_sequence", F.lit(manifest.batch_sequence).cast("long"))
            .withColumn("source_file", F.lit(src.batch_id))
            .withColumn("source_file_path", F.lit(src.path))
            .withColumn("source_system", F.lit(manifest.source_system or ctx.default_source_system))
            .withColumn("ingestion_timestamp", F.lit(ctx.now))
            .withColumn("ingestion_run_id", F.lit(ctx.run_id)))
    # Insert-only merge on the constructed id: a crash between this write and the
    # registry update below is safe to replay (no duplicate Bronze rows).
    ctx.store.merge(TABLE, rows, ["bronze_row_id"], insert=True)
    registry.upsert(ctx, [{
        "batch_id": src.batch_id, "source_file": src.batch_id, "source_file_path": src.path,
        "file_content_hash": src.file_hash, "file_size_bytes": src.size,
        "file_modification_time": src.modification_time, "batch_type": manifest.batch_type,
        "batch_sequence": manifest.batch_sequence, "expected_row_count": manifest.expected_row_count,
        "ingested_row_count": src.row_count, "source_system": manifest.source_system or ctx.default_source_system,
        "status": BatchStatus.INGESTED, "status_detail": None, "registered_at": ctx.now,
    }])
