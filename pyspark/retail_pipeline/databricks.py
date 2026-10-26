"""Helpers used by the Databricks notebooks in pyspark/notebooks/."""
from .config import DEFAULT_TARGET_SCHEMA
from .context import PipelineContext
from .storage import DeltaStore

DEFAULT_LANDING = "/Volumes/workspace/retail_pipeline_pyspark/landing/incoming"


def widgets(dbutils):
    dbutils.widgets.text("target_schema", DEFAULT_TARGET_SCHEMA)
    dbutils.widgets.text("landing_dir", DEFAULT_LANDING)
    dbutils.widgets.text("run_id", "")  # pass {{job.run_id}} from the Job so all tasks share it
    return {k: dbutils.widgets.get(k) for k in ("target_schema", "landing_dir", "run_id")}


def context(spark, params) -> PipelineContext:
    ctx = PipelineContext(spark, DeltaStore(spark, params["target_schema"]))
    if params.get("run_id"):
        ctx.run_id = params["run_id"]
    return ctx
