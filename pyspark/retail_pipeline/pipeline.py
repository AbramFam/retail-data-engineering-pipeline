"""Run the four stages in order. Each stage can also be run on its own (one Databricks task each)."""
from typing import Dict

from . import bronze, commit, gold, silver
from .context import PipelineContext


def run_all(ctx: PipelineContext, landing_dir: str) -> Dict:
    result = {"bronze": bronze.run(ctx, landing_dir)}
    result["silver"] = silver.run(ctx)
    result["gold"] = gold.run(ctx)
    result["commit"] = commit.run(ctx)
    return result
