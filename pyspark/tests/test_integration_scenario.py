"""Runs the Databricks Phase 3 scenario script locally, so CI keeps it working."""
import pytest

from retail_pipeline import registry
from retail_pipeline.context import PipelineContext
from scripts.integration_scenario import run_scenario


def test_integration_scenario_all_checks_pass(spark, store, tmp_path):
    results = run_scenario(spark, store, str(tmp_path / "landing"))
    failed = [(step, name) for step, name, ok in results if not ok]
    assert len(results) == 16 and not failed, failed


def test_integration_scenario_refuses_non_empty_tables(spark, store, tmp_path):
    registry.upsert(PipelineContext(spark, store), [{"batch_id": "existing.csv", "status": "COMMITTED"}])
    with pytest.raises(RuntimeError, match="not empty"):
        run_scenario(spark, store, str(tmp_path / "landing"))
