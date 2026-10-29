import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from .storage import TableStore


@dataclass
class PipelineContext:
    spark: SparkSession
    store: TableStore
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    # One timestamp per run: every row written by this run carries it.
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0))
    default_source_system: str = "retail_sales"
    # When True the commit step raises if any correction rejection is still
    # open, so the job shows as failed until someone reviews it.
    fail_on_open_issues: bool = True

    def new_run(self) -> "PipelineContext":
        """Same store/settings, new run id and timestamp (used by tests to simulate reruns)."""
        return PipelineContext(self.spark, self.store, default_source_system=self.default_source_system,
                               fail_on_open_issues=self.fail_on_open_issues)
