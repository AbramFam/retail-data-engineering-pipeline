import csv
import json
import os
import shutil
import sys
import time
import uuid

import pytest

# Spark returns timestamps to Python in the machine's local zone; pin it so
# results are the same on any laptop or CI runner (Databricks runs in UTC).
os.environ["TZ"] = "UTC"
time.tzset()

PYSPARK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_ROOT = os.path.dirname(PYSPARK_DIR)
sys.path.insert(0, PYSPARK_DIR)

from pyspark.sql import SparkSession  # noqa: E402

from retail_pipeline.config import SOURCE_COLUMNS  # noqa: E402
from retail_pipeline.context import PipelineContext  # noqa: E402
from retail_pipeline.storage import DeltaStore, ParquetStore  # noqa: E402

RUN_DELTA = os.environ.get("RUN_DELTA_TESTS") == "1"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    builder = (SparkSession.builder.master("local[2]").appName("retail-pipeline-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse"))))
    if RUN_DELTA:
        from delta import configure_spark_with_delta_pip
        builder = (builder
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(params=["parquet", pytest.param("delta", marks=pytest.mark.delta)])
def store(request, spark, tmp_path):
    if request.param == "parquet":
        yield ParquetStore(spark, str(tmp_path / "tables"))
        return
    if not RUN_DELTA:
        pytest.skip("Delta tests need RUN_DELTA_TESTS=1 (Delta jars are downloaded from Maven; runs in CI)")
    schema = f"retail_ci_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {schema}")
    yield DeltaStore(spark, schema)
    spark.sql(f"DROP DATABASE {schema} CASCADE")


@pytest.fixture
def ctx(spark, store):
    return PipelineContext(spark, store)


@pytest.fixture
def landing(tmp_path):
    d = tmp_path / "landing"
    d.mkdir()
    return Lander(str(d))


class Lander:
    """Writes CSV files + manifests into a landing directory."""

    def __init__(self, directory):
        self.dir = directory

    def path(self, name):
        return os.path.join(self.dir, name)

    def csv(self, name, rows, header=SOURCE_COLUMNS, newline="\n", bom=False):
        with open(self.path(name), "w", newline="", encoding="utf-8") as fh:
            if bom:
                fh.write("﻿")
            w = csv.writer(fh, lineterminator=newline)
            if header is not None:
                w.writerow(header)
            for r in rows:
                w.writerow(r)
        return self.path(name)

    def manifest(self, name, batch_type, seq, expected=None, rows=None, **extra):
        body = {"batch_type": batch_type, "batch_sequence": seq,
                "expected_row_count": expected if expected is not None else len(rows or []), **extra}
        with open(self.path(name) + ".manifest.json", "w") as fh:
            json.dump(body, fh)

    def batch(self, name, batch_type, seq, rows, **kw):
        self.csv(name, rows, **{k: v for k, v in kw.items() if k in ("header", "newline", "bom")})
        self.manifest(name, batch_type, seq, rows=rows)
        return self.path(name)

    def copy(self, src, name=None):
        dst = self.path(name or os.path.basename(src))
        shutil.copyfile(src, dst)
        return dst

    def remove(self, name):
        for p in (self.path(name), self.path(name) + ".manifest.json"):
            if os.path.exists(p):
                os.remove(p)


def line(inv, stock, qty, price, cust="17850", desc="ITEM", date="2026-08-01 10:00:00", country="Cyprus"):
    """One source row in CSV column order."""
    return [inv, stock, desc, str(qty), date, str(price), cust, country]
