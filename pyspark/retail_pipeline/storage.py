"""Table storage behind one small interface.

* DeltaStore   - Databricks (and the CI Delta tests). Real Delta MERGE.
* ParquetStore - local development / unit tests when Delta jars are not
                 available. Each write produces a new immutable version
                 directory and then swaps a pointer, so a crash mid-write
                 leaves the previous version intact (mirrors a Delta commit).

Merge expressions use the aliases ``t`` (target) and ``s`` (source), exactly
as in a Delta MERGE statement, so both stores run the same merge plans.
"""
import os
import uuid
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import PROTECTED_SCHEMAS, TABLE_SCHEMAS


class StorageSafetyError(RuntimeError):
    """Raised when a write would touch a table outside the isolated schema."""


def align(df: DataFrame, name: str) -> DataFrame:
    """Select the table's columns in order, cast to the table's types."""
    _check_known(name)  # refuse unknown tables before anything else (StorageSafetyError, not KeyError)
    schema = TABLE_SCHEMAS[name]
    cols = []
    for field in schema.fields:
        if field.name in df.columns:
            cols.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            cols.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*cols)


def _check_known(name: str) -> None:
    if name not in TABLE_SCHEMAS:
        raise StorageSafetyError(f"Unknown table '{name}'. Only pipeline tables may be written.")


def _assert_unique_source(source: DataFrame, key_cols) -> None:
    dupes = source.groupBy(*key_cols).count().filter("count > 1").limit(1).count()
    if dupes:
        raise ValueError(f"MERGE source has duplicate keys on {key_cols}; refusing to merge.")


class TableStore:
    spark: SparkSession

    def exists(self, name: str) -> bool: ...
    def read(self, name: str) -> DataFrame: ...
    def overwrite(self, name: str, df: DataFrame) -> None: ...
    def append(self, name: str, df: DataFrame) -> None: ...

    def merge(self, name, source, key_cols, update_set=None, update_condition=None,
              insert=True, insert_condition=None) -> None: ...

    def ensure(self, name: str) -> None:
        _check_known(name)
        if not self.exists(name):
            empty = self.spark.createDataFrame([], TABLE_SCHEMAS[name])
            self.overwrite(name, empty)

    def ensure_all(self) -> None:
        for name in TABLE_SCHEMAS:
            self.ensure(name)

    def read_or_empty(self, name: str) -> DataFrame:
        if self.exists(name):
            return self.read(name)
        return self.spark.createDataFrame([], TABLE_SCHEMAS[name])


class DeltaStore(TableStore):
    """Delta tables named ``{target_schema}.{logical_name}``."""

    def __init__(self, spark: SparkSession, target_schema: str):
        if target_schema.strip().lower() in {s.lower() for s in PROTECTED_SCHEMAS}:
            raise StorageSafetyError(
                f"Refusing to use protected schema '{target_schema}'. "
                "The PySpark pipeline only writes to its own isolated schema."
            )
        self.spark = spark
        self.target_schema = target_schema

    def fqn(self, name: str) -> str:
        _check_known(name)
        return f"{self.target_schema}.{name}"

    def exists(self, name):
        return self.spark.catalog.tableExists(self.fqn(name))

    def read(self, name):
        return self.spark.table(self.fqn(name))

    def overwrite(self, name, df):
        align(df, name).write.format("delta").mode("overwrite").saveAsTable(self.fqn(name))

    def append(self, name, df):
        align(df, name).write.format("delta").mode("append").saveAsTable(self.fqn(name))

    def merge(self, name, source, key_cols, update_set=None, update_condition=None,
              insert=True, insert_condition=None):
        from delta.tables import DeltaTable  # imported lazily: not needed for ParquetStore

        _assert_unique_source(source, key_cols)
        self.ensure(name)
        target_cols = [f.name for f in TABLE_SCHEMAS[name].fields]
        cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in key_cols)
        builder = DeltaTable.forName(self.spark, self.fqn(name)).alias("t").merge(source.alias("s"), cond)
        if update_set:
            builder = builder.whenMatchedUpdate(condition=update_condition, set=update_set)
        if insert:
            values = {c: f"s.`{c}`" for c in target_cols if c in source.columns}
            builder = builder.whenNotMatchedInsert(condition=insert_condition, values=values)
        builder.execute()


class ParquetStore(TableStore):
    """Versioned Parquet tables under a local directory (tests / local dev)."""

    def __init__(self, spark: SparkSession, base_dir: str):
        self.spark = spark
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def _pointer(self, name):
        return os.path.join(self.base_dir, name, "_CURRENT")

    def _current_path(self, name):
        p = self._pointer(name)
        if not os.path.exists(p):
            return None
        with open(p) as fh:
            return os.path.join(self.base_dir, name, fh.read().strip())

    def exists(self, name):
        _check_known(name)
        return self._current_path(name) is not None

    def read(self, name):
        _check_known(name)
        path = self._current_path(name)
        if path is None:
            raise FileNotFoundError(f"Table '{name}' does not exist")
        return self.spark.read.schema(TABLE_SCHEMAS[name]).parquet(path)

    def _commit(self, name, df):
        _check_known(name)
        version = f"v_{uuid.uuid4().hex}"
        path = os.path.join(self.base_dir, name, version)
        align(df, name).write.mode("error").parquet(path)
        tmp = self._pointer(name) + ".tmp"
        with open(tmp, "w") as fh:
            fh.write(version)
        os.replace(tmp, self._pointer(name))  # atomic pointer swap = commit

    def overwrite(self, name, df):
        self._commit(name, df)

    def append(self, name, df):
        base = self.read_or_empty(name)
        self._commit(name, align(base, name).unionByName(align(df, name)))

    def merge(self, name, source, key_cols, update_set=None, update_condition=None,
              insert=True, insert_condition=None):
        _assert_unique_source(source, key_cols)
        self.ensure(name)
        schema = TABLE_SCHEMAS[name]
        t = self.read(name).alias("t")
        s = source.withColumn("__matched", F.lit(True)).alias("s")
        cond = reduce(lambda a, b: a & b, [F.col(f"t.{k}") == F.col(f"s.{k}") for k in key_cols])
        joined = t.join(s, cond, "left")
        matched = F.col("s.__matched").isNotNull()
        do_update = matched & F.expr(update_condition) if update_condition else matched
        new_cols = []
        for field in schema.fields:
            current = F.col(f"t.{field.name}")
            if update_set and field.name in update_set:
                value = F.when(do_update, F.expr(update_set[field.name]).cast(field.dataType)).otherwise(current)
            else:
                value = current
            new_cols.append(value.alias(field.name))
        updated = joined.select(*new_cols)
        result = updated
        if insert:
            src = source.alias("s")
            if insert_condition:
                src = src.filter(F.expr(insert_condition))
            new_rows = src.join(self.read(name).select(*key_cols), list(key_cols), "left_anti")
            result = updated.unionByName(align(new_rows, name))
        # Materialise before swapping the pointer (the source of `updated` is the old version).
        self._commit(name, result)
