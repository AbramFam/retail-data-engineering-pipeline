"""Names, schemas and constants shared by every pipeline stage.

Nothing in this package writes outside the configured target schema, and the
Delta store refuses the schemas that hold the original SQL / PySpark tables.
"""
from pyspark.sql.types import (
    BooleanType, DecimalType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)

DEFAULT_TARGET_SCHEMA = "workspace.retail_pipeline_pyspark"

# Schemas that contain pre-existing work. The pipeline must never write here.
PROTECTED_SCHEMAS = frozenset({
    "workspace.default",
    "workspace.retail_pipeline_test",
})

PIPELINE_NAME = "pyspark_retail_pipeline"

SOURCE_COLUMNS = [
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
]

# Accepted InvoiceDate formats (the same two the SQL implementation accepts).
INVOICE_DATE_FORMATS = ("M/d/yyyy H:mm", "yyyy-MM-dd HH:mm:ss")

MANIFEST_SUFFIX = ".manifest.json"

UNKNOWN_PRODUCT = "Unknown Product"


class BatchType:
    NEW_DATA = "NEW_DATA"
    CORRECTION = "CORRECTION"
    ALL = (NEW_DATA, CORRECTION)


class BatchStatus:
    VALIDATION_FAILED = "VALIDATION_FAILED"  # never ingested; may be re-delivered
    INGESTED = "INGESTED"                    # in Bronze, waiting for Silver
    SILVER_APPLIED = "SILVER_APPLIED"        # in Silver, waiting for Gold
    GOLD_PUBLISHED = "GOLD_PUBLISHED"        # Gold rebuilt, waiting for commit
    BLOCKED_REVIEW = "BLOCKED_REVIEW"        # processed, but has open correction rejections
    COMMITTED = "COMMITTED"                  # fully processed, counted by the watermark


class LineStatus:
    CURRENT = "CURRENT"                       # counted by Gold
    SUPERSEDED = "SUPERSEDED"                 # replaced by a later CORRECTION
    STALE_ON_ARRIVAL = "STALE_ON_ARRIVAL"     # older than a correction already applied
    HELD = "HELD_REJECTED_CORRECTION"         # correction key rejected by DQ


class RejectionStatus:
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


def _s(name, dtype=StringType(), nullable=True):
    return StructField(name, dtype, nullable)


TS = TimestampType()
LONG = LongType()
PRICE = DecimalType(18, 4)

BATCH_REGISTRY = StructType([
    _s("batch_id", nullable=False),
    _s("source_file"),
    _s("source_file_path"),
    _s("file_content_hash"),
    _s("file_size_bytes", LONG),
    _s("file_modification_time", TS),
    _s("batch_type"),
    _s("batch_sequence", LONG),
    _s("expected_row_count", LONG),
    _s("ingested_row_count", LONG),
    _s("quarantined_row_count", LONG),
    _s("held_line_count", LONG),
    _s("stale_line_count", LONG),
    _s("source_system"),
    _s("status", nullable=False),
    _s("status_detail"),
    _s("registered_at", TS),
    _s("updated_at", TS),
    _s("last_run_id"),
])

BRONZE_SALES = StructType(
    [_s(c) for c in SOURCE_COLUMNS] + [
        _s("malformed_extra_field"),
        _s("bronze_row_id", nullable=False),
        _s("batch_id", nullable=False),
        _s("batch_type", nullable=False),
        _s("batch_sequence", LONG, False),
        _s("source_file", nullable=False),
        _s("source_file_path"),
        _s("file_content_hash", nullable=False),
        _s("source_row_number", LONG, False),
        _s("source_system"),
        _s("ingestion_timestamp", TS, False),
        _s("ingestion_run_id", nullable=False),
    ]
)

SILVER_SALES = StructType([
    _s("bronze_row_id", nullable=False),
    _s("batch_id", nullable=False),
    _s("batch_type", nullable=False),
    _s("batch_sequence", LONG, False),
    _s("source_file", nullable=False),
    _s("source_row_number", LONG, False),
    _s("InvoiceNo", nullable=False),
    _s("StockCode", nullable=False),
    _s("Description"),
    _s("Quantity", IntegerType(), False),
    _s("InvoiceDate", TS, False),
    _s("UnitPrice", PRICE, False),
    _s("CustomerID"),
    _s("Country"),
    _s("is_valid_customer", BooleanType(), False),
    _s("is_cancellation", BooleanType(), False),
    _s("is_adjustment", BooleanType(), False),
    _s("is_zero_price", BooleanType(), False),
    _s("is_negative_price", BooleanType(), False),
    _s("is_exact_repeat", BooleanType(), False),
    _s("line_status", nullable=False),
    _s("superseded_by_sequence", LONG),
    _s("status_updated_at", TS),
    _s("ingestion_timestamp", TS, False),
    _s("silver_processed_at", TS, False),
    _s("silver_run_id", nullable=False),
])

SILVER_QUARANTINE = StructType(
    [
        _s("bronze_row_id", nullable=False),
        _s("batch_id", nullable=False),
        _s("batch_type", nullable=False),
        _s("batch_sequence", LONG, False),
        _s("source_file", nullable=False),
        _s("source_row_number", LONG, False),
    ]
    + [_s(c) for c in SOURCE_COLUMNS]
    + [
        _s("malformed_extra_field"),
        _s("dq_reasons", nullable=False),
        _s("quarantined_at", TS, False),
        _s("silver_run_id", nullable=False),
    ]
)

CORRECTION_REJECTIONS = StructType([
    _s("rejection_id", nullable=False),
    _s("batch_id", nullable=False),
    _s("batch_sequence", LONG, False),
    _s("InvoiceNo"),
    _s("StockCode"),
    _s("reason", nullable=False),
    _s("status", nullable=False),
    _s("resolved_by_batch"),
    _s("resolution_note"),
    _s("created_at", TS, False),
    _s("resolved_at", TS),
])

PIPELINE_WATERMARK = StructType([
    _s("pipeline_name", nullable=False),
    _s("committed_through_sequence", LONG),
    _s("previous_committed_through_sequence", LONG),
    _s("last_committed_run_id"),
    _s("updated_at", TS),
])

GOLD_KPIS = StructType([
    _s("GrossRevenue", DecimalType(38, 2)),
    _s("ReturnsValue", DecimalType(38, 2)),
    _s("NetRevenue", DecimalType(38, 2)),
    _s("TotalCustomers", LONG),
    _s("TotalOrders", LONG),
    _s("AOV", DecimalType(38, 2)),
    _s("AdjustmentsExcludedValue", DecimalType(38, 2)),
    _s("gold_run_id"),
    _s("published_at", TS),
])

GOLD_MONTHLY_SALES = StructType([_s("Month", TS), _s("NetRevenue", DecimalType(38, 2))])
GOLD_SALES_BY_COUNTRY = StructType([_s("Country"), _s("NetRevenue", DecimalType(38, 2))])
GOLD_TOP_PRODUCTS = StructType([
    _s("Rank", IntegerType()),
    _s("StockCode"),
    _s("Description"),
    _s("GrossRevenue", DecimalType(38, 2)),
    _s("UnitsSold", LONG),
])

TABLE_SCHEMAS = {
    "batch_registry": BATCH_REGISTRY,
    "bronze_sales": BRONZE_SALES,
    "silver_sales": SILVER_SALES,
    "silver_quarantine": SILVER_QUARANTINE,
    "correction_rejections": CORRECTION_REJECTIONS,
    "pipeline_watermark": PIPELINE_WATERMARK,
    "gold_kpis": GOLD_KPIS,
    "gold_monthly_sales": GOLD_MONTHLY_SALES,
    "gold_sales_by_country": GOLD_SALES_BY_COUNTRY,
    "gold_top_products": GOLD_TOP_PRODUCTS,
}
