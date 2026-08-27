-- Run this file first, before 01-04.

-- Bronze: raw ingested rows + ingestion metadata.
CREATE TABLE IF NOT EXISTS bronze_sales_v2 (
  InvoiceNo            STRING,
  StockCode            STRING,
  Description          STRING,
  Quantity             STRING,
  InvoiceDate          STRING,
  UnitPrice            STRING,
  CustomerID           STRING,
  Country              STRING,
  source_file          STRING,
  ingestion_timestamp  TIMESTAMP,
  batch_id             STRING,
  source_system        STRING
)
USING delta;

-- Silver: cleaned, typed data. Business key is InvoiceNo + StockCode.
CREATE TABLE IF NOT EXISTS silver_sales_v2 (
  InvoiceNo            STRING,
  StockCode            STRING,
  Description          STRING,
  Quantity             INT,
  InvoiceDate          TIMESTAMP,
  UnitPrice            DECIMAL(10,2),
  CustomerID           STRING,
  Country              STRING,
  source_file          STRING,
  ingestion_timestamp  TIMESTAMP,
  batch_id             STRING,
  source_system        STRING
)
USING delta;

-- Tracks the last successfully processed Bronze ingestion timestamp.
CREATE TABLE IF NOT EXISTS pipeline_watermark (
  pipeline_name              STRING,
  last_processed_timestamp   TIMESTAMP
)
USING delta;

-- Original seed was: INSERT INTO pipeline_watermark
-- VALUES ('bronze_to_silver_sales', TIMESTAMP '2026-01-01 00:00:00');
-- Rewritten as a MERGE so rerunning this file doesn't insert a duplicate
-- row or reset the watermark once it has advanced past 2026-01-01.
MERGE INTO pipeline_watermark AS target
USING (
    SELECT
        'bronze_to_silver_sales' AS pipeline_name,
        TIMESTAMP '2026-01-01 00:00:00' AS last_processed_timestamp
) AS source
ON target.pipeline_name = source.pipeline_name
WHEN NOT MATCHED THEN
    INSERT (pipeline_name, last_processed_timestamp)
    VALUES (source.pipeline_name, source.last_processed_timestamp);
