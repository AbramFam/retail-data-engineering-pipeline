UPDATE pipeline_watermark
SET last_processed_timestamp = (
    SELECT MAX(ingestion_timestamp)
    FROM bronze_sales_v2
)
WHERE pipeline_name = 'bronze_to_silver_sales';
