COPY INTO bronze_sales_v2
FROM (
    SELECT
        InvoiceNo,
        StockCode,
        Description,
        Quantity,
        InvoiceDate,
        UnitPrice,
        CustomerID,
        Country,
        _metadata.file_name AS source_file,
        current_timestamp() AS ingestion_timestamp,
        REGEXP_EXTRACT(
            _metadata.file_name,
            '(batch_[0-9]+)',
            1
        ) AS batch_id,
        'retail_sales' AS source_system
    FROM '/Volumes/workspace/default/raw_data/daily_sales'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true'
);