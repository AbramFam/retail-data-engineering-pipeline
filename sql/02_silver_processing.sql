MERGE INTO silver_sales_v2 AS target

USING (
    SELECT
        InvoiceNo,
        StockCode,
        COALESCE(Description, 'Unknown Product') AS Description,
        CAST(Quantity AS INT) AS Quantity,
        COALESCE(
            try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm'),
            try_to_timestamp(InvoiceDate, 'yyyy-MM-dd HH:mm:ss')
        ) AS InvoiceDate,
        CAST(UnitPrice AS DECIMAL(10,2)) AS UnitPrice,
        CustomerID,
        Country,
        source_file,
        ingestion_timestamp,
        batch_id,
        source_system
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY InvoiceNo, StockCode
                ORDER BY ingestion_timestamp DESC
            ) AS rn
        FROM bronze_sales_v2
        WHERE ingestion_timestamp > (
            SELECT last_processed_timestamp
            FROM pipeline_watermark
            WHERE pipeline_name = 'bronze_to_silver_sales'
        )
    ) AS latest
    WHERE rn = 1
) AS source

ON target.InvoiceNo = source.InvoiceNo
AND target.StockCode = source.StockCode

WHEN MATCHED AND NOT (
    target.Description <=> source.Description
    AND target.Quantity <=> source.Quantity
    AND target.InvoiceDate <=> source.InvoiceDate
    AND target.UnitPrice <=> source.UnitPrice
    AND target.CustomerID <=> source.CustomerID
    AND target.Country <=> source.Country
)
THEN UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *;
