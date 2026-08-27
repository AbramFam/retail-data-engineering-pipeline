CREATE OR REPLACE TABLE gold_kpis_v2 AS

SELECT
    ROUND(
        SUM(
            CASE
                WHEN Quantity > 0 THEN Quantity * UnitPrice
                ELSE 0
            END
        ),
        2
    ) AS GrossRevenue,

    ROUND(
        SUM(Quantity * UnitPrice),
        2
    ) AS NetRevenue,

    ROUND(
        ABS(
            SUM(
                CASE
                    WHEN Quantity < 0 THEN Quantity * UnitPrice
                    ELSE 0
                END
            )
        ),
        2
    ) AS ReturnsValue,

    COUNT(
        DISTINCT CASE
            WHEN CustomerID > 0 THEN CustomerID
        END
    ) AS TotalCustomers,

    ROUND(
        SUM(Quantity * UnitPrice)
        / COUNT(DISTINCT InvoiceNo),
        2
    ) AS AOV,

    COUNT(DISTINCT InvoiceNo) AS TotalOrders

FROM silver_sales_v2;


-- gold_monthly_sales_v2
CREATE OR REPLACE TABLE gold_monthly_sales_v2 AS
SELECT
    DATE_TRUNC('MONTH', InvoiceDate) AS Month,
    ROUND(SUM(Quantity * UnitPrice), 2) AS NetRevenue
FROM silver_sales_v2
GROUP BY DATE_TRUNC('MONTH', InvoiceDate)
ORDER BY Month;


-- gold_sales_by_country_v2
CREATE OR REPLACE TABLE gold_sales_by_country_v2 AS

SELECT
    Country,
    ROUND(SUM(Quantity * UnitPrice), 2) AS NetRevenue
FROM silver_sales_v2
GROUP BY Country
ORDER BY NetRevenue DESC;


-- gold_top_products_v2
CREATE OR REPLACE TABLE gold_top_products_v2 AS

SELECT
    StockCode,
    Description,
    ROUND(SUM(Quantity * UnitPrice), 2) AS NetRevenue
FROM silver_sales_v2
WHERE Quantity > 0
GROUP BY StockCode, Description
ORDER BY NetRevenue DESC
LIMIT 10;
