# Retail Data Engineering Pipeline

This project is an automated incremental data pipeline built in Databricks using the Medallion Architecture.

The pipeline ingests retail sales CSV files and processes the data through Bronze, Silver, and Gold layers. It uses incremental processing, watermarking, MERGE operations, and Databricks Workflows to automate the full pipeline.

## Architecture

The pipeline follows the Medallion Architecture:

`CSV Files → Bronze → Silver → Gold → Business KPIs`

- **Bronze:** Ingests the raw CSV files and adds metadata such as the source file, ingestion timestamp, batch ID, and source system.
- **Silver:** Cleans and standardizes the data, selects the latest record, and uses MERGE to insert new records or update existing ones.
- **Gold:** Creates business-ready tables for KPIs, monthly sales, country sales, and top products.

## Technologies Used

- Databricks
- SQL
- Delta Lake
- Databricks Workflows
- Databricks Volumes
- Git and GitHub

## Pipeline Automation

The pipeline is orchestrated using a Databricks Job with the following task order:

`bronze_ingestion → silver_processing → gold_tables → watermark_update`

A file-arrival trigger starts the pipeline automatically when a new CSV file is uploaded to the Databricks Volume.

The watermark is updated only after the Bronze, Silver, and Gold tasks finish successfully. This prevents unprocessed data from being skipped if an upstream task fails.

## Incremental Processing

- `COPY INTO` ingests only files that have not already been processed.
- A watermark stores the latest successfully processed ingestion timestamp.
- Silver processes only Bronze records newer than the current watermark.
- `ROW_NUMBER()` selects the latest version of each record using `InvoiceNo + StockCode` as the business key.
- `MERGE` updates matched records and inserts new records into the Silver table.

## Gold Layer Outputs

The Gold layer creates the following tables:

- `gold_kpis_v2`: Gross revenue, net revenue, returns value, total customers, average order value, and total orders.
- `gold_monthly_sales_v2`: Monthly net revenue.
- `gold_sales_by_country_v2`: Net revenue by country.
- `gold_top_products_v2`: Top 10 products based on positive revenue.

Returns are preserved as valid business events in Silver. Their effect on revenue is handled in the Gold business logic.

## Testing

The pipeline was tested end-to-end using a new CSV batch containing updated and new records.

The test confirmed that:

- The file-arrival trigger started the Job automatically.
- Bronze ingested the new file.
- Silver updated existing records and inserted new records.
- Gold tables were refreshed.
- The watermark advanced after the pipeline succeeded.
- Reprocessing the same files did not create duplicate Bronze records.

Failure handling was also tested by intentionally failing the Gold task. The downstream watermark task did not run, and the watermark did not advance. After fixing the error, a Repair Run reran only the failed Gold task and the dependent watermark task.

## Project Structure

```text
retail-data-engineering-pipeline/
├── data/
│   └── batch_006.csv
├── docs/
│   └── architecture.md
├── sql/
│   ├── 01_bronze_ingestion.sql
│   ├── 02_silver_processing.sql
│   ├── 03_gold_tables.sql
│   └── 04_watermark_update.sql
└── README.md