# Retail Data Engineering Pipeline

An incremental data pipeline built in Databricks using the Medallion Architecture. It ingests retail sales CSV files and processes them through Bronze, Silver, and Gold layers using incremental processing, watermarking, `MERGE` operations, and Databricks Workflows orchestration.

## Architecture

The pipeline follows the Medallion Architecture:

`CSV Files → Bronze → Silver → Gold → Business KPIs`

- **Bronze:** Ingests raw CSV files and adds metadata: source file, ingestion timestamp, batch ID, and source system.
- **Silver:** Cleans and standardizes the data, selects the latest record per business key, and uses `MERGE` to insert new records or update existing ones.
- **Gold:** Creates business-ready tables for KPIs, monthly sales, country sales, and top products.

Full diagram and per-task dependency breakdown: [docs/architecture.md](docs/architecture.md).

## Technologies Used

- Databricks
- SQL
- Delta Lake
- Databricks Workflows
- Databricks Volumes
- Git and GitHub

## Pipeline Automation and Incremental Processing

The pipeline runs as a Databricks Job with task order `bronze_ingestion → silver_processing → gold_tables → watermark_update`. A file-arrival trigger starts the Job automatically when a new CSV lands in the Databricks Volume. The watermark only advances after Bronze, Silver, and Gold all succeed, so a failed run doesn't cause data to be skipped.

- `COPY INTO` ingests only files not already processed.
- A watermark table stores the latest successfully processed ingestion timestamp.
- Silver processes only Bronze records newer than the current watermark.
- `ROW_NUMBER()` selects the latest version of each record using `InvoiceNo + StockCode` as the business key.
- `MERGE` updates matched records and inserts new ones into Silver.

## Gold Layer Outputs

- `gold_kpis_v2`: Gross revenue, net revenue, returns value, total customers, average order value, and total orders.
- `gold_monthly_sales_v2`: Monthly net revenue.
- `gold_sales_by_country_v2`: Net revenue by country.
- `gold_top_products_v2`: Top 10 products by positive revenue.

Returns are preserved as valid business events in Silver; their effect on revenue is handled explicitly in Gold (see KPI Definitions below).

## KPI Definitions

Business assumptions encoded in `sql/03_gold_tables.sql`, not universal accounting definitions — they could change depending on how a business wants revenue/returns treated.

| KPI | Definition |
|---|---|
| Gross Revenue | Sum of `Quantity × UnitPrice` for rows with positive `Quantity` only (excludes returns). |
| Net Revenue | Sum of `Quantity × UnitPrice` across all rows, including returns. |
| Returns Value | Absolute value of the summed `Quantity × UnitPrice` for rows with negative `Quantity`. |
| Total Customers | Distinct `CustomerID` values greater than 0. |
| Total Orders | Distinct `InvoiceNo` values. |
| AOV | Net Revenue ÷ distinct `InvoiceNo` count. |
| Monthly Sales | Net Revenue grouped by invoice month. |
| Country Sales | Net Revenue grouped by `Country`. |
| Top Products | Positive-quantity revenue only, top 10 by revenue. |

## RFM Customer Segmentation (Extension)

A PySpark notebook, `notebooks/05_rfm_customer_segmentation.ipynb`, extends the Gold layer with customer segmentation. It runs separately from the Databricks Job and is not one of its tasks.

- **Source:** `workspace.default.silver_sales_v2`.
- **Filters:** Positive `Quantity` and `UnitPrice` only; `CustomerID` `-1` (unknown customers) is excluded.
- **Analysis window:** Dynamic 12 months. The reference date is the latest `InvoiceDate` + 1 day, and only sales on or after `reference date - 12 months` are used.

| Metric | Calculation |
|---|---|
| Recency | Days between the reference date and the customer's last purchase date. |
| Frequency | Distinct `InvoiceNo` count in the window. |
| Monetary | Sum of `Quantity × UnitPrice` in the window. |

**Scores (1–5):**

- **R:** Fixed day thresholds: ≤7 days = 5, ≤14 = 4, ≤30 = 3, ≤60 = 2, otherwise 1.
- **F:** 4+ invoices = 5, 3 = 4, 2 = 3, otherwise 1.
- **M:** Quintiles of Monetary across customers in the window.

**Segments:** Champions, Loyal Customers, Potential Loyalists, At Risk, Hibernating, and Needs Attention (the fallback). Rules are applied in that order, and the first match wins.

**Gold outputs** (Delta tables, overwritten on each run):

- `workspace.default.gold_customer_rfm`: One row per customer with reference date, last purchase date, Recency, Frequency, Monetary, R/F/M scores, and Segment.
- `workspace.default.gold_rfm_segment_summary`: Customer count per segment.

## Key Engineering Decisions

- **Bronze preserves source metadata** (`source_file`, `ingestion_timestamp`, `batch_id`, `source_system`) so every row can be traced to its file/batch, and Silver has a reliable timestamp for incremental filtering.
- **Silver uses a business key (`InvoiceNo + StockCode`) and latest-record logic** because the same invoice/product pair can reappear across batches if a record is corrected or resent; only the newest version should apply.
- **`MERGE` replaces insert-only processing** so a corrected/resent record updates the existing Silver row instead of creating a duplicate.
- **Returns are preserved**, not filtered out, because they're real business events — handled explicitly in Gold's revenue logic instead of being dropped upstream.
- **The watermark moves only after Bronze, Silver, and Gold succeed**, so a mid-pipeline failure doesn't cause rows to be silently skipped on the next run.

## Testing

Tested end-to-end in Databricks — manually, via the workspace UI — using a 5-row incremental CSV batch with 2 designed updates and 3 designed inserts. Coverage included the file-arrival trigger, Bronze/Silver/Gold processing, watermark advancement, idempotency on reprocessing, an intentional Gold-task failure with the watermark correctly blocked, a Repair Run recovering from it, and validation of the setup script in a separate schema.

Full test list, expected vs. observed results, and evidence status (screenshots not yet added): [docs/testing.md](docs/testing.md).

## Sample Data

`data/batch_006.csv` is a small synthetic incremental test batch, not the project's full dataset. It contains 5 rows: 2 using `InvoiceNo + StockCode` values designed to match existing business keys already loaded into `silver_sales_v2` (exercising the `MERGE` update path), and 3 using new keys designed as inserts.

The original base retail dataset these updates were tested against isn't included in this repository. Reproducing the exact update behavior in [docs/testing.md](docs/testing.md) requires a base dataset already loaded into Silver with matching `InvoiceNo`/`StockCode` values. Without one, the pipeline still runs end-to-end — the 3 new-key rows insert normally, but nothing matches the 2 update-designed rows.

### Expected CSV Schema

```text
InvoiceNo
StockCode
Description
Quantity
InvoiceDate
UnitPrice
CustomerID
Country
```

## Prerequisites

- A Databricks workspace with SQL warehouse or cluster access.
- Permission to create catalogs/schemas, tables, Volumes, and Jobs.
- `data/batch_006.csv`, or a compatible CSV matching the [Expected CSV Schema](#expected-csv-schema).

## Configuration Note: Volume Path

`sql/01_bronze_ingestion.sql` reads from a hardcoded path:

```text
/Volumes/workspace/default/raw_data/daily_sales
```

This is the path tested in this project's own workspace (`workspace` catalog, `default` schema) and is environment-specific. If your catalog, schema, or Volume name differs, edit the `FROM '...'` path directly in `sql/01_bronze_ingestion.sql` before running it.

## How to Run

Steps are labeled by where they happen: **[SQL]** run from a file in this repo, **[UI]** configured in the Databricks workspace UI, **[Optional]** testing/validation only.

1. **[UI]** Create or select the Databricks catalog and schema (tested example: `workspace.default`).
2. **[UI]** Create a Volume and an input directory for incoming CSV files (tested example: `/Volumes/workspace/default/raw_data/daily_sales`).
3. **[SQL, manual edit]** If using a different catalog/schema/Volume, update the `FROM '...'` path in `sql/01_bronze_ingestion.sql` to match.
4. **[SQL]** Run `sql/00_setup_tables.sql` to create `bronze_sales_v2`, `silver_sales_v2`, and `pipeline_watermark`, and seed the initial watermark row.
5. **[UI]** Upload your base retail CSV data (matching the schema above) into the Volume directory from step 2.
6. **[SQL]** Run the files in order for the initial load: `sql/01_bronze_ingestion.sql` → `sql/02_silver_processing.sql` → `sql/03_gold_tables.sql` → `sql/04_watermark_update.sql`.
7. **[UI]** A Databricks Job cannot run a local repository file directly — for each of the four SQL files, create and save a Databricks SQL query containing that file's contents, then create a matching Job task (`bronze_ingestion`, `silver_processing`, `gold_tables`, `watermark_update`) configured to run that saved query.
8. **[UI]** Configure task dependencies so each task only runs after the previous one succeeds, in the order above.
9. **[UI]** Configure a file-arrival trigger on the Job, pointed at the Volume directory from step 2.
10. **[Optional]** Upload `data/batch_006.csv` (or another compatible incremental CSV) to trigger an incremental run.
11. **[Optional]** Validate results by querying `bronze_sales_v2`, `silver_sales_v2`, the four Gold tables, and `pipeline_watermark`.

This is not a one-click deployment — steps 1, 2, and 7–9 require manual setup in the Databricks UI.

## Project Structure

```text
retail-data-engineering-pipeline/
├── data/
│   └── batch_006.csv
├── docs/
│   ├── architecture.md
│   ├── testing.md
│   └── screenshots/
├── notebooks/
│   └── 05_rfm_customer_segmentation.ipynb
├── sql/
│   ├── 00_setup_tables.sql
│   ├── 01_bronze_ingestion.sql
│   ├── 02_silver_processing.sql
│   ├── 03_gold_tables.sql
│   └── 04_watermark_update.sql
├── .gitignore
└── README.md
```

## Limitations and Scope

- This is a portfolio/learning project demonstrating Medallion Architecture, incremental processing, and Databricks Workflows — not a production system.
- The Databricks Job, its tasks, dependencies, and file-arrival trigger are configured in the Databricks UI and aren't exported as files in this repository.
