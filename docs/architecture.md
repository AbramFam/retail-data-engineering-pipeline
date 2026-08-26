# Pipeline Architecture

```mermaid
flowchart TD
    A["CSV files in Databricks Volume"]
    B["Bronze: Raw ingestion and metadata"]
    C["Silver: Cleaning, latest record, and MERGE"]
    D["Gold: Business tables and KPIs"]
    E["Watermark: Last successful timestamp"]

    A -->|"File-arrival trigger"| B
    B --> C
    C --> D
    D -->|"Only after success"| E
```

## Workflow Dependencies

The Databricks Job runs the tasks in this order:

`bronze_ingestion → silver_processing → gold_tables → watermark_update`

If a task fails, its dependent tasks do not run. This prevents the watermark from advancing before the data has been processed successfully.