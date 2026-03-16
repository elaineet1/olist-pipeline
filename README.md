# Olist E-Commerce Data Pipeline

An end-to-end data engineering pipeline built on the [Brazilian E-Commerce Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## Architecture

![Pipeline Architecture](docs/architecture_diagram.png)

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Meltano |
| Data Warehouse | BigQuery |
| Transformation | dbt |
| Data Quality | Great Expectations + dbt tests |
| Orchestration | Dagster |
| Analysis | Python, Jupyter, pandas |

## Project Structure
```
olist-pipeline/
├── olist_dbt/          # dbt project (staging + star schema models)
├── dagster_pipeline/   # Dagster assets and schedules
├── docs/               # Architecture diagram, GE report, charts
├── great_expectations_checks.py
├── olist_analysis.ipynb
└── README.md
```

## Data Warehouse Datasets

| Dataset | Description |
|---|---|
| olist_raw | Raw ingested tables (9 CSV files) |
| olist_dbt_olist_staging | Cleaned staging models |
| olist_dbt_olist_dbt | Star schema (6 dims, 3 facts) |

## Data Quality

- **67/67** dbt generic and custom SQL tests passing
- **16/16** Great Expectations expectations passing
- Full GE report: `docs/ge_report.html`

## Key Findings

- Monthly sales peaked in **November 2017** (Black Friday effect)
- **Health & Beauty** is the top revenue-generating category
- **92%** of customers are one-time buyers
- Average delivery time: **12 days**; SP state fastest at ~8 days
- **Credit card** dominates at 74% of all payments
