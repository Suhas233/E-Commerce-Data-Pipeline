# E-Commerce Data Pipeline

## Overview

This project builds an end-to-end data pipeline for processing e-commerce data using PySpark and Databricks.

The pipeline takes customer, product, order, and transaction data through a Medallion Architecture and produces analytics-ready datasets for business analysis.

## Architecture

```text
Source Data
    |
    v
Bronze
    |
    v
Silver
    |
    v
Gold
```

### Bronze Layer

Raw source files are ingested and stored in Delta format with minimal transformation.

### Silver Layer

Data is cleaned and validated by removing duplicates, handling null values, standardizing data types, and applying basic quality checks.

### Gold Layer

Cleaned data is combined to create datasets for:

* Revenue and sales analysis
* Average Order Value
* Customer Lifetime Value
* Repeat purchases
* Product performance
* Customer analysis

## Technologies

* Python
* PySpark
* Databricks
* Delta Lake
* Apache Airflow
* Docker

## Orchestration

Apache Airflow is used to automate the pipeline. The TaskFlow API manages the dependency between the Bronze, Silver, and Gold processing steps.

```text
Bronze Ingestion
       |
       v
Silver Transformation
       |
       v
Gold Transformation
```

## Project Structure

```text
ecommerce-data-pipeline/
│
├── notebooks/
│   ├── 01_bronze_ingestion
│   ├── 02_silver_transformation
│   └── 03_gold_transformation
│
├── source/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── transactions.csv
│
└── airflow/
    └── ecommerce_dag.py
```

## Outcome

The final Gold datasets provide structured, analytics-ready data that can be used to understand sales, customers, and product performance.
