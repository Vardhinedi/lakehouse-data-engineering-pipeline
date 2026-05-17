````md
# Mini Lakehouse Data Pipeline with PySpark

## Overview

This project demonstrates a mini end-to-end data engineering pipeline using PySpark following the Medallion (Bronze, Silver, Gold) architecture.

The pipeline ingests raw JSON order data, performs cleaning and validation, and generates analytics-ready business datasets.

---

# Architecture

```text
Raw JSON Data
      ↓
Bronze Layer (Raw Ingestion)
      ↓
Silver Layer (Cleaned & Validated Data)
      ↓
Gold Layer (Business Analytics)
````

---

# Tech Stack

* Python
* PySpark
* Spark SQL
* Pandas
* CSV Storage
* VS Code

---

# Project Structure

```text
lakehouse-pipeline/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── utils/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

# Pipeline Layers

## Bronze Layer

### Purpose

Ingest raw JSON data and create structured raw datasets.

### Input

* orders.json
* customers.json

### Output

* bronze/orders.csv
* bronze/customers.csv

### Responsibilities

* Read raw JSON files
* Create Spark DataFrames
* Preserve raw data structure
* Store structured raw layer

---

## Silver Layer

### Purpose

Clean and validate datasets for downstream analytics.

### Transformations Performed

* Remove negative transaction amounts
* Remove duplicate order IDs
* Convert timestamps into proper date format
* Run reusable data quality checks

### Output

* silver/orders_clean.csv
* silver/customers_clean.csv

### Data Quality Checks

* Negative amount validation
* Null order ID validation

---

## Gold Layer

### Purpose

Generate analytics-ready business datasets.

### Analytics Generated

* Revenue by country
* Top customers by spending
* Daily revenue trends

### Output

* revenue_by_country.csv
* top_customers.csv
* daily_revenue.csv

---

# Sample Analytics Output

## Revenue By Country

| Country | Revenue |
| ------- | ------- |
| India   | 1400    |
| Canada  | 300     |

---

## Top Customers

| Customer | Total Spent |
| -------- | ----------- |
| Ananya   | 700         |
| Indra    | 700         |
| John     | 300         |

---

## Daily Revenue

| Date       | Revenue |
| ---------- | ------- |
| 2026-05-17 | 500     |
| 2026-05-18 | 900     |
| 2026-05-19 | 300     |

---

# Data Quality Framework

Reusable validation module implemented in:

```text
src/utils/quality_checks.py
```

Checks include:

* negative transaction detection
* null order ID validation

---

# How to Run

## 1. Clone Repository

```bash
git clone <your-github-repo-url>
cd lakehouse-pipeline
```

---

## 2. Create Virtual Environment

```bash
python -m venv venv
```

---

## 3. Activate Virtual Environment

### Windows

```bash
venv\Scripts\activate
```

### Mac/Linux

```bash
source venv/bin/activate
```

---

## 4. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 5. Run Bronze Pipeline

```bash
python src/bronze/bronze_pipeline.py
```

---

## 6. Run Silver Pipeline

```bash
python src/silver/silver_pipeline.py
```

---

## 7. Run Gold Pipeline

```bash
python src/gold/gold_pipeline.py
```

---

# Key Concepts Demonstrated

* ETL Pipelines
* Medallion Architecture
* PySpark DataFrames
* Spark Transformations
* Data Cleaning
* Data Validation
* Aggregations
* Joins
* Business Analytics
* Modular Project Structure

---

# Future Improvements

* Airflow orchestration
* Parquet storage
* Docker support
* Spark Structured Streaming
* Cloud deployment
* Automated testing

