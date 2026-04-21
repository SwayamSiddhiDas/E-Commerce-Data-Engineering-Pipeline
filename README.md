# 🏗️ E-Commerce Data Engineering Pipeline
### Capstone Project — SAP ABAP Data Engineer Track

| Field | Details |
|-------|---------|
| **Name** | Swayam Siddhi Das |
| **Roll Number** | 23051634 |
| **Program** | B.Tech Computer Science and Engineering |
| **Track** | SAP ABAP Data Engineer |
| **Submission** | April 21, 2026 |

---

## 📌 Overview

A complete, end-to-end Data Engineering pipeline built for an e-commerce domain, covering **Task 30** (Final Capstone) which integrates all 30 Data Engineering concepts:

```
CSV Data → Ingestion → Star Schema → Data Quality → Orchestration → Dashboard
```

## 🏛️ Architecture

```
data/               ← Synthetic dataset (500 orders, 10 products, 10 customers)
ingestion/          ← CSV → SQLite raw tables with validation
processing/         ← Star Schema transformation + 10 quality checks
orchestration/      ← Airflow-style DAG (dependencies, retries, scheduling)
storage/            ← SQLite warehouse (fact + dim + agg tables)
dashboard/          ← HTML dashboard with Chart.js (6 charts)
logs/               ← Per-stage log files + JSON quality report
docs/               ← Project documentation PDF
```

## 🚀 Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/capstone-data-engineering.git
cd capstone-data-engineering

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate sample data
python data/generate_data.py

# 4. Run the full pipeline
python main.py

# 5. Open the dashboard
open dashboard/dashboard.html   # macOS
# or just double-click dashboard/dashboard.html
```

## 📊 Pipeline Stages

### Stage 1 — Data Ingestion (`ingestion/ingest.py`)
- Reads `orders.csv`, `products.csv`, `customers.csv`
- Validates required fields, logs skipped rows
- Bulk-inserts into SQLite raw tables

### Stage 2 — Data Processing (`processing/transform.py`)
- Builds **Star Schema**: `fact_orders` + `dim_customer` + `dim_product` + `dim_date`
- Computes 3 aggregation tables: by category, by month, by customer
- Uses INSERT OR IGNORE for idempotency

### Stage 3 — Data Quality (`processing/data_quality.py`)
- 10 automated checks: nulls, duplicates, orphan keys, anomalies
- Severity tiers: CRITICAL / HIGH / MEDIUM / LOW
- CRITICAL failures block pipeline; others are logged
- Exports `logs/quality_report.json`

### Stage 4 — Orchestration (`orchestration/dag_runner.py`)
- Airflow-style DAG with topological sort
- Configurable retries per task
- Skip-on-failure propagation
- Airflow-compatible operator code included as comments

### Stage 5 — Dashboard (`dashboard/dashboard.html`)
- Standalone HTML — no server required
- KPI cards: Total Orders, Revenue, Avg Order Value, Customers
- Monthly revenue line chart
- Order status donut chart
- Revenue & orders by category bar charts
- Top-10 customers table

## 📈 Results

| Metric | Value |
|--------|-------|
| Raw Orders Ingested | 500 |
| Fact Orders (excl. cancelled) | 455 |
| Total Revenue | ₹1,34,45,290 |
| Avg Order Value | ₹29,550 |
| Top Category | Electronics (₹1.17 Cr) |
| Quality Checks | 9/10 PASS |

## 🛠️ Tech Stack

- **Python 3.8+** — pipeline logic
- **SQLite** — lightweight data warehouse
- **HTML5 + Chart.js** — dashboard
- **ReportLab** — PDF documentation

## 📁 File Structure

```
capstone_project/
├── main.py
├── data/
│   ├── generate_data.py
│   ├── orders.csv
│   ├── products.csv
│   └── customers.csv
├── ingestion/
│   └── ingest.py
├── processing/
│   ├── transform.py
│   └── data_quality.py
├── orchestration/
│   └── dag_runner.py
├── dashboard/
│   ├── export_data.py
│   ├── dashboard_data.json
│   └── dashboard.html
├── storage/
│   └── warehouse.db
├── logs/
├── docs/
│   └── Project_Documentation.pdf
├── requirements.txt
└── README.md
```

## 🔮 Future Improvements

- Migrate storage to PostgreSQL / Google BigQuery
- Replace custom DAG with Apache Airflow
- Add Kafka for real-time streaming ingestion
- Use Apache Spark for distributed processing
- Deploy dashboard to AWS S3 / GitHub Pages
- Add SAP ABAP OData API connector

---

> **Academic Integrity**: This is an individual project. All code is original and independently developed.
