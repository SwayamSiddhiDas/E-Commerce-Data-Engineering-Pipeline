"""
Step 1: Data Ingestion Module
Reads CSV files → validates → loads into SQLite (simulating a data warehouse)
Swayam Siddhi Das | 23051634 | B.Tech CSE | SAP ABAP Data Engineer
"""

import os
import csv
import sqlite3
import logging
from datetime import datetime

# ── Logging Setup ────────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "ingestion.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("ingestion")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DB_PATH  = os.path.join(os.path.dirname(__file__), "..", "storage", "warehouse.db")

# ── Schema Definitions ────────────────────────────────────────────────────────
SCHEMAS = {
    "raw_orders": """
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id       TEXT,
            order_date     TEXT,
            customer_id    TEXT,
            customer_name  TEXT,
            city           TEXT,
            state          TEXT,
            product_id     TEXT,
            product_name   TEXT,
            category       TEXT,
            unit_price     REAL,
            quantity       INTEGER,
            discount_pct   REAL,
            total_amount   REAL,
            status         TEXT,
            ingested_at    TEXT
        )
    """,
    "raw_products": """
        CREATE TABLE IF NOT EXISTS raw_products (
            product_id      TEXT,
            product_name    TEXT,
            category        TEXT,
            unit_price      REAL,
            stock_quantity  INTEGER,
            supplier        TEXT,
            ingested_at     TEXT
        )
    """,
    "raw_customers": """
        CREATE TABLE IF NOT EXISTS raw_customers (
            customer_id    TEXT,
            customer_name  TEXT,
            city           TEXT,
            state          TEXT,
            email          TEXT,
            signup_date    TEXT,
            ingested_at    TEXT
        )
    """,
}

FILE_TABLE_MAP = {
    "orders.csv":   "raw_orders",
    "products.csv": "raw_products",
    "customers.csv":"raw_customers",
}


def validate_row(row: dict, table: str) -> bool:
    """Basic validation — no nulls in key fields."""
    required = {
        "raw_orders":    ["order_id", "customer_id", "product_id", "total_amount"],
        "raw_products":  ["product_id", "product_name"],
        "raw_customers": ["customer_id", "customer_name"],
    }
    for field in required.get(table, []):
        if not row.get(field, "").strip():
            return False
    return True


def ingest_csv(conn: sqlite3.Connection, csv_file: str, table: str) -> dict:
    """Read a CSV file and bulk-insert rows into SQLite."""
    path = os.path.join(DATA_DIR, csv_file)
    if not os.path.exists(path):
        log.warning(f"File not found: {path}")
        return {"loaded": 0, "skipped": 0}

    loaded = skipped = 0
    ingested_at = datetime.utcnow().isoformat()

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            if not validate_row(row, table):
                skipped += 1
                continue
            row["ingested_at"] = ingested_at
            rows.append(row)

    if rows:
        cols = rows[0].keys()
        placeholders = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        conn.executemany(sql, [list(r.values()) for r in rows])
        conn.commit()
        loaded = len(rows)

    log.info(f"[{csv_file}] → [{table}]  loaded={loaded}  skipped={skipped}")
    return {"loaded": loaded, "skipped": skipped}


def run_ingestion() -> dict:
    """Main ingestion entry point."""
    log.info("=" * 55)
    log.info("DATA INGESTION STARTED")
    log.info("=" * 55)

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Create tables
    for ddl in SCHEMAS.values():
        conn.execute(ddl)
    conn.commit()

    summary = {}
    for csv_file, table in FILE_TABLE_MAP.items():
        summary[table] = ingest_csv(conn, csv_file, table)

    conn.close()
    log.info("DATA INGESTION COMPLETE")
    log.info("=" * 55)
    return summary


if __name__ == "__main__":
    result = run_ingestion()
    print("\nIngestion Summary:")
    for table, stats in result.items():
        print(f"  {table}: {stats}")
