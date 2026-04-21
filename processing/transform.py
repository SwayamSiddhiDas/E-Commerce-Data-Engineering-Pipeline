"""
Step 2: Data Processing & Transformation Module
Raw tables → cleaned → star schema (fact + dimension tables)
Swayam Siddhi Das | 23051634 | B.Tech CSE | SAP ABAP Data Engineer
"""

import sqlite3
import logging
import os
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "processing.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("processing")

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "storage", "warehouse.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── DDL for Star Schema ───────────────────────────────────────────────────────
STAR_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk    INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id    TEXT UNIQUE,
    customer_name  TEXT,
    city           TEXT,
    state          TEXT,
    email          TEXT,
    signup_date    TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk    INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id    TEXT UNIQUE,
    product_name  TEXT,
    category      TEXT,
    unit_price    REAL,
    supplier      TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk   INTEGER PRIMARY KEY,
    full_date TEXT UNIQUE,
    year      INTEGER,
    quarter   INTEGER,
    month     INTEGER,
    month_name TEXT,
    day       INTEGER,
    weekday   TEXT
);

CREATE TABLE IF NOT EXISTS fact_orders (
    fact_sk        INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id       TEXT UNIQUE,
    date_sk        INTEGER,
    customer_sk    INTEGER,
    product_sk     INTEGER,
    quantity       INTEGER,
    unit_price     REAL,
    discount_pct   REAL,
    total_amount   REAL,
    status         TEXT,
    FOREIGN KEY (date_sk)     REFERENCES dim_date(date_sk),
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (product_sk)  REFERENCES dim_product(product_sk)
);

CREATE TABLE IF NOT EXISTS agg_sales_by_category (
    category         TEXT PRIMARY KEY,
    total_revenue    REAL,
    total_orders     INTEGER,
    avg_order_value  REAL
);

CREATE TABLE IF NOT EXISTS agg_sales_by_month (
    year_month       TEXT PRIMARY KEY,
    total_revenue    REAL,
    total_orders     INTEGER
);

CREATE TABLE IF NOT EXISTS agg_top_customers (
    customer_id    TEXT PRIMARY KEY,
    customer_name  TEXT,
    total_spent    REAL,
    order_count    INTEGER
);
"""

MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]
WEEKDAYS    = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]


def build_dim_date(conn: sqlite3.Connection):
    """Populate dim_date from all distinct order dates."""
    log.info("Building dim_date...")
    rows = conn.execute("SELECT DISTINCT order_date FROM raw_orders WHERE order_date IS NOT NULL").fetchall()
    inserted = 0
    for row in rows:
        date_str = row[0]
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue
        date_sk = int(dt.strftime("%Y%m%d"))
        quarter = (dt.month - 1) // 3 + 1
        conn.execute(
            """INSERT OR IGNORE INTO dim_date
               (date_sk, full_date, year, quarter, month, month_name, day, weekday)
               VALUES (?,?,?,?,?,?,?,?)""",
            (date_sk, date_str, dt.year, quarter, dt.month,
             MONTH_NAMES[dt.month - 1], dt.day, WEEKDAYS[dt.weekday()])
        )
        inserted += 1
    conn.commit()
    log.info(f"dim_date: {inserted} dates loaded")


def build_dim_customer(conn: sqlite3.Connection):
    log.info("Building dim_customer...")
    conn.execute("""
        INSERT OR IGNORE INTO dim_customer
            (customer_id, customer_name, city, state, email, signup_date)
        SELECT DISTINCT
            rc.customer_id,
            rc.customer_name,
            rc.city,
            rc.state,
            rc.email,
            rc.signup_date
        FROM raw_customers rc
    """)
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    log.info(f"dim_customer: {n} rows")


def build_dim_product(conn: sqlite3.Connection):
    log.info("Building dim_product...")
    conn.execute("""
        INSERT OR IGNORE INTO dim_product
            (product_id, product_name, category, unit_price, supplier)
        SELECT DISTINCT
            product_id, product_name, category, unit_price, supplier
        FROM raw_products
    """)
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]
    log.info(f"dim_product: {n} rows")


def build_fact_orders(conn: sqlite3.Connection):
    log.info("Building fact_orders...")
    conn.execute("""
        INSERT OR IGNORE INTO fact_orders
            (order_id, date_sk, customer_sk, product_sk,
             quantity, unit_price, discount_pct, total_amount, status)
        SELECT
            o.order_id,
            CAST(REPLACE(o.order_date, '-', '') AS INTEGER) AS date_sk,
            c.customer_sk,
            p.product_sk,
            CAST(o.quantity AS INTEGER),
            CAST(o.unit_price AS REAL),
            CAST(o.discount_pct AS REAL),
            CAST(o.total_amount AS REAL),
            o.status
        FROM raw_orders o
        LEFT JOIN dim_customer c ON c.customer_id = o.customer_id
        LEFT JOIN dim_product  p ON p.product_id  = o.product_id
        WHERE o.status != 'Cancelled'
    """)
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    log.info(f"fact_orders: {n} rows")


def build_aggregations(conn: sqlite3.Connection):
    log.info("Building aggregation tables...")

    # Sales by category
    conn.execute("DELETE FROM agg_sales_by_category")
    conn.execute("""
        INSERT INTO agg_sales_by_category
        SELECT
            p.category,
            ROUND(SUM(f.total_amount), 2),
            COUNT(*),
            ROUND(AVG(f.total_amount), 2)
        FROM fact_orders f
        JOIN dim_product p ON p.product_sk = f.product_sk
        GROUP BY p.category
    """)

    # Sales by month
    conn.execute("DELETE FROM agg_sales_by_month")
    conn.execute("""
        INSERT INTO agg_sales_by_month
        SELECT
            d.year || '-' || PRINTF('%02d', d.month),
            ROUND(SUM(f.total_amount), 2),
            COUNT(*)
        FROM fact_orders f
        JOIN dim_date d ON d.date_sk = f.date_sk
        GROUP BY d.year, d.month
    """)

    # Top customers
    conn.execute("DELETE FROM agg_top_customers")
    conn.execute("""
        INSERT INTO agg_top_customers
        SELECT
            c.customer_id,
            c.customer_name,
            ROUND(SUM(f.total_amount), 2),
            COUNT(*)
        FROM fact_orders f
        JOIN dim_customer c ON c.customer_sk = f.customer_sk
        GROUP BY c.customer_id
        ORDER BY SUM(f.total_amount) DESC
    """)

    conn.commit()
    log.info("Aggregations complete")


def run_processing():
    log.info("=" * 55)
    log.info("DATA PROCESSING STARTED")
    log.info("=" * 55)

    conn = get_conn()
    for ddl in STAR_SCHEMA_DDL.strip().split(";"):
        ddl = ddl.strip()
        if ddl:
            conn.execute(ddl)
    conn.commit()

    build_dim_date(conn)
    build_dim_customer(conn)
    build_dim_product(conn)
    build_fact_orders(conn)
    build_aggregations(conn)

    conn.close()
    log.info("DATA PROCESSING COMPLETE")
    log.info("=" * 55)


if __name__ == "__main__":
    run_processing()
