"""
Step 3: Data Quality & Validation Module
Runs checks on raw and transformed tables, generates quality report
Swayam Siddhi Das | 23051634 | B.Tech CSE | SAP ABAP Data Engineer
"""

import sqlite3
import logging
import os
import json
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "data_quality.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("data_quality")

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "storage", "warehouse.db")


CHECKS = [
    {
        "name": "No NULL order_id in raw_orders",
        "sql": "SELECT COUNT(*) FROM raw_orders WHERE order_id IS NULL OR order_id = ''",
        "expect": 0,
        "severity": "CRITICAL",
    },
    {
        "name": "No negative total_amount",
        "sql": "SELECT COUNT(*) FROM raw_orders WHERE CAST(total_amount AS REAL) < 0",
        "expect": 0,
        "severity": "HIGH",
    },
    {
        "name": "All orders linked to a customer",
        "sql": """SELECT COUNT(*) FROM fact_orders f
                  LEFT JOIN dim_customer c ON c.customer_sk = f.customer_sk
                  WHERE c.customer_sk IS NULL""",
        "expect": 0,
        "severity": "HIGH",
    },
    {
        "name": "All orders linked to a product",
        "sql": """SELECT COUNT(*) FROM fact_orders f
                  LEFT JOIN dim_product p ON p.product_sk = f.product_sk
                  WHERE p.product_sk IS NULL""",
        "expect": 0,
        "severity": "HIGH",
    },
    {
        "name": "No duplicate order_id in fact_orders",
        "sql": """SELECT COUNT(*) FROM (
                      SELECT order_id, COUNT(*) AS cnt FROM fact_orders
                      GROUP BY order_id HAVING cnt > 1
                  )""",
        "expect": 0,
        "severity": "CRITICAL",
    },
    {
        "name": "fact_orders row count > 0",
        "sql": "SELECT COUNT(*) FROM fact_orders",
        "expect_min": 1,
        "severity": "CRITICAL",
    },
    {
        "name": "dim_date fully populated",
        "sql": "SELECT COUNT(*) FROM dim_date WHERE year IS NULL",
        "expect": 0,
        "severity": "MEDIUM",
    },
    {
        "name": "unit_price > 0 in dim_product",
        "sql": "SELECT COUNT(*) FROM dim_product WHERE unit_price <= 0",
        "expect": 0,
        "severity": "HIGH",
    },
    {
        "name": "Aggregation: agg_sales_by_category not empty",
        "sql": "SELECT COUNT(*) FROM agg_sales_by_category",
        "expect_min": 1,
        "severity": "MEDIUM",
    },
    {
        "name": "Anomaly: orders with unusually high total (>200000)",
        "sql": "SELECT COUNT(*) FROM fact_orders WHERE total_amount > 200000",
        "expect": 0,
        "severity": "LOW",
    },
]


def run_quality_checks() -> list:
    log.info("=" * 55)
    log.info("DATA QUALITY CHECKS STARTED")
    log.info("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    results = []

    for check in CHECKS:
        try:
            val = conn.execute(check["sql"]).fetchone()[0]
            if "expect_min" in check:
                passed = val >= check["expect_min"]
            else:
                passed = val == check["expect"]

            status = "PASS" if passed else "FAIL"
            results.append({
                "check": check["name"],
                "severity": check["severity"],
                "result": val,
                "status": status,
            })
            icon = "✅" if passed else "❌"
            log.info(f"{icon} [{check['severity']}] {check['name']} → {status} (value={val})")
        except Exception as e:
            results.append({
                "check": check["name"],
                "severity": check["severity"],
                "result": None,
                "status": "ERROR",
                "error": str(e),
            })
            log.error(f"ERROR in check '{check['name']}': {e}")

    conn.close()

    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    log.info(f"\nSummary: {passed}/{len(results)} checks passed, {failed} failed")
    log.info("=" * 55)

    # Save JSON report
    report = {
        "run_at": datetime.utcnow().isoformat(),
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "checks": results,
    }
    report_path = os.path.join(os.path.dirname(__file__), "..", "logs", "quality_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    log.info(f"Quality report saved to: {report_path}")

    return results


if __name__ == "__main__":
    run_quality_checks()
