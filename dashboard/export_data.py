"""
Step 5: Dashboard Data Exporter
Exports aggregated tables to JSON for the HTML dashboard
Swayam Siddhi Das | 23051634 | B.Tech CSE | SAP ABAP Data Engineer
"""

import sqlite3
import json
import os

DB_PATH   = os.path.join(os.path.dirname(__file__), "..", "storage", "warehouse.db")
DASH_DIR  = os.path.join(os.path.dirname(__file__))

def export_dashboard_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    def q(sql):
        return [dict(r) for r in conn.execute(sql).fetchall()]

    data = {
        "sales_by_category": q(
            "SELECT category, total_revenue, total_orders, avg_order_value FROM agg_sales_by_category ORDER BY total_revenue DESC"
        ),
        "sales_by_month": q(
            "SELECT year_month, total_revenue, total_orders FROM agg_sales_by_month ORDER BY year_month"
        ),
        "top_customers": q(
            "SELECT customer_name, total_spent, order_count FROM agg_top_customers ORDER BY total_spent DESC LIMIT 10"
        ),
        "status_distribution": q(
            "SELECT status, COUNT(*) as count FROM fact_orders GROUP BY status ORDER BY count DESC"
        ),
        "kpis": q("""
            SELECT
                COUNT(*)                        AS total_orders,
                ROUND(SUM(total_amount),2)       AS total_revenue,
                ROUND(AVG(total_amount),2)       AS avg_order_value,
                COUNT(DISTINCT customer_sk)      AS unique_customers
            FROM fact_orders
        """)[0],
    }
    conn.close()

    out = os.path.join(DASH_DIR, "dashboard_data.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Dashboard data exported → {out}")
    return data

if __name__ == "__main__":
    export_dashboard_data()
