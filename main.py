"""
E-Commerce Data Engineering Pipeline — Main Entry Point
Swayam Siddhi Das | 23051634 | B.Tech CSE | SAP ABAP Data Engineer

Run this file to execute the full end-to-end pipeline:
  Ingestion → Processing → Data Quality → Dashboard Export
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from orchestration.dag_runner  import build_dag
from dashboard.export_data     import export_dashboard_data

def main():
    print("\n" + "=" * 60)
    print("  E-COMMERCE DATA ENGINEERING PIPELINE")
    print("  Swayam Siddhi Das | 23051634 | B.Tech CSE")
    print("=" * 60 + "\n")

    # Step 1-3: Run DAG (Ingest → Transform → Quality)
    dag     = build_dag()
    success = dag.run()

    if not success:
        print("\n[ERROR] Pipeline failed. Check logs/ directory.")
        sys.exit(1)

    # Step 4: Export dashboard data
    print("\n[5/5] Exporting dashboard data...")
    export_dashboard_data()

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE ✅")
    print("  Open dashboard/dashboard.html in your browser")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
