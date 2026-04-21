"""
Step 4: Pipeline Orchestration (Airflow-Style DAG in Pure Python)
Defines tasks, dependencies, scheduling, retry logic, and logging
Swayam Siddhi Das | 23051634 | B.Tech CSE | SAP ABAP Data Engineer

Note: This is a self-contained DAG runner that mirrors Airflow concepts
(tasks, dependencies, retries, logging) without requiring an Airflow server.
To run on actual Airflow, uncomment the Airflow imports at the bottom.
"""

import time
import logging
import os
import sys
import traceback
from datetime import datetime
from typing import Callable

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "orchestration.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("orchestrator")

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ingestion.ingest       import run_ingestion
from processing.transform   import run_processing
from processing.data_quality import run_quality_checks


# ── Task Definition ────────────────────────────────────────────────────────────
class Task:
    def __init__(self, task_id: str, callable_fn: Callable,
                 retries: int = 2, retry_delay: int = 3):
        self.task_id     = task_id
        self.callable_fn = callable_fn
        self.retries     = retries
        self.retry_delay = retry_delay
        self.upstream    = []
        self.status      = "pending"   # pending | running | success | failed
        self.result      = None
        self.started_at  = None
        self.ended_at    = None

    def set_upstream(self, *tasks):
        self.upstream.extend(tasks)

    def run(self):
        attempt = 0
        while attempt <= self.retries:
            try:
                self.status     = "running"
                self.started_at = datetime.utcnow()
                log.info(f"▶  Task [{self.task_id}] started (attempt {attempt + 1})")
                self.result   = self.callable_fn()
                self.status   = "success"
                self.ended_at = datetime.utcnow()
                elapsed       = (self.ended_at - self.started_at).total_seconds()
                log.info(f"✅ Task [{self.task_id}] succeeded in {elapsed:.2f}s")
                return True
            except Exception as e:
                attempt += 1
                log.warning(f"⚠️  Task [{self.task_id}] failed (attempt {attempt}): {e}")
                if attempt <= self.retries:
                    log.info(f"   Retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    self.status   = "failed"
                    self.ended_at = datetime.utcnow()
                    log.error(f"❌ Task [{self.task_id}] permanently failed:\n{traceback.format_exc()}")
                    return False


# ── DAG Runner ────────────────────────────────────────────────────────────────
class DAG:
    def __init__(self, dag_id: str, schedule: str = "@daily"):
        self.dag_id   = dag_id
        self.schedule = schedule
        self.tasks    = {}

    def add_task(self, task: Task):
        self.tasks[task.task_id] = task

    def _topo_sort(self):
        """Topological sort to respect task dependencies."""
        visited = set()
        order   = []

        def dfs(tid):
            if tid in visited:
                return
            visited.add(tid)
            for up in self.tasks[tid].upstream:
                dfs(up.task_id)
            order.append(self.tasks[tid])

        for tid in self.tasks:
            dfs(tid)
        return order

    def run(self):
        log.info("=" * 60)
        log.info(f"DAG [{self.dag_id}] EXECUTION STARTED")
        log.info(f"Schedule: {self.schedule} | Run time: {datetime.utcnow().isoformat()}")
        log.info("=" * 60)

        task_order = self._topo_sort()
        dag_start  = datetime.utcnow()
        failed_tasks = []

        for task in task_order:
            # Check all upstreams succeeded
            for up in task.upstream:
                if up.status != "success":
                    task.status = "skipped"
                    log.warning(f"⏭  Task [{task.task_id}] SKIPPED (upstream [{up.task_id}] failed)")
                    failed_tasks.append(task.task_id)
                    break
            else:
                success = task.run()
                if not success:
                    failed_tasks.append(task.task_id)

        dag_end = datetime.utcnow()
        elapsed = (dag_end - dag_start).total_seconds()

        log.info("=" * 60)
        log.info(f"DAG [{self.dag_id}] FINISHED in {elapsed:.2f}s")
        for t in task_order:
            icon = {"success": "✅", "failed": "❌", "skipped": "⏭", "pending": "⏳"}.get(t.status, "?")
            log.info(f"  {icon} {t.task_id}: {t.status}")
        if failed_tasks:
            log.error(f"Failed tasks: {failed_tasks}")
        else:
            log.info("All tasks completed successfully 🎉")
        log.info("=" * 60)
        return len(failed_tasks) == 0


# ── Quality gate wrapper ───────────────────────────────────────────────────────
def quality_gate():
    results = run_quality_checks()
    critical_fails = [r for r in results if r["status"] == "FAIL" and r["severity"] == "CRITICAL"]
    if critical_fails:
        raise RuntimeError(f"CRITICAL quality checks failed: {[r['check'] for r in critical_fails]}")
    return results


# ── Build & Run DAG ───────────────────────────────────────────────────────────
def build_dag() -> DAG:
    dag = DAG(
        dag_id  = "ecommerce_data_pipeline",
        schedule= "@daily",
    )

    t1 = Task("ingest_raw_data",   run_ingestion,  retries=3, retry_delay=5)
    t2 = Task("transform_data",    run_processing, retries=2, retry_delay=3)
    t3 = Task("data_quality_gate", quality_gate,   retries=1, retry_delay=2)

    t2.set_upstream(t1)
    t3.set_upstream(t2)

    dag.add_task(t1)
    dag.add_task(t2)
    dag.add_task(t3)

    return dag


if __name__ == "__main__":
    dag = build_dag()
    success = dag.run()
    sys.exit(0 if success else 1)


# ── Airflow-Compatible DAG (uncomment if running on Airflow) ──────────────────
# from airflow import DAG as AirflowDAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
#
# with AirflowDAG("ecommerce_pipeline", start_date=datetime(2024,1,1), schedule_interval="@daily") as af_dag:
#     t1 = PythonOperator(task_id="ingest",   python_callable=run_ingestion)
#     t2 = PythonOperator(task_id="transform",python_callable=run_processing)
#     t3 = PythonOperator(task_id="quality",  python_callable=quality_gate)
#     t1 >> t2 >> t3
