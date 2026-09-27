"""Airflow adapter. Install Airflow only in the orchestration environment."""
from datetime import timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

PROJECT = Path(__file__).resolve().parents[1]
PYTHON = "python"
DEFAULT_ARGS = {"owner": "cineinsight-data", "depends_on_past": False, "retries": 3, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="cineinsight_movielens_incremental",
    description="Incremental MovieLens medallion pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=None,
    tags=["medallion", "movielens", "incremental"],
) as dag:
    landing = BashOperator(task_id="landing_verify", bash_command=f'cd "{PROJECT}" && {PYTHON} src/pipeline.py init', execution_timeout=timedelta(hours=1), sla=timedelta(hours=1))
    bronze_silver = BashOperator(task_id="bronze_silver_incremental", bash_command=f'cd "{PROJECT}" && {PYTHON} src/pipeline.py run --as-of {{{{ ds }}}}', execution_timeout=timedelta(hours=6), sla=timedelta(hours=6))
    analytics = BashOperator(task_id="gold_marts_and_dq_report", bash_command=f'cd "{PROJECT}" && {PYTHON} src/pipeline.py report', execution_timeout=timedelta(hours=2), sla=timedelta(hours=2))
    landing >> bronze_silver >> analytics
