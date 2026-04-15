from __future__ import annotations

from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/dbt_olist_snowflake"

default_args = {
    "owner": "analytics",
    "retries": 0,
}

@dag(
    dag_id="dbt_olist_product_performance_mart",
    description="Run dbt models for Olist dataset in Snowflake",
    schedule=None,  # Manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "snowflake", "olist"],
)
def dbt_olist_product_performance_mart():
    """
    Orchestrates dbt commands (deps -> clean -> compile -> debug -> run -> test) using BashOperator.
    Uses --profiles-dir explicitly and captures logs for easier debugging.
    """

    # Install dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        rm -rf dbt_packages && \
        dbt deps --profiles-dir {PROJECT_DIR} > dbt_deps.log 2>&1
        """,
        do_xcom_push=False,
    )

    # Clean dbt artifacts
    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        dbt clean --profiles-dir {PROJECT_DIR} > dbt_clean.log 2>&1
        """,
        do_xcom_push=False,
    )

    # Compile project
    dbt_compile = BashOperator(
        task_id="dbt_compile",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        dbt compile --profiles-dir {PROJECT_DIR} > dbt_compile.log 2>&1
        """,
        do_xcom_push=False,
    )

    # Debug dbt setup
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        dbt debug --profiles-dir {PROJECT_DIR} > dbt_debug.log 2>&1
        """,
        do_xcom_push=False,
    )

    # Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        dbt run --select +fact_product_performance --profiles-dir {PROJECT_DIR} > dbt_run.log 2>&1
        """,
        do_xcom_push=False,
    )

    # Test dbt models
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        dbt test --select +fact_product_performance --profiles-dir {PROJECT_DIR} > dbt_test.log 2>&1
        """,
        do_xcom_push=False,
    )

    # Task dependencies
    dbt_deps >> dbt_clean >> dbt_compile >> dbt_debug >> dbt_run >> dbt_test


# Instantiate the DAG
dag = dbt_olist_product_performance_mart()