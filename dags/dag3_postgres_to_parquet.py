# dags/dag3_postgres_to_parquet.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

# ---------------- DAG DEFINITION ----------------

dag = DAG(
    dag_id='postgres_to_parquet_export',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
)

# ---------------- FUNCTIONS ----------------

def check_table_exists(table_name: str):
    """
    Checks if the PostgreSQL table exists and has data.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")

    # Check table exists
    table_check = hook.get_first(
        f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        """
    )

    if table_check[0] == 0:
        raise Exception(f"Table {table_name} does not exist")

    # Check table has data
    row_count = hook.get_first(f"SELECT COUNT(*) FROM {table_name}")[0]

    if row_count == 0:
        raise Exception(f"Table {table_name} exists but has no data")

    return True


def export_table_to_parquet(table_name: str, output_path: str):
    """
    Exports PostgreSQL table to Parquet format.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    file_size = os.path.getsize(output_path)

    return {
        "file_path": output_path,
        "row_count": len(df),
        "file_size_bytes": file_size
    }


def validate_parquet(file_path: str):
    """
    Validates that the Parquet file is readable and correct.
    """
    df = pd.read_parquet(file_path)

    expected_columns = [
        "id",
        "name",
        "age",
        "city",
        "salary",
        "join_date",
        "full_info",
        "age_group",
        "salary_category",
        "year_joined",
    ]

    for col in expected_columns:
        if col not in df.columns:
            raise Exception(f"Missing column: {col}")

    if len(df) == 0:
        raise Exception("Parquet file has no rows")

    return True

# ---------------- TASKS ----------------

check_table_task = PythonOperator(
    task_id='check_source_table_exists',
    python_callable=check_table_exists,
    op_kwargs={'table_name': 'transformed_employee_data'},
    dag=dag,
)

export_task = PythonOperator(
    task_id='export_to_parquet',
    python_callable=export_table_to_parquet,
    op_kwargs={
        'table_name': 'transformed_employee_data',
        'output_path': '/opt/airflow/output/employee_data_{{ ds }}.parquet'
    },
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_parquet_file',
    python_callable=validate_parquet,
    op_kwargs={
        'file_path': '/opt/airflow/output/employee_data_{{ ds }}.parquet'
    },
    dag=dag,
)

# ---------------- DEPENDENCIES ----------------

check_table_task >> export_task >> validate_task
