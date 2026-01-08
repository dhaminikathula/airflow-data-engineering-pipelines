# dags/dag2_data_transformation.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

dag = DAG(
    dag_id="data_transformation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

def create_transformed_table():
    """
    Creates transformed_employee_data table with additional columns.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS transformed_employee_data (
        id INTEGER,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary FLOAT,
        join_date DATE,
        full_info VARCHAR(500),
        age_group VARCHAR(20),
        salary_category VARCHAR(20),
        year_joined INTEGER
    );
    """

    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run(sql)

def transform_data():
    """
    Reads raw_employee_data, applies transformations, inserts into transformed_employee_data.
    """

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # Read raw data
    sql = "SELECT * FROM raw_employee_data;"
    df = hook.get_pandas_df(sql)

    # Transformations
    df["full_info"] = df["name"] + " - " + df["city"]

    df["age_group"] = df["age"].apply(
        lambda x: "Young" if x < 30 else "Mid" if x < 50 else "Senior"
    )

    df["salary_category"] = df["salary"].apply(
        lambda x: "Low" if x < 50000 else "Medium" if x < 80000 else "High"
    )

    df["year_joined"] = pd.to_datetime(df["join_date"]).dt.year

    # Truncate before insert (idempotent)
    hook.run("TRUNCATE TABLE transformed_employee_data;")

    # Insert transformed data
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        "transformed_employee_data",
        engine,
        if_exists="append",
        index=False,
    )

    return {
        "rows_processed": len(df),
        "rows_inserted": len(df),
    }

create_transformed_table_task = PythonOperator(
    task_id="create_transformed_table",
    python_callable=create_transformed_table,
    dag=dag,
)

transform_and_load_task = PythonOperator(
    task_id="transform_and_load",
    python_callable=transform_data,
    dag=dag,
)

create_transformed_table_task >> transform_and_load_task
