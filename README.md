Airflow Data Engineering Pipelines (Dockerized)

This project demonstrates an end-to-end data engineering workflow using Apache Airflow, fully Dockerized, covering ingestion, transformation, export, conditional branching, notifications, and unit testing.

It is designed to showcase real-world Airflow concepts used in production data pipelines.

Tech Stack :
 -> Apache Airflow 2.8.1
 -> Docker & Docker Compose
 -> PostgreSQL
 -> Python
 -> Pandas
 -> Pytest (Unit Testing)

Project Structure :-
airflow-data-engineering-pipelines/
│
├── dags/
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
│
├── tests/
│   ├── test_dag1.py
│   └── test_utils.py
│
├── data/
│   └── input.csv
│
├── output/
│   └── (generated parquet files)
│
├── docker-compose.yml
├── README.md
└── logs/

DAG Overview
 DAG 1: CSV → PostgreSQL Ingestion
  Purpose:
    Ingests CSV data into PostgreSQL.
    Tasks:
     -> Create table if not exists
     -> Truncate table
     -> Load CSV into PostgreSQL
    Concepts Used:
     -> PythonOperator
     -> PostgresHook
     -> Idempotent ingestion
 
 DAG 2: Data Transformation Pipeline
  Purpose:
    Transforms raw employee data into enriched data.
    Transformations:
     -> full_info = name + city
     -> age_group classification
     -> salary_category classification
     -> year_joined extracted from join date
    Concepts Used:
     -> Pandas transformations
     -> Read from PostgreSQL
     -> Write transformed table

 DAG 3: PostgreSQL → Parquet Export
  Purpose:
    Exports transformed data to Parquet format.
    Steps:
     -> Validate source table exists
     -> Export table to Parquet (Snappy compression)
     -> Validate Parquet file
    Concepts Used:
     -> Data lake export pattern
     -> Parquet + Pandas
     -> File validation

 DAG 4: Conditional Workflow (Branching)
  Purpose:
    Executes different logic based on day of week.
    Branches:
     -> Weekdays: weekday processing
     -> End of week: reporting
     -> Weekend: cleanup
    Concepts Used:
     -> BranchPythonOperator
    -> Trigger rules
    -> Conditional workflows

 DAG 5: Notification & Callbacks Workflow
  Purpose:
    Demonstrates success/failure notifications and cleanup logic.
    Behavior:
     -> Simulated risky task (fails on specific dates)
     -> Success & failure callbacks
     -> Cleanup task always runs
    Concepts Used:
     -> on_success_callback
     -> on_failure_callback
     -> trigger_rule = all_done

 Unit Testing :-
  Unit tests validate:
   -> DAGs load without errors
   -> Correct number of DAGs
   -> Task structure and dependencies
   -> No import errors
   -> Unique DAG IDs

 Run Tests :-
   -> docker compose exec airflow-webserver pytest /opt/airflow/tests

How to Run the Project :-
 1. Start Airflow :
    -> docker compose up -d
 2. Open Airflow UI :
    -> http://localhost:8080
  [ Login :
    -- Username: admin
    -- Password: admin ]
 3. Trigger DAGs :
    -> Enable DAGs and trigger them manually or wait for schedules.

 Key Airflow Concepts Demonstrated :-
  -> DAG orchestration
  -> Task dependencies
  -> PostgreSQL integration
  -> Pandas transformations
  -> Branching workflows
  -> Notifications & callbacks
  -> Dockerized Airflow
  -> Unit testing with Pytest

  