## Airflow Data Engineering Pipelines

### DAG 1: CSV to PostgreSQL Ingestion
- Reads employee CSV
- Creates raw_employee_data table
- Loads data idempotently

### DAG 2: Data Transformation Pipeline
- Reads raw_employee_data
- Creates transformed_employee_data
- Adds derived business columns
