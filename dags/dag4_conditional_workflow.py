# dags/dag4_conditional_workflow.py

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


# DAG definition
dag = DAG(
    dag_id='conditional_workflow_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
)


# Branching logic

def determine_branch(**context):
    """
    Decide which branch to follow based on day of week.
    Monday = 0, Sunday = 6
    """
    execution_date = context['execution_date']
    day_of_week = execution_date.weekday()

    if day_of_week <= 2:
        return 'weekday_processing'
    elif day_of_week <= 4:
        return 'end_of_week_processing'
    else:
        return 'weekend_processing'



# Processing functions

def weekday_process(**context):
    execution_date = context['execution_date']
    return {
        'day_name': execution_date.strftime('%A'),
        'task_type': 'weekday',
        'record_count': 100
    }


def end_of_week_process(**context):
    execution_date = context['execution_date']
    return {
        'day_name': execution_date.strftime('%A'),
        'task_type': 'end_of_week',
        'weekly_summary': 'Weekly summary generated'
    }


def weekend_process(**context):
    execution_date = context['execution_date']
    return {
        'day_name': execution_date.strftime('%A'),
        'task_type': 'weekend',
        'cleanup_status': 'Cleanup completed'
    }


# Tasks
start_task = EmptyOperator(
    task_id='start',
    dag=dag
)

branch_task = BranchPythonOperator(
    task_id='branch_by_day',
    python_callable=determine_branch,
    provide_context=True,
    dag=dag
)

weekday_task = PythonOperator(
    task_id='weekday_processing',
    python_callable=weekday_process,
    provide_context=True,
    dag=dag
)

weekday_summary_task = EmptyOperator(
    task_id='weekday_summary',
    dag=dag
)

end_of_week_task = PythonOperator(
    task_id='end_of_week_processing',
    python_callable=end_of_week_process,
    provide_context=True,
    dag=dag
)

end_of_week_report_task = EmptyOperator(
    task_id='end_of_week_report',
    dag=dag
)

weekend_task = PythonOperator(
    task_id='weekend_processing',
    python_callable=weekend_process,
    provide_context=True,
    dag=dag
)

weekend_cleanup_task = EmptyOperator(
    task_id='weekend_cleanup',
    dag=dag
)

end_task = EmptyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)


# Dependencies
start_task >> branch_task

branch_task >> weekday_task >> weekday_summary_task >> end_task
branch_task >> end_of_week_task >> end_of_week_report_task >> end_task
branch_task >> weekend_task >> weekend_cleanup_task >> end_task
