from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import random
from airflow.utils.state import State


def send_success_notification(context):
    ti = context["task_instance"]
    execution_date = context["execution_date"]

    message = f"Task {ti.task_id} succeeded on {execution_date}"

    result = {
        "notification_type": "success",
        "status": "sent",
        "message": message,
        "timestamp": datetime.utcnow().isoformat()
    }

    print(result)
    return result


def send_failure_notification(context):
    ti = context["task_instance"]
    execution_date = context["execution_date"]
    exception = context.get("exception")

    message = f"Task {ti.task_id} failed on {execution_date}"

    result = {
        "notification_type": "failure",
        "status": "sent",
        "message": message,
        "error": str(exception),
        "timestamp": datetime.utcnow().isoformat()
    }

    print(result)
    return result


def risky_operation(**context):
    execution_date = context["execution_date"]
    day_of_month = execution_date.day

    # Fail if day of month divisible by 5
    if day_of_month % 5 == 0:
        raise Exception(f"Simulated failure on day {day_of_month}")

    result = {
        "status": "success",
        "execution_date": str(execution_date),
        "success": True
    }

    print(result)
    return result


def cleanup_task():
    result = {
        "cleanup_status": "completed",
        "timestamp": datetime.utcnow().isoformat()
    }

    print(result)
    return result


with DAG(
    dag_id="notification_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start_task = EmptyOperator(
        task_id="start_task"
    )

    risky_task = PythonOperator(
        task_id="risky_operation",
        python_callable=risky_operation,
        on_success_callback=send_success_notification,
        on_failure_callback=send_failure_notification
    )

    success_notification_task = EmptyOperator(
        task_id="success_notification",
        trigger_rule="all_success"
    )

    failure_notification_task = EmptyOperator(
        task_id="failure_notification",
        trigger_rule="all_failed"
    )

    always_execute_task = PythonOperator(
        task_id="always_execute",
        python_callable=cleanup_task,
        trigger_rule="all_done"
    )

    start_task >> risky_task
    risky_task >> [success_notification_task, failure_notification_task]
    [success_notification_task, failure_notification_task] >> always_execute_task
