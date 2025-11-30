from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from operators.capstone2.extract_operator import ExtractGreenTaxiOperator
from operators.capstone2.transform_operator import TransformGreenTaxiOperator
from operators.capstone2.load_operator import LoadGreenTaxiOperator
from utils.notifier import create_failure_callback, create_success_callback
from services.capstone2.p2.tasks import decide_date_task
from utils.config_loader import load_config

CONFIG_PATH = "/opt/airflow/config/config_capstone2_p2.yaml"
config = load_config(CONFIG_PATH)

WEBHOOK_URL = config['discord_webhook_url']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 25),
    # 'retries': 1,
    # "retry_delay": timedelta(minutes=3),
    "on_failure_callback": create_failure_callback(WEBHOOK_URL),
}

with DAG(
    dag_id='green_taxi_pg_etl',
    default_args=default_args,
    schedule_interval="0 5 5 * *",  # Every 5th day of month at 05:00 ATC
    catchup=False,
    max_active_runs=1,
    tags=["capstone2", "nyc", "green-taxi", "postgres", "etl"],
) as dag:

    # Task 1: PythonOperator with function factory (closure-based)
    t0 = PythonOperator(
        task_id="decide_date",
        python_callable=decide_date_task(config),  # injects config via closure
        provide_context=True
    )

    # # Task 2: Custom Operator with config injected via __init__
    extract_task = ExtractGreenTaxiOperator(
        task_id="extract_green_taxi",
        config=config,  # passed to __init__ of the custom operator
        dag=dag
    )

    # Task 3: Custom Operator with config injected via __init__
    transform_task = TransformGreenTaxiOperator(
        task_id="transform_green_taxi",
        config=config,  # passed to __init__ of the custom operator
        dag=dag
    )

    # Task 3: Custom Operator with config injected via __init__
    load_task = LoadGreenTaxiOperator(
        task_id="load_green_taxi",
        config=config,  # passed to __init__ of the custom operator
        dag=dag
    )

    t0 >> extract_task >> transform_task >> load_task
    