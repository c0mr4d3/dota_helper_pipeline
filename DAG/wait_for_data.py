import os
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from util.gcp_ops import gcp_ops


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def create_dag(dag_id , default_args):
    dag = DAG(dag_id=dag_id, schedule_interval=None, default_args=default_args)

    with dag:
        wait_for_data = GoogleCloudStoragePrefixSensor(
            task_id="Waiting_for_match_data",
            bucket="{{var.value.gcp_bucket_id}}",
            prefix="high_skill_matches/high_mmr",
        )

        rerun_dag = TriggerDagRunOperator(
            task_id="Rerun_DAG",
            trigger_dag_id=dag.dag_id,
        )

        wait_for_data >> gcp_ops(rerun_dag)

    return dag


dags_folder = os.getenv('DAGS_FOLDER', "./dags")
dag_id = "match_data_operations"

globals()[dag_id] = create_dag(dag_id, default_args)