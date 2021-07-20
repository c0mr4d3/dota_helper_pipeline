from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
#from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.operators.dummy_operator import DummyOperator
import json

def _create_merge_sql(source, target, **context):
    return f"""
        INSERT INTO `{target}`
        SELECT * FROM `{source}`
    """

def _create_emptying_sql(source, **context):
    return f"""
        DELETE FROM `{source}`
        WHERE true
    """


def gcp_ops(next_task=DummyOperator(task_id="Done")):
    bq_staging = "{{var.value.bigquery_staging}}"
    bq_warehouse = "{{var.value.bigquery_warehouse}}"

    t1 = GoogleCloudStorageToBigQueryOperator(
        task_id="staging_matchdata",
        bucket="{{var.value.gcp_bucket_id}}",
        source_objects=["high_skill_matches/*.csv"],
        destination_project_dataset_table=bq_staging,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        schema_object="{{var.value.gcp_match_schema}}",
        skip_leading_rows=1,
    )

    t2 = BigQueryOperator(
        task_id="Load_data_to_main_warehouse",
        sql=_create_merge_sql(bq_staging, bq_warehouse),
        use_legacy_sql=False,
    )

    
    t3 = BigQueryOperator(
        task_id="Empty_Staging_Table",
        sql=_create_emptying_sql(bq_staging),
        use_legacy_sql=False,
    )

    t4 = GoogleCloudStorageToGoogleCloudStorageOperator(
        task_id="move_data_to_processed",
        source_bucket="{{var.value.gcp_bucket_id}}",
        source_object="high_skill_matches/",
        destination_bucket="{{var.value.gcp_bucket_id}}",
        destination_object="processed/",
        move_object=True,
    )

    t1 >> t2 >> t3 >> t4 >> next_task

    return t1