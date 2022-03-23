from datetime import datetime, timedelta

import json
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python import get_current_context

from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    ALREADY_EXISTING_IN_SINK,
    AWS_S3_DATA_SOURCE,
    BUCKET_NAME,
    DESCRIPTION,
    FILTER_JOB_NAMES,
    FILTER_PROJECT_ID,
    GCS_DATA_SINK,
    JOB_NAME,
    OBJECT_CONDITIONS,
    PROJECT_ID,
    SCHEDULE,
    SCHEDULE_END_DATE,
    SCHEDULE_START_DATE,
    START_TIME_OF_DAY,
    STATUS,
    TRANSFER_OPTIONS,
    TRANSFER_SPEC,
    GcpTransferJobsStatus,
    GcpTransferOperationStatus,
)
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator


@task()
def transform_message():
    context = get_current_context()
    ti = context["ti"]
    messages = ti.xcom_pull(key='messages')
    include_list = []
    for msg in messages:
        body = json.loads(msg['Body'])
        for record in body['Records']:
            #print(record['s3']['object']['key'])
            key = record['s3']['object']['key']
            print(key)
            include_list.append(key)
    return include_list

@task()
def create_transfer():
    context = get_current_context()
    ti = context["ti"]
    include_list = ti.xcom_pull(key='return_value')
    print(type(include_list))
    print(include_list)

    now = datetime.utcnow()
    one_time_schedule = {
        'day': now.day,
        'month': now.month,
        'year': now.year
    }

    execution_date = ti.execution_date.strftime("%m%d%Y%H%M%S")
	 
    gcp_transfer_job_name = f"transferJobs/{ti.task_id}-{execution_date}"
   

    aws_to_gcs_transfer_body = {
        DESCRIPTION: "transfer job from aws",
        STATUS: GcpTransferJobsStatus.ENABLED,
        PROJECT_ID: "YOUR_GCP_PROJECT_ID",
        JOB_NAME: gcp_transfer_job_name,
        SCHEDULE: {
            SCHEDULE_START_DATE: one_time_schedule,
            SCHEDULE_END_DATE: one_time_schedule,
            },
            TRANSFER_SPEC: {
                AWS_S3_DATA_SOURCE: {BUCKET_NAME: "YOUR_AWS_BUCKET"},
                GCS_DATA_SINK: {BUCKET_NAME: "YOUR_GCP_BUCKET"},
                TRANSFER_OPTIONS: {ALREADY_EXISTING_IN_SINK: True},
                OBJECT_CONDITIONS:{ "include_prefixes": include_list }
                },
    }

    print(json.dumps(aws_to_gcs_transfer_body))

    create_transfer_job_from_aws = CloudDataTransferServiceCreateJobOperator(
        task_id="create_transfer_job_from_aws", body=aws_to_gcs_transfer_body
    )

    create_transfer_job_from_aws.execute(context)

       

with DAG(
    dag_id='transfer_from_aws',
    schedule_interval=None,
    start_date=datetime(2021, 3, 15),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
) as dag:
   

    read_from_queue = SqsSensor(
        task_id='read_from_queue',
        sqs_queue='URI_OF_SQS_QUEUE',
        max_messages=10,
        wait_time_seconds=20,
        visibility_timeout=None,
        message_filtering=None,
        message_filtering_match_values=None,
        message_filtering_config=None,
    )

    
         
    
read_from_queue >> transform_message() >> create_transfer()
