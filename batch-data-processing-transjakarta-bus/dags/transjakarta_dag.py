from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago


PROJECT_ID = 'golden-union-392713'
REGION = 'asia-southeast2'
CLUSTER_NAME ='my-cluster'
PYSPARK_URI = 'gs://transjakarta-data/jobs/transjakarta_etl.py'

CLUSTER_CONFIG = {
    'master_config': {'num_instances': 1, 'machine_type_uri': 'n1-standard-2',
                      "disk_config": {"boot_disk_size_gb": 100}},
    'worker_config': {'num_instances': 2, 'machine_type_uri': 'n1-standard-2',
                      "disk_config": {"boot_disk_size_gb": 100}},
    'software_config': {'image_version': '2.1-ubuntu20'},
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    'transjakarta_dag',
    default_args=default_args,
    description='DAG with ETL to process Transjakarta data.',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        dag=dag,
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB, 
        dag=dag,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='stop_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        dag=dag,
    )

    create_cluster >> submit_pyspark_job >> delete_cluster
