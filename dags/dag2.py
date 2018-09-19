import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from operators import HttpToGcsOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id="my_second_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 6, 20),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


def print_exec_date(**context):
    print(context["execution_date"])


my_task = PythonOperator(
    task_id="task_name",
    python_callable=print_exec_date,
    provide_context=True,
    dag=dag
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    postgres_conn_id="postgres_airflow_training",
    sql="SELECT * FROM public.land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket='airflow-training-knab-geert',
    filename='land_registry_price_paid_uk/{{ ds }}/properties_{}.json',
    dag=dag
)


for currency in {'EUR', 'USD'}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=" + currency,
        http_conn_id="http_airflow_training",
        gcs_conn_id="google_cloud_default",
        gcs_bucket="airflow-training-knab-geert",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag
    )


dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="gdd-25d677142443a8e2ace1927d48",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)


compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://airflow-training-knab-geert/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=["{{ ds }}"],
    dag=dag,
)


dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="gdd-25d677142443a8e2ace1927d48",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
