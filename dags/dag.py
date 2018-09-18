import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id="my_first_dag",
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
        gcs_bucket="airflow-training-knab-geert"
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    ) >> pgsl_to_gcs
