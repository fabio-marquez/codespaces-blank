from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from util.spotify import SpotifyAPI
from util.storage import Storage

# Get the current date and time
current_time = datetime.now()

# Format it as yyyy_mm_dd_hh_mm_ss
formatted_time = current_time.strftime('%Y_%m_%d')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_spotify',
    default_args=default_args,
    description='Spotify data lake',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['spotify', 'landing'],
) as dag:
    with TaskGroup(group_id='spotify_ingestion') as spotify_ingestion:

        def ingestion(s3_path: str):
            api = SpotifyAPI()
            json_data = api.get_top50_releases()
            storage = Storage()
            storage.save_top50_releases_to_bucket(data=json_data, path=s3_path)

        PythonOperator(
            task_id='spotify_top50_releases_ingestion',
            python_callable=ingestion,
            op_kwargs={'s3_path': f's3a://landing/{formatted_time}/'},
        )