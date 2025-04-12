from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.reddit_pipeline import reddit_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline

default_args = {
    'owner': 'Bharath Raju Palla',
    # 'depends_on_past': False,
    'start_date': datetime(2025, 3, 23),
    # 'email': ['V4eF2@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': datetime.timedelta(minutes=5),
}

file_postfix = datetime.now().strftime('%Y%m%d%H%M%S')

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    description='Reddit Data Pipeline',
    schedule_interval='@hourly',
    catchup=False,
    tags=['reddit', 'etl'],
)

# extraction from reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}', 
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100,        
    },
    dag=dag
)

# upload to s3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

extract >> upload_s3