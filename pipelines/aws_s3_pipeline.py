from etls.aws_etl import connect_to_s3, create_bucket_if_not_exists, upload_file_to_s3
from utils.constants import AWS_BUCKET_NAME

def upload_s3_pipeline(ti):
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')
    
    s3 = connect_to_s3()
    create_bucket_if_not_exists(s3, AWS_BUCKET_NAME)
    upload_file_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])