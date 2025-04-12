from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_KEY
import s3fs


def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                               key=AWS_ACCESS_KEY_ID,
                               secret=AWS_SECRET_KEY
                               )
        return s3
    
    except Exception as e:
        print(f"Error connecting to S3: {e}")
        # return None
    

def create_bucket_if_not_exists(s3: s3fs.S3FileSystem, bucket:str):
    
    try:        
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print(f"Bucket {bucket} created successfully.")
        else:
            print(f"Bucket {bucket} already exists.")
    except Exception as e:
        print(f"Error creating bucket: {e}")
        

def upload_file_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket: str, s3_file_name: str):
    try:
        s3.put(file_path, f"{bucket}/raw/{s3_file_name}")
        print(f"File {s3_file_name} uploaded to S3 bucket {bucket} successfully.")
    except FileNotFoundError as e:
        print(f"File not found: {e}")