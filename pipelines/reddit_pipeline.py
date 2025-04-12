import pandas as pd
from utils.constants import CLIENT_ID, SECRET, USER_AGENT, OUTPUT_PATH
from etls.reddit_etl import connect_reddit, extract_posts,transform_data, load_data_to_csv



def reddit_pipeline(file_name, subreddit, time_filter='day', limit=None):
    # connect to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, USER_AGENT)
    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    # transformation
    transformed_df = transform_data(post_df)
    # loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(transformed_df, file_path)
    
    return file_path