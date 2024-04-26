from airflow.decorators import dag, task
from datetime import datetime, timedelta, time
import pandas as pd
import os
import yt_vid_scraper 
import yt_comments_scraper
import boto3


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

@dag(dag_id='youtube_data', default_args=default_args, schedule=None, catchup=False, tags=['Group_Project'])
def youtube_etl():
    @task
    def extract_top_trending_videos():
        
        print("start")
        top_video_data = yt_vid_scraper.scrape()
        print("data extraction complete")

        return top_video_data
    
    @task
    def extract_comments(video_df):
        # get top trending videos data from S3 bucket
        df = video_df

        # retrive scrapped video IDs from yt_video and perform comments extraction to S3 bucket
        video_IDs = df["video_id"]
        for video in video_IDs:
            comments_data = yt_comments_scraper.retrieve_comments(video)

    ## Data Cleaning Functions ##
    def remove_non_ascii(s):
        return ''.join(char for char in s if ord(char) < 128)

    def format_duration(s):
        s = s[2:]
        hours = minutes = seconds = 0
        digits = 0
        for i in range(len(s)):
            if s[i].isalpha():
                if s[i] == 'H':
                    if digits == 1:
                        hours = s[i-1]
                    else:
                        hours = s[i-2:i]
                if s[i] == 'M':
                    if digits == 1:
                        minutes = s[i-1]
                    else:
                        minutes = s[i-2:i]
                if s[i] == 'S':
                    if digits == 1:
                        seconds = s[i-1]
                    else:
                        seconds = s[i-2:i]
                digits = 0
            else:
                digits += 1
        
        formatted_duration = ''
        if hours:
            formatted_duration += f"{hours} hour{'s' if int(hours) > 1 else ''} "
        if minutes:
            formatted_duration += f"{minutes} minute{'s' if int(minutes) > 1 else ''} "
        if seconds:
            formatted_duration += f"{seconds} second{'s' if int(seconds) > 1 else ''}"
        
        formatted_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

        return formatted_time

    @task
    def transform_youtube_data(video_df):
        ## data transformation
        df = video_df
        df['title'] = df['title'].apply(remove_non_ascii)
        df['video_duration'] = df['video_duration'].apply(format_duration)
        df['engagement_rate'] = (((df['like_count'].astype(int) + df['comment_count'].astype(int)) / df['view_count'].astype(int)) * 100)

        return df

    @task
    def load_youtube_data(df):
        ## load data into s3 bucket
        vid_df = df
        # Set AWS credentials as environment variables
        os.environ['AWS_ACCESS_KEY_ID'] = 'AKIA4MTWMYXTNMY6QWBL'
        os.environ['AWS_SECRET_ACCESS_KEY'] = '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'

        # initialize S3 client
        s3 = boto3.client('s3')
        S3_BUCKET_NAME = "is3107-bucket"

        current_date = datetime.now()
        # Format the current date into a string for naming
        csv_file_name = f"{current_date.strftime('%y.%d.%m')}_videos.csv"
        # csv_file_name = f"{time.strftime('%y.%d.%m')}_videos.csv"
        vid_df.to_csv('s3://is3107-bucket/' + csv_file_name)

    yt_video_data = extract_top_trending_videos()
    yt_comments_data = extract_comments(yt_video_data)
    cleaned_df = transform_youtube_data(yt_video_data)
    load_df = load_youtube_data(cleaned_df)

    # extract_top_trending_videos >> extract_comments >> transform_youtube_data >> load_youtube_data

youtube_etl_dag = youtube_etl()
