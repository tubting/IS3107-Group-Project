from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from datetime import datetime, timedelta
import pandas as pd
import io
import boto3
import requests, sys, time, os, argparse
# import string

# Set AWS credentials as environment variables
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIA4MTWMYXTNMY6QWBL'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'K82xfyJWIf6ytf40s7y82JmYrQYgENLxATQJAYGh'

# initialize S3 client
s3 = boto3.client('s3')
S3_BUCKET_NAME = "is3107-bucket"

def scrape():

    # Define your API key and other parameters
    # API_KEY = 'AIzaSyDjdGOX9RovL60ejhNcEM5npBi6O-YsUds' 
    API_KEY = 'AIzaSyA5m6DnqC7i1bRFiAG5QTB4_I6YbqEnMfY'
    REGION_CODE = 'US'  # Default region code

    # Authenticate and build the API client
    youtube = build('youtube', 'v3', developerKey=API_KEY)

    # Call API to retrieve the most popular videos
    full_df = []
    hasNextPage = True
    while True:
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            chart='mostPopular',
            regionCode=REGION_CODE,
            maxResults=50  # Number of videos to retrieve
        )
        response = request.execute()
        stats_df = get_stats(response)
        full_df.append(stats_df)
        hasNextPageToken = response.get('nextPageToken')
        if not hasNextPageToken:
            df = pd.DataFrame(full_df)
            break

    # write to file

    csv_file_name = f"{time.strftime('%y.%d.%m')}_videos.csv"
    df.to_csv(csv_file_name, index=False)

    return df


def get_stats(res):
    top_trending_videos = []
    for video in res['items']:
        video_stats = {
            "video_id" : video['id'],
            "title" : video['snippet']['title'],
            "description" : video['snippet']['description'],
            "category_id": video['snippet']['categoryId'],
            "publish_date" : video['snippet']['publishedAt'],
            "channel_title" : video['snippet']['channelTitle'],
            "tags" : video['snippet']['tags'] if 'tags' in video['snippet'] else [],
            "trending_date" : time.strftime("%y.%d.%m"),  # date video appears on most popular charts
            "view_count" : video['statistics'].get("viewCount", 0),
            "like_count" : video['statistics'].get('likeCount', 0),
            "comment_count" : video['statistics'].get('commentCount', 0),
            "video_duration" : video['contentDetails']['duration'],
            "thumbnail_link": video['snippet'].get("thumbnails", dict()).get("default", dict()).get("url", "")
        }
        top_trending_videos.append(video_stats)

    df = pd.DataFrame(top_trending_videos)

    return df
