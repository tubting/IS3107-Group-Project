import os
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pandas as pd
import boto3
from io import StringIO
from textblob import TextBlob

# Set AWS credentials as environment variables
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIA4MTWMYXTNMY6QWBL'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'

# initialize S3 client
s3 = boto3.client('s3')
S3_BUCKET_NAME = "is3107-bucket"

def retrieve_comments(vidID):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "AIzaSyA5m6DnqC7i1bRFiAG5QTB4_I6YbqEnMfY"

    ytvidID = vidID
    # ytvidID = "pkIr5FaNGhU" # testing
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)
    # Request from a specific video ID
    # looping through multiple pages
    # empty comment retrieval due to error or disabled comments
    try:
        request = youtube.commentThreads().list(
        part="snippet",
        videoId=ytvidID,
        maxResults=1,
        )
        # Check Comments Disabled
        response = request.execute()
        df = pd.DataFrame()
        # Check for disabled comment section - Check if first Author = 0
        if not response["items"]:
            emptyComments = [{"author": 0, "comment" : "Comment Section is disabled", 
                              "like count": 0, 'sentimental scoring': 0}]
            df = pd.DataFrame(emptyComments)
        else:
            # retrieve comments
            hasNextPageToken = None
            allComments = []
            while True:
                request = youtube.commentThreads().list(
                part="snippet",
                videoId=ytvidID,
                pageToken = hasNextPageToken,
                maxResults=100,
                # replace with vidID later
                )
                response = request.execute()
                # Append Comments
                for comment in response["items"]:
                    author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
                    like_count = comment['snippet']['topLevelComment']['snippet']['likeCount']
                    # Sentimental Scoring - Analysis
                    blob = TextBlob(comment_text)
                    score = blob.sentiment.polarity
                    comment_info = {'author': author, 
                                'comment': comment_text, 'like count': like_count, 'sentimental scoring': score}
                    allComments.append(comment_info)
                hasNextPageToken = response.get('nextPageToken')
                if not hasNextPageToken:
                    df = pd.DataFrame(allComments)
                    break
    except HttpError as errorHandle:
        if errorHandle.resp.status == 403 and "commentsDisabled" in str(errorHandle):
            # For disabled comments - Check if first Author = 0
            emptyComments = [{"author": 0, "comment" : "Comment Section is disabled", "like count": 0, 'sentimental scoring': 0}]
            df = pd.DataFrame(emptyComments)
        else:
            # Error Message in video - Check if first Author = 1
            emptyComments = [{"author": 1, "comment" : f"Error has occurred while fetching comments: {str(errorHandle)}",
                              "like count": 0, 'sentimental scoring': 0}]
            df = pd.DataFrame(emptyComments)

    # saving file to csv
    # csv_buffer = StringIO()
    # csv_file = df.to_csv(csv_buffer)

    # send scrapped data to S3 bucket
    csv_file_name = 'ytComments_' + ytvidID + '.csv'
    # upload CSV file to S3
    df.to_csv(f"s3://is3107-bucket/video_comments/{csv_file_name}")  
    