import pandas as pd
import streamlit as st
import altair as alt 
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import seaborn as sns
from textblob import TextBlob
import sklearn
import boto3
from io import StringIO
import time

import os 

## Import Data from S3
# Set AWS credentials as environment variables
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIATCKAO5XHEYSQ3UGS'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'

# initialize S3 client
s3 = boto3.client('s3')
S3_BUCKET_NAME = "is3107-bucket"

daily_file_name = f"{time.strftime('%y.%d.%m')}_videos.csv"

daily_file = s3.get_object(Bucket = S3_BUCKET_NAME, Key=daily_file_name)
csv_file = daily_file['Body'].read().decode()
df = pd.read_csv(StringIO(csv_file))


st.set_page_config(
    page_title = "Youtube Plots",
    page_icon=":love_letter:",
    layout = "wide",
    initial_sidebar_state="expanded",
)

category_data = {
    1: 'Film & Animation',
    2: 'Autos & Vehicle',
    10: 'Music',
    15: 'Pets & Animals',
    17: 'Sports',
    19: 'Travel & Events',
    20: 'Gaming',
    22: 'People & Blogs',
    23: 'Comedy',
    24: 'Entertainment',
    25: 'News & Politics',
    26: 'Howto & Style',
    27: 'Education',
    28: 'Science & Technology',
    30: 'Movies',
    43: 'Shows',
    29: 'Nonprofits & Activism'
}

df['category_id'] = df['category_id'].map(category_data)

alt.themes.enable("dark")

st.sidebar.title('Page Selection')
page = st.sidebar.selectbox("Select from the following dropdown list", ["Homepage", "Category Distribution", "Views vs. Likes", "Channel Comparison",
                                               "Channel/Video Analysis",
                                               "Title Word Frequency", "Views vs. Comment Count Correlation",
                                                "Category Word Cloud",
                                                "Video Duration vs Engagement Rate Analysis"])

if page == "Homepage":
    st.title('Homepage')

    st.dataframe(df.loc[:, ['channel_title','title','view_count','category_id','engagement_rate']], hide_index=True)

elif page == "Category Distribution":
    st.header('Category Distribution')
    category_counts = df['category_id'].value_counts()
    fig = px.pie(category_counts, values=category_counts.values, names=category_counts.index, 
                 title='Distribution of Videos Across Categories')
    st.plotly_chart(fig)

elif page == "Views vs. Likes":
    st.header('Views vs. Likes')
    fig = px.scatter(df, x='view_count', y='like_count', hover_name='title', 
                     title='Views vs. Likes Scatter Plot')
    st.plotly_chart(fig)

elif page == "Channel Comparison":
    st.header('Channel Comparison')
    top_channels = df.groupby('channel_title').agg({'view_count': 'sum', 'like_count': 'sum', 'comment_count': 'sum'}).nlargest(5, 'view_count')
    top_channels = top_channels.reset_index()

    fig = px.bar(top_channels, x='channel_title', y=['view_count', 'like_count', 'comment_count'], 
                 title='Top Channels Comparison', barmode='group')
    st.plotly_chart(fig)

elif page == "Channel/Video Analysis":
    st.subheader('Video Selection')
    selected_channel = st.selectbox('Select Channel', df['channel_title'].unique())
    channel_data = df[df['channel_title'] == selected_channel]
    left_column, right_column = st.columns(2)
    with left_column: 
        st.write('Total Videos:', len(channel_data))
    with right_column:
        st.write('Total Views:', channel_data['view_count'].sum())

    st.markdown("#")

    selected_video = st.selectbox('Select Video', channel_data['title'])
    video_info = channel_data[channel_data['title'] == selected_video].iloc[0]
    st.subheader(selected_video)
    with st.expander("See Description"):
        st.write('Description:', video_info['description'])

    ## show thumbnail
    st.header('Video Thumbnail')
    st.image(video_info['thumbnail_link'], width=500)
    left_column, right_column = st.columns(2)
    with left_column: 
        st.write('Likes:', video_info['like_count'])
    with right_column:
        st.write('Comment Count:', video_info['comment_count'])
    tags_text = '|'.join(channel_data['tags'])  # Concatenate all tags into a single string separated by |
    tags_list = tags_text.split('|')  # Split the string into a list of individual tags
    tags_text_cleaned = ' '.join(tags_list)  # Join the list of tags into a single string with spaces
    
    wordcloud = WordCloud(width=300, height=100, background_color='black', margin = 5).generate(tags_text_cleaned)
    st.header('Word Cloud For Tags')
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot(plt)

    st.header('Sentiment Analysis of Comments') 

    # get comment file from s3 bucket
    com_file_name = video_info['video_id']
    vid_comment_file = s3.get_object(Bucket = S3_BUCKET_NAME, Key=com_file_name)
    comment_file = vid_comment_file['Body'].read().decode()
    df = pd.read_csv(StringIO(comment_file))

    sentiment_counts = pd.cut(df['sentimental scoring'], bins=[-1, -0.5, 0, 0.5, 1], labels=['Very Negative',  
                                                                                             'Negative', 'Neutral', 'Positive']).value_counts() 
    fig = px.pie(sentiment_counts, values=sentiment_counts.values, names=sentiment_counts.index,  
                 title='Sentiment Distribution of Comments') 
    st.plotly_chart(fig)
    

elif page == "Title Word Frequency":
    st.header('Title Word Frequency')
    titles_text = ' '.join(df['title'])
    titles_list = titles_text.split()
    title_freq = pd.Series(titles_list).value_counts().nlargest(10)
    fig = px.bar(x=title_freq.index, y=title_freq.values, labels={'x': 'Word', 'y': 'Frequency'}, 
                 title='Top Words in Video Titles')
    st.plotly_chart(fig)

elif page == "Views vs. Comment Count Correlation":
    st.header('Views vs. Comment Count Correlation')
    fig = px.scatter(df, x='view_count', y='comment_count', trendline='ols', 
                     title='Correlation Between Views and Comment Count')
    st.plotly_chart(fig)

elif page == "Category Word Cloud": 
    selected_category = st.selectbox('Select Category', df['category_id'].unique())
    selected_tags = df[df['category_id'] == selected_category]['tags'].values.tolist()
    combined_tags = ' '.join(selected_tags)
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(combined_tags)
    
    # Display word cloud
    st.title('Word Cloud for Tags')
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot(plt)

elif page == "Video Duration vs Engagement Rate Analysis":
    st.title('Duration Vs Engagement Rate Analysis')
    dur_df = df
    dur_df['video_duration'] = pd.to_datetime(dur_df['video_duration'])
    dur_df['seconds'] =[(t.hour * 3600 + t.minute * 60 + t.second) for t in dur_df['video_duration']]
    fig = px.scatter(dur_df, x='seconds', y='engagement_rate', title = 'Distribution of Video Duration against Engagement Rate')
    st.plotly_chart(fig)

