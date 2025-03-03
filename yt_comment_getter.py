import pandas as pd
import time
import sys
from googleapiclient.discovery import build
from rich.console import Console
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")
MAX_VIDEO = int(os.getenv("MAX_VIDEO", 100))
MAX_COMMENT = int(os.getenv("MAX_COMMENT", 1000))
MAX_TOTAL_COMMENTS = int(os.getenv("MAX_TOTAL_COMMENTS", 15000))

console = Console(record=False)
youtube = build(api_service_name, api_version, developerKey=DEVELOPER_KEY)

# Function to search videos on YouTube
def search_videos(youtube, query, max_results):
    videos = []
    next_page_token = None
    while True:
        request = youtube.search().list(
            q=query,
            part='snippet',
            type='video',
            maxResults=min(max_results, 100),
            publishedAfter='2023-09-01T00:00:00Z',
            publishedBefore='2024-05-31T23:59:59Z',
            order='viewCount',
            pageToken=next_page_token
        )
        response = request.execute()
        
        for item in response['items']:
            videos.append({
                'id': item['id']['videoId'],
                'title': item['snippet']['title'],
                'date': item['snippet']['publishedAt']
            })
        
        next_page_token = response.get('nextPageToken')
        if not next_page_token or len(videos) >= max_results:
            break
        
        time.sleep(1)  # Avoid hitting API rate limits
    
    return videos

# Function to get comments from a video
def get_video_comments(youtube, video_id, max_comments, total_comments_counter, max_total_comments):
    comments = []
    comment_counter = 0
    next_page_token = None
    
    while True:
        if total_comments_counter >= max_total_comments:
            console.log("[yellow]Reached maximum allowed total comments.")
            break
        
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                pageToken=next_page_token,
                textFormat="plainText",
                maxResults=min(max_comments, 100)
            )
            response = request.execute()
            
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                
                if comment_counter >= max_comments or total_comments_counter >= max_total_comments:
                    break
                
                comments.append({
                    'author': comment['authorDisplayName'],
                    'text': comment['textOriginal'].replace('\n', '\\n').replace('\r', '\\r'),
                    'date': comment['publishedAt']
                })
                
                comment_counter += 1
                total_comments_counter += 1
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token or comment_counter >= max_comments or total_comments_counter >= max_total_comments:
                break
            
            time.sleep(1)
        except Exception as e:
            console.log(f"[red]Error retrieving comments: {e}")
            break
    
    return comments, comment_counter, total_comments_counter

# Function to write data to a CSV
def write_to_csv(data, filename="youtube_data.csv"):
    try:
        df = pd.DataFrame(data)
        if os.path.isfile(filename):
            df.to_csv(filename, mode='a', index=False, header=False, encoding='utf-8')
        else:
            df.to_csv(filename, index=False, encoding='utf-8')
        console.log(f"[green]Data successfully written to {filename}")
    except Exception as e:
        console.log(f"[red]Error writing to CSV: {e}")

# Main function to collect data from YouTube
def get_data_from_youtube(query, max_video, max_comment, max_total_comments):
    console.rule()
    youtube_data = []
    total_comments_counter = 0
    console.log(f"[yellow][bold]Searching for videos on YouTube for query: '{query}'")
    videos = search_videos(youtube, query, max_results=max_video)
    console.log(f"[green]Found {len(videos)} videos for query: '{query}'")
    
    for video in videos:
        if total_comments_counter >= max_total_comments:
            console.log("[yellow]Reached the allowed limit for total comments. Stopping data collection.")
            break
        
        video_id = video['id']
        video_title = video['title']
        video_date = video['date']
        console.rule()
        console.log(f"[blue][bold]Getting comments for video: {video_title}")
        
        comments, total_comment, total_comments_counter = get_video_comments(
            youtube, video_id, max_comments=max_comment,
            total_comments_counter=total_comments_counter, max_total_comments=max_total_comments
        )
        
        console.log(f"[green]Found {len(comments)} comments for video: {video_title}")
        
        for comment in comments:
            youtube_data.append({
                'query': query,
                'video_id': video_id,
                'video_title': video_title,
                'video_date': video_date,
                'comment_text': comment['text'],
                'comment_date': comment['date']
            })
        
    write_to_csv(youtube_data)

# Function to read queries from daftar_query.txt and remove each after processing
def process_queries_from_file():
    with open('daftar_query.txt', 'r') as f:
        queries = f.readlines()
    
    queries = [query.strip() for query in queries if query.strip()]
    
    for query in queries:
        get_data_from_youtube(query, max_video=MAX_VIDEO, max_comment=MAX_COMMENT, max_total_comments=MAX_TOTAL_COMMENTS)
        
        # Remove processed query from file
        with open('daftar_query.txt', 'r') as f:
            lines = f.readlines()
        with open('daftar_query.txt', 'w') as f:
            for line in lines:
                if line.strip() != query:
                    f.write(line)

    console.log("[green]All queries have been processed and removed from 'daftar_query.txt'.")

if __name__ == "__main__":
    process_queries_from_file()
