from datetime import date
from dotenv import load_dotenv
import os
from grpc import Channel
import requests
import json
from airflow.decorators import task
from airflow.models import Variable

# load_dotenv()

# CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")
# API_KEY = os.getenv("API_KEY")


API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
max_results = 50

@task
def get_playlist_ID():
    """
    Gets the channel playlist ID from the youtube API and returns a JSON Formatted String.

    """
    
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
    
    try:
        response = requests.get(url=url)
        data = response.json()
        with open("ChannelOutput.json", "w") as f:
            json.dump(data, f, indent=4)
        channel_items = data["items"][0]
        channel_playlistID = channel_items["contentDetails"]["relatedPlaylists"][
            "uploads"
        ]
        return channel_playlistID
    #   This excpetions is to catch request's exceptions while using the get/set methods and comes from "json" module.
    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_ID(playlistID: str):
    """
    gets the VideoID of the respective playlistID and can give a min of 1 result or max 50 results (API Bound).

    """
    nextPageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlistID}&key={API_KEY}"
    
    try:
        response = requests.get(url=base_url)
        data = response.json()
        with open("UploadStats.json", "w") as f:
            json.dump(data, f, indent=4)
        videoID = []
        
        while True:
            for item in data["items"]:
                videoID.append(item["contentDetails"]["videoId"])
                
            if "nextPageToken" not in data:
                break
            
            nextPageToken = data["nextPageToken"]
            url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&pageToken={nextPageToken}&playlistId={playlistID}&key={API_KEY}"
            response = requests.get(url=url)
            data = response.json()
            
                
        return videoID

    except requests.exceptions.RequestException as e:
        raise e
  
    
@task   
def extract_video_data(video_ids):
    """
    gets the Video data using the Video IDs we got earlier.
    """
    extracted_data = []
    
    def batch_list(video_id_lst, batch_size):
        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id: video_id + batch_size]
            
    try:
        for batch in batch_list(video_ids, max_results):
            video_ids_str = ",".join(batch)
        
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=statistics&part=contentDetails&part=snippet&id={video_ids_str}&key={API_KEY}"
        
            response = requests.get(url)
        
            response.raise_for_status()
        
            data = response.json()
        
            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
            
                video_data = {
                    "video_id" : video_id,
                    "title" : snippet['title'],
                    "publisedAt" : snippet['publishedAt'],
                    "duration" : contentDetails['duration'],      
                    "viewCount" : statistics.get('viewCount', None),
                    "likeCount" : statistics.get('likeCount', None),
                    "commentCount" : statistics.get('commentCount', None),    
                } 
            
                extracted_data.append(video_data) 
              
        return extracted_data 
       
    except requests.exceptions.RequestException as e:
        raise e                            


@task
def save_to_json(extracted_data):
    
    file_path = f"./data/YT_data{date.today()}.json"
    
    with open(file_path, "w", encoding = "utf-8") as f:
        json.dump(extracted_data, f, indent = 4, ensure_ascii= False )
        
    
if __name__ == "__main__":
    playlistID = get_playlist_ID()
    videoIDs = get_video_ID(playlistID)
    
    video_data = extract_video_data(videoIDs)
    save_to_json(video_data)

