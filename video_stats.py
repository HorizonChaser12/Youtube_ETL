from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()


def get_playlist_ID(url: str):
    """
    Gets the channel playlist ID from the youtube API and returns a JSON Formatted String.

    """
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


if __name__ == "__main__":

    ChannelName = "BeastBoyShub"
    youtube_api_key = os.getenv("youtube_api_key")

    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={ChannelName}&key={youtube_api_key}"

    print(get_playlist_ID(url))
