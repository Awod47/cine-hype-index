import os
import pandas as pd

import datetime
from dateutil.parser import isoparse
from dotenv import load_dotenv

import googleapiclient.discovery



movies = {
    "Heretic": {
        "id": "O9i2vmFhSSY",
        "release_date": datetime.datetime(2024, 11, 21, tzinfo=datetime.timezone.utc)
    },
    "Dune Part 2": {
        "id": "Way9Dexny3w",
        "release_date": datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc)
    },
    "Nosferatu": {
        "id": "nulvWqYUM8k",
        "release_date": datetime.datetime(2024, 12, 25, tzinfo=datetime.timezone.utc)
    },
    "Godzilla X Kong": {
        "id": "lV1OOlGwExM",
        "release_date": datetime.datetime(2024, 4, 12, tzinfo=datetime.timezone.utc)
    },
    "Fall guy": {
        "id": "j7jPnwVGdZ8",
        "release_date": datetime.datetime(2024, 5, 3, tzinfo=datetime.timezone.utc)
    },
    "Kingdom of the Planet of the Apes": {
        "id": "XtFI7SNtVpY",
        "release_date": datetime.datetime(2024, 5, 10, tzinfo=datetime.timezone.utc)
    },
    "Bad Boys 4": {
        "id": "hRFY_Fesa9Q",
        "release_date": datetime.datetime(2024, 6, 5, tzinfo=datetime.timezone.utc)
    },
    "Lisa Frankenstein": {
        "id": "POOeA3zCuUY",
        "release_date": datetime.datetime(2024, 2, 9, tzinfo=datetime.timezone.utc)
    },
    "Deadpool 3": {
        "id": "73_1biulkYk",
        "release_date": datetime.datetime(2024, 7, 26, tzinfo=datetime.timezone.utc)
    },
    "Alien Romulus": {
        "id": "OzY2r2JXsDM",
        "release_date": datetime.datetime(2024, 8, 16, tzinfo=datetime.timezone.utc)
    },
    "Transformers One": {
        "id": "0rmJXXKDrsM",
        "release_date": datetime.datetime(2024, 9, 13, tzinfo=datetime.timezone.utc)
    },
    "Joker Folie à Deux": {
        "id": "_OKAwz2MsJs",
        "release_date": datetime.datetime(2024, 10, 4, tzinfo=datetime.timezone.utc)
    },
    "Smile 2": {
        "id": "0HY6QFlBzUY",
        "release_date": datetime.datetime(2024, 10, 18, tzinfo=datetime.timezone.utc)
    },
    "Venom 3": {
        "id": "__2bjWbetsA",
        "release_date": datetime.datetime(2024, 10, 25, tzinfo=datetime.timezone.utc)
    },
    "Red One": {
        "id": "U8XH3W0cMss",
        "release_date": datetime.datetime(2024, 11, 15, tzinfo=datetime.timezone.utc)
    },
    "The Book of Clarence": {
        "id": "aTMqRPOqkGs",
        "release_date": datetime.datetime(2024, 1, 12, tzinfo=datetime.timezone.utc)
    },
    "Madame Web": {
        "id": "s_76M4c4LTo",
        "release_date": datetime.datetime(2024, 2, 14, tzinfo=datetime.timezone.utc)
    },
    "Road House": {
        "id": "Y0ZsLudtfjI",
        "release_date": datetime.datetime(2024, 3, 21, tzinfo=datetime.timezone.utc)
    },
    "Gladiator 2": {
        "id": "4rgYUipGJNo",
        "release_date": datetime.datetime(2024, 11, 22, tzinfo=datetime.timezone.utc)
    },
    "Mufasa The lion king": {
        "id": "o17MF9vnabg",
        "release_date": datetime.datetime(2024, 12, 20, tzinfo=datetime.timezone.utc)
    },
    "Sonic the hedgehog 3": {
        "id": "qSu6i2iFMO0",
        "release_date": datetime.datetime(2024, 12, 20, tzinfo=datetime.timezone.utc)
    },
    "Wicked": {
        "id": "6COmYeLsz4c",
        "release_date": datetime.datetime(2024, 12, 5, tzinfo=datetime.timezone.utc)
    },
    "Mean girls": {
        "id": "fFtdbEgnUOk",
        "release_date": datetime.datetime(2024, 1, 12, tzinfo=datetime.timezone.utc)
    },
    "Kraven the hunter": {
        "id": "I8gFw4-2RBM",
        "release_date": datetime.datetime(2024, 12, 13, tzinfo=datetime.timezone.utc)
    },
    "Borderlands": {
        "id": "lU_NKNZljoQ",
        "release_date": datetime.datetime(2024, 8, 9, tzinfo=datetime.timezone.utc)
    },
}


def fetch_comments():

    load_dotenv()
    api_service_name = "youtube"
    api_version = "v3"
    developer_key = os.getenv('API_KEY')

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=developer_key
    )

    for key,value in movies.items():
        print(key, value['id'], value['release_date'])

        release_date = value['release_date']
        window_start = release_date - datetime.timedelta(days=120)
        window_end   = release_date + datetime.timedelta(days=120)


        comments = []
        next_page_token = None

        while True:

            request = youtube.commentThreads().list(
                part="snippet",
                videoId= value['id'],
                maxResults=200,
                pageToken=next_page_token,
                order="time"  
            )
            response = request.execute()

            stop_fetching = False

            for item in response["items"]:
                c = item["snippet"]["topLevelComment"]["snippet"]
                ts = isoparse(c["publishedAt"])

                if ts > window_end:
                    continue

                if ts < window_start:
                    stop_fetching = True
                    break

                # Inside window → save
                comments.append([
                    c["authorDisplayName"],
                    c["publishedAt"],
                    c["updatedAt"],
                    c["likeCount"],
                    c["textDisplay"]
                ])

            if stop_fetching:
                break

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        df = pd.DataFrame(
            comments,
            columns=["author", "published_at", "updated_at", "like_count", "text"]
        )

        df.to_csv(f"../comments/{key}.csv", index=False)


fetch_comments()