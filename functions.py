import os
import requests
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from pymongo.server_api import ServerApi
import pandas as pd


# ---- LOAD ENVIRONMENT VARIABLES ----

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")


# ---- CONNECT TO MONGO ----

client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
db = client[MONGO_DB]
users = db["users"]


# ---- SPOTIFY AUTH FUNCTIONS ----

def exchange_code_for_tokens(auth_code: str):
    """
    Exchange Spotify authorization code for access + refresh tokens.
    """
    url = "https://accounts.spotify.com/api/token"
    payload = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(url, data=payload)
    response.raise_for_status()

    tokens = response.json()

    return tokens

def store_user_tokens(user_id: str, refresh_token: str):
    """
    Insert or update a user's tokens in MongoDB.
    """
    users.update_one(
        {"_id": user_id},
        {"$set": {
            "refresh_token": refresh_token,
        }},
        upsert=True
    )

def get_refresh_token_from_mongo(user_id: str) -> str:
    """
    Fetch the refresh token for a given user.
    """

    user = users.find_one({"_id": user_id}, {"refresh_token": 1})

    if not user or "refresh_token" not in user:
        raise ValueError(f"No refresh token found for user {user_id}")
    
    return user["refresh_token"]

def get_user_info(user_id: str) -> dict:
    """
    Fetch detailed Spotify user profile information for a given user_id.
    Automatically refreshes the access token if necessary.
    Returns a dictionary with user data (display_name, followers, country, etc.).
    """
    # Get a valid access token (refresh if needed)
    access_token = get_access_token_from_refresh(user_id)

    # Spotify "Get Current User's Profile" endpoint
    url = "https://api.spotify.com/v1/me"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    return data

def get_access_token_from_refresh(user_id: str) -> str:
    """
    Use a user's refresh token (from MongoDB) to get a new access token.
    Updates MongoDB automatically.
    """
    refresh_token = get_refresh_token_from_mongo(user_id)

    url = "https://accounts.spotify.com/api/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(url, data=payload)
    response.raise_for_status()
    data = response.json()

    access_token = data["access_token"]

    user_info = get_user_info(user_id)

    # --- Build update document for MongoDB ---
    update_data = {
        "access_token": access_token,
        "display_name": user_info.get("display_name"),
        "country": user_info.get("country"),
        "email": user_info.get("email"),
        "followers": user_info.get("followers", {}).get("total") if user_info.get("followers") else None,
        "product": user_info.get("product"),  # e.g., 'premium' or 'free'
        "profile_image": (
            user_info["images"][0]["url"]
            if user_info.get("images") and len(user_info["images"]) > 0
            else None
        ),
        "last_profile_sync": datetime.utcnow(),
        "refresh_token": refresh_token
    }

    # --- Update MongoDB document ---
    users.update_one({"_id": user_id}, {"$set": update_data}, upsert=True)

    return access_token

def get_recently_played_tracks(user_id: str):
    """
    Fetch the 50 most recently played Spotify tracks for a given user.
    Automatically refreshes access token from MongoDB.
    """
    # Get a fresh access token from refresh token
    access_token = get_access_token_from_refresh(user_id)

    # Spotify API endpoint for recently played
    url = "https://api.spotify.com/v1/me/player/recently-played?limit=50"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Clean + simplify the response
    tracks = []
    for item in data.get("items", []):
        track = item["track"]
        played_at = item["played_at"]

        tracks.append({
            "name": track["name"],
            "artist": track["artists"][0]["name"],
            "album": track["album"]["name"],
            "album_image": track["album"]["images"][0]["url"] if track["album"]["images"] else None,
            "uri": track["uri"],
            "played_at": played_at
        })

    return tracks

def update_user_history(user_id: str):
    """
    Stores each playback as its own document in 'user-history-flat'.
    _id = user_id + played_at ensures uniqueness.
    """
    collection = db["user-history"]
    tracks = get_recently_played_tracks(user_id)
    if not tracks:
        return

    operations = []
    for t in tracks:
        doc_id = f"{user_id}_{t['played_at']}"
        operations.append(UpdateOne(
            {"_id": doc_id},
            {"$setOnInsert": {"user_id": user_id, **t}},
            upsert=True
        ))

    if operations:
        result = collection.bulk_write(operations, ordered=False)

def get_user_history_df(user_id: str) -> pd.DataFrame:
    """
    Fetch all stored songs for a given user from MongoDB
    and return them as a pandas DataFrame.
    """
    collection = db["user-history"]
    # Query all plays by that user
    docs = list(collection.find({"user_id": user_id}, {"_id": 0}))  # omit Mongo's _id

    if not docs:
        print(f"No history found for user '{user_id}'")
        return pd.DataFrame()  # empty df
    
    # Convert to DataFrame
    df = pd.DataFrame(docs)

    # Convert played_at to datetime for easy sorting/analysis
    if "played_at" in df.columns:
        df["played_at"] = pd.to_datetime(df["played_at"])

    # Sort by most recent
    df = df.sort_values("played_at", ascending=False).reset_index(drop=True)
    
    return df