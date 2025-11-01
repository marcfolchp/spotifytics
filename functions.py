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
    """Exchange Spotify authorization code for access + refresh tokens."""
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
    return response.json()


def get_user_info(user_id: str) -> dict:
    """Fetch Spotify user profile info via refresh token stored in MongoDB."""
    refresh_token = get_refresh_token_from_mongo(user_id)

    # Step 1: Generate new access token
    token_url = "https://accounts.spotify.com/api/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    token_response = requests.post(token_url, data=payload)
    token_response.raise_for_status()
    token_data = token_response.json()
    access_token = token_data["access_token"]

    # Step 2: Fetch Spotify profile
    profile_url = "https://api.spotify.com/v1/me"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(profile_url, headers=headers)
    response.raise_for_status()
    return response.json()


def store_user_data(user_id: str, refresh_token: str):
    """Create or update user data in MongoDB on first login."""
    existing_user = users.find_one({"_id": user_id})

    if existing_user:
        if existing_user.get("refresh_token") != refresh_token:
            users.update_one({"_id": user_id}, {"$set": {"refresh_token": refresh_token}})
            print(f"🔁 Updated refresh token for existing user {user_id}")
        else:
            print(f"✅ User {user_id} already exists — no update needed")
        return

    print(f"🆕 New user detected: fetching Spotify profile for {user_id}")

    # Get access token using refresh_token
    token_url = "https://accounts.spotify.com/api/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    token_response = requests.post(token_url, data=payload)
    token_response.raise_for_status()
    token_data = token_response.json()
    access_token = token_data["access_token"]

    # Fetch Spotify profile
    profile_url = "https://api.spotify.com/v1/me"
    headers = {"Authorization": f"Bearer {access_token}"}
    profile_response = requests.get(profile_url, headers=headers)
    profile_response.raise_for_status()
    user_info = profile_response.json()

    # Build document
    user_doc = {
        "_id": user_id,
        "display_name": user_info.get("display_name"),
        "country": user_info.get("country"),
        "email": user_info.get("email"),
        "followers": user_info.get("followers", {}).get("total") if user_info.get("followers") else None,
        "product": user_info.get("product"),
        "profile_image": (
            user_info["images"][0]["url"]
            if user_info.get("images") and len(user_info["images"]) > 0
            else None
        ),
        "created_at": datetime.utcnow(),
        "last_profile_sync": datetime.utcnow(),
        "refresh_token": refresh_token
    }

    users.insert_one(user_doc)
    print(f"✅ New user {user_id} stored in MongoDB successfully.")


def get_refresh_token_from_mongo(user_id: str) -> str:
    """Fetch the refresh token for a given user."""
    user = users.find_one({"_id": user_id}, {"refresh_token": 1})
    if not user or "refresh_token" not in user:
        raise ValueError(f"No refresh token found for user {user_id}")
    return user["refresh_token"]


def get_access_token_from_refresh(user_id: str) -> str:
    """Use a user's refresh token to get a new access token. Update DB."""
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
    expires_in = data.get("expires_in", 3600)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

    users.update_one(
        {"_id": user_id},
        {"$set": {"access_token": access_token, "expires_at": expires_at}},
        upsert=True
    )

    return access_token


def get_recently_played_tracks(user_id: str):
    """Fetch 50 most recent Spotify tracks for a user."""
    access_token = get_access_token_from_refresh(user_id)
    url = "https://api.spotify.com/v1/me/player/recently-played?limit=50"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

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
    """Upsert each playback in 'user-history'."""
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
        print(f"✅ Updated {len(tracks)} tracks for {user_id}")


def get_user_history_df(user_id: str) -> pd.DataFrame:
    """Fetch all stored songs for a user and return as DataFrame."""
    collection = db["user-history"]
    docs = list(collection.find({"user_id": user_id}, {"_id": 0}))
    if not docs:
        print(f"No history found for {user_id}")
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    if "played_at" in df.columns:
        df["played_at"] = pd.to_datetime(df["played_at"])
    return df.sort_values("played_at", ascending=False).reset_index(drop=True)
