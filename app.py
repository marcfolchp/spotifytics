from flask import Flask, redirect, request, render_template, session, url_for
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta
import os
from collections import defaultdict, Counter
from flask import jsonify
from dotenv import load_dotenv
from functions import store_user_data, get_total_play_time, get_user_top_artists, get_user_top_tracks, get_spotify_client

# ---- CONFIG ----
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
SCOPE = os.getenv("SCOPE")
SECRET_KEY = os.getenv("SECRET_KEY")  # needed for Flask session

app = Flask(__name__)
app.secret_key = SECRET_KEY

@app.route("/")
def home():
    """Landing page before login."""
    return render_template("login.html")

@app.route("/login")
def login():
    sp_oauth = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        show_dialog=True
    )

    auth_url = sp_oauth.get_authorize_url()
    return redirect(auth_url)


@app.route("/callback")
def callback():
    sp_oauth = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
    )

    # --- Get authorization code from Spotify ---
    code = request.args.get("code")
    if not code:
        return "Missing authorization code", 400

    # --- Exchange code for tokens (SpotifyOAuth handles it) ---
    token_info = sp_oauth.get_access_token(code, check_cache=False)
    access_token = token_info["access_token"]
    refresh_token = token_info.get("refresh_token")

    # --- Get user profile info from Spotify ---
    sp = get_spotify_client(session["user"]["id"])
    user = sp.current_user()
    user_id = user["id"]

    # ✅ Store or update tokens in MongoDB
    store_user_data(user_id, refresh_token)

    # --- Save minimal info in Flask session ---
    session["token_info"] = token_info
    session["user"] = {
        "id": user_id,
        "name": user["display_name"],
        "image": user["images"][0]["url"] if user["images"] else None
    }

    return redirect(url_for("top_tracks"))


@app.route("/general")
def general():
    token_info = session.get("token_info")
    if not token_info:
        return redirect(url_for("home"))

    sp = get_spotify_client(session["user"]["id"])
    user = sp.current_user()
    user_id = user["id"]

    total_ms = get_total_play_time(user_id)

    # --- Top track
    top_tracks = get_user_top_tracks(user_id, time_range="short_term", limit=1)
    favorite_song = top_tracks[0]["name"]
    favorite_song_artist = top_tracks[0]["artist"]
    favorite_song_image = top_tracks[0]["image"]

    # ---- FAVORITE ARTIST ----
    top_artists = get_user_top_artists(user_id, time_range="short_term", limit=1)
    favorite_artist = top_artists[0]["name"]
    favorite_artist_image = top_artists[0]["image"]

    return render_template(
        "general.html",
        total_minutes=total_ms,
        favorite_song=favorite_song,
        favorite_song_artist=favorite_song_artist,
        favorite_song_image=favorite_song_image,
        favorite_artist=favorite_artist,
        favorite_artist_image=favorite_artist_image
    )

@app.route("/top-tracks")
def top_tracks():

    token_info = session.get("token_info")
    if not token_info:
        return redirect(url_for("home"))
    
    sp = get_spotify_client(session["user"]["id"])

    # --- Get selected time range from URL (default = short_term)
    time_range = request.args.get("range", "short_term")

    # --- Fetch top tracks using selected range
    results = sp.current_user_top_tracks(limit=50, time_range=time_range)

    tracks = [
        {
            "name": item["name"],
            "artist": item["artists"][0]["name"],
            "image": item["album"]["images"][1]["url"] if item["album"]["images"] else None
        }
        for item in results["items"]
    ]

    # pass the current range to the template
    return render_template("top_tracks.html", tracks=tracks, time_range=time_range)


@app.route("/api/top-tracks/<time_range>")
def api_top_tracks(time_range):
    token_info = session.get("token_info")
    if not token_info:
        return redirect(url_for("home"))
    sp = get_spotify_client(session["user"]["id"])

    results = sp.current_user_top_tracks(limit=50, time_range=time_range)
    tracks = [
        {
            "name": t["name"],
            "artist": t["artists"][0]["name"],
            "image": t["album"]["images"][1]["url"] if t["album"]["images"] else None
        }
        for t in results["items"]
    ]
    return {"tracks": tracks}


@app.route("/top-artists")
def top_artists():
    token_info = session.get("token_info")
    if not token_info:
        return redirect(url_for("home"))
    sp = get_spotify_client(session["user"]["id"])

    # --- Get selected time range from query string (default: short_term)
    time_range = request.args.get("range", "short_term")

    # --- Fetch top artists
    results = sp.current_user_top_artists(limit=10, time_range=time_range)

    artists = [
        {
            "name": artist["name"],
            "image": artist["images"][1]["url"] if artist["images"] else None,
            "genres": ", ".join(artist["genres"][:2]) if artist["genres"] else "",
        }
        for artist in results["items"]
    ]

    return render_template("top_artists.html", artists=artists, time_range=time_range)


@app.route("/top-genres")
def top_genres():
    token_info = session.get("token_info")
    if not token_info:
        return redirect(url_for("home"))
    sp = get_spotify_client(session["user"]["id"])
    time_range = request.args.get("range", "short_term")

    results = sp.current_user_top_artists(limit=50, time_range=time_range)

    genre_to_artists = defaultdict(list)

    for artist in results["items"]:
        for genre in artist.get("genres", []):
            genre_to_artists[genre].append(artist["name"])

    # Count + attach artist names
    genre_counts = {g: len(a) for g, a in genre_to_artists.items()}
    top_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    genres = [
        {"name": g, "count": c, "artists": genre_to_artists[g]} for g, c in top_genres
    ]

    return render_template("top_genres.html", genres=genres, time_range=time_range)


@app.route("/recently-played")
def recently_played():
    token_info = session.get("token_info")
    if not token_info:
        return redirect(url_for("home"))
    sp = get_spotify_client(session["user"]["id"])

    # --- Get the user's recently played tracks (last 20)
    results = sp.current_user_recently_played(limit=50)

    # --- Extract relevant info
    tracks = []
    for item in results["items"]:
        track = item["track"]
        played_at = item["played_at"]
        tracks.append({
            "name": track["name"],
            "artist": track["artists"][0]["name"],
            "album_image": track["album"]["images"][1]["url"] if track["album"]["images"] else None,
            "played_at": played_at
        })

    return render_template("recently_played.html", tracks=tracks)


@app.route("/logout")
def logout():
    # Delete Spotipy cache file(s)
    cache_files = [".cache", ".cache-spotify", ".cache-" + CLIENT_ID]
    for f in cache_files:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception as e:
                print("Could not delete cache file:", e)
    
    # Clear Flask session
    session.clear()
    
    # Redirect to home (login page)
    return redirect(url_for("home"))


@app.route("/api/create_playlist/<time_range>", methods=["POST"])
def create_playlist(time_range):
    try:
        token_info = session.get("token_info")
        if not token_info:
            return jsonify({"success": False, "error": "Not authenticated"}), 401

        sp = get_spotify_client(session["user"]["id"])

        # --- Get current user ---
        user = sp.current_user()
        user_id = user.get("id")
        if not user_id:
            return jsonify({"success": False, "error": "Unable to get user ID"}), 400

        # --- Get top tracks ---
        results = sp.current_user_top_tracks(limit=50, time_range=time_range)
        items = results.get("items", [])
        if not items:
            return jsonify({"success": False, "error": "No tracks found"}), 400

        track_uris = [t["uri"] for t in items]

        # --- Create playlist ---
        time_labels = {
            "short_term": "Last 4 Weeks",
            "medium_term": "Last 6 Months",
            "long_term": "Last 12 Months"
        }
        playlist_name = f"Top 50 Songs – {time_labels.get(time_range, 'All Time')}"

        playlist = sp.user_playlist_create(
            user=user_id,
            name=playlist_name,
            public=False,
            description=f"Automatically generated playlist of your top 50 songs from {time_labels.get(time_range, 'Spotify stats')}."
        )

        if not playlist or "id" not in playlist:
            return jsonify({"success": False, "error": "Playlist creation failed"}), 400

        # --- Add items ---
        sp.playlist_add_items(playlist["id"], track_uris)

        return jsonify({
            "success": True,
            "playlist_url": playlist["external_urls"]["spotify"],
            "playlist_name": playlist_name
        })

    except spotipy.exceptions.SpotifyException as e:
        print("Spotify API Error:", e)
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        print("General Error:", e)
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=8080, use_reloader=False)