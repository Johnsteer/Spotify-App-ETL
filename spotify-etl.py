# ------------------------------------------------------------------------------------------------------------
# ETL script to test working with Spotify API and writing to PostgreSQL db hosted on digitalocean droplet
# TODOS
# Do more research on Spotify API and what could be useful
# Determine best endpoints/fields to use to get available data without duplications
# Rewrite using async
# Add error hadling and console logging
# ------------------------------------------------------------------------------------------------------------

import time
import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import create_engine
from datetime import datetime
from credentials import USER, PASSWORD, CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, REFRESH_TOKEN

USER = USER
PASSWORD = PASSWORD
SPOTIPY_CLIENT_ID = CLIENT_ID
SPOTIPY_CLIENT_SECRET = CLIENT_SECRET
SPOTIPY_REDIRECT_URI = 'http://localhost:3000/'
SCOPE = 'user-top-read user-read-private user-read-email playlist-read-private user-library-read user-read-recently-played user-follow-read'

dfs = []

# Initialize the Spotify client
s = time.time()

sp_oauth = SpotifyOAuth(
    client_id=SPOTIPY_CLIENT_ID,
    client_secret=SPOTIPY_CLIENT_SECRET,
    redirect_uri=SPOTIPY_REDIRECT_URI,
    scope=SCOPE
)

token_info = sp_oauth.refresh_access_token(REFRESH_TOKEN)
access_token = token_info['access_token']

sp = spotipy.Spotify(auth=access_token)

print("connected to Spotify API")
# Get user's playlists
# TODO rewrite using asynciohttp
playlists_data = []
playlists = sp.current_user_playlists()
for playlist in playlists['items']:
    playlists_data.append({
        'id': playlist['id'],
        'href': playlist['href'],
        'name': playlist['name'],
        'owner': playlist['owner']['display_name'],
        'public': playlist['public'],
        'collaborative': playlist['collaborative'],
        'tracks': playlist['tracks']['total']
    })
playlists_df = pd.DataFrame(playlists_data)
print("loaded playlists")

playlist_tracks = []
for playlist in playlists['items']:
    r = sp.playlist(playlist["id"])
    try:
        for track in r["tracks"]["items"]:
            d = track["track"]
            d["playlist_id"] = playlist["id"]
            playlist_tracks.append(d)
    except:
        print(playlist["id"])
pt_df = pd.DataFrame(playlist_tracks)
dfs.append(pt_df)
print("loaded tracks")
# Get user's saved tracks
# TODO rewrite using asynciohttp
saved_tracks_data = []
results = sp.current_user_saved_tracks()
while results:
    for item in results['items']:
        track = item['track']
        saved_tracks_data.append({
            'id': track['id'],
            'name': track['name'],
            'artist': track['artists'][0]['name'],
            'album': track['album']['name'],
            'added_at': item['added_at']
        })
    if results['next']:
        results = sp.next(results)
    else:
        results = None
saved_tracks_df = pd.DataFrame(saved_tracks_data)
dfs.append(saved_tracks_df)
print("loaded saved tracks")
# Get user's recently played tracks
recent_tracks_data = []
results = sp.current_user_recently_played()
for item in results['items']:
    track = item['track']
    recent_tracks_data.append({
        'id': track['id'],
        'name': track['name'],
        'artist': track['artists'][0]['name'],
        'album': track['album']['name'],
        'played_at': item['played_at']
    })
recent_tracks_df = pd.DataFrame(recent_tracks_data)
dfs.append(recent_tracks_df)
print("loaded recent tracks")
# Get user's followed artists
followed_artists_data = []
results = sp.current_user_followed_artists()
for artist in results['artists']['items']:
    followed_artists_data.append({
        'id': artist['id'],
        'name': artist['name'],
        'genres': ', '.join(artist['genres']),
        'popularity': artist['popularity'],
        'followers': artist['followers']['total']
    })
followed_artists_df = pd.DataFrame(followed_artists_data)
dfs.append(followed_artists_df)
print("loaded followed artists")



for df in dfs:
    df["ingest_date"] = datetime.now()


engine = create_engine(F'postgresql://{USER}:{PASSWORD}@steer-postgres-nyc-do-user-17259900-0.e.db.ondigitalocean.com:25060/defaultdb?sslmode=require')
print("created sql engine")

playlists_df.astype(str).to_sql('playlists', engine, if_exists='append', index=False)
print("wrote playlists")
pt_df.astype(str).to_sql('playlists_tracks', engine, if_exists='append', index=False)
print("wrote tracks")
followed_artists_df.astype(str).to_sql('followed_artists', engine, if_exists='append', index=False)
print("wrote followed artists")
recent_tracks_df.astype(str).to_sql('recent_tracks', engine, if_exists='append', index=False)
print("wrote recent tracks")
saved_tracks_df.astype(str).to_sql('saved_tracks', engine, if_exists='append', index=False)
print("saved tracks")
print("success")
#add console logging