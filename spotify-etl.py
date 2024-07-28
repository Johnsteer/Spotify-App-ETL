import time
import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import create_engine
from datetime import datetime
import logging
from credentials import USER, PASSWORD, CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, REFRESH_TOKEN

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
USER = USER
PASSWORD = PASSWORD
SPOTIPY_CLIENT_ID = CLIENT_ID
SPOTIPY_CLIENT_SECRET = CLIENT_SECRET
SPOTIPY_REDIRECT_URI = 'http://localhost:3000/'
SCOPE = 'user-top-read user-read-private user-read-email playlist-read-private user-library-read user-read-recently-played user-follow-read'
DB_CONNECTION_STRING = f'postgresql://{USER}:{PASSWORD}@steer-postgres-nyc-do-user-17259900-0.e.db.ondigitalocean.com:25060/defaultdb?sslmode=require'

def initialize_spotify_client():
    try:
        sp_oauth = SpotifyOAuth(
            client_id=SPOTIPY_CLIENT_ID,
            client_secret=SPOTIPY_CLIENT_SECRET,
            redirect_uri=SPOTIPY_REDIRECT_URI,
            scope=SCOPE
        )
        token_info = sp_oauth.refresh_access_token(REFRESH_TOKEN)
        access_token = token_info['access_token']
        return spotipy.Spotify(auth=access_token)
    except Exception as e:
        logger.error(f"Failed to initialize Spotify client: {str(e)}")
        raise

def get_playlists(sp):
    try:
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
        return pd.DataFrame(playlists_data)
    except Exception as e:
        logger.error(f"Error fetching playlists: {str(e)}")
        raise

def get_playlist_tracks(sp, playlists):
    playlist_tracks = []
    for playlist in playlists['items']:
        try:
            r = sp.playlist(playlist["id"])
            for track in r["tracks"]["items"]:
                d = track["track"]
                d["playlist_id"] = playlist["id"]
                playlist_tracks.append(d)
        except Exception as e:
            logger.warning(f"Error fetching tracks for playlist {playlist['id']}: {str(e)}")
    return pd.DataFrame(playlist_tracks)

def get_saved_tracks(sp):
    try:
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
        return pd.DataFrame(saved_tracks_data)
    except Exception as e:
        logger.error(f"Error fetching saved tracks: {str(e)}")
        raise

def get_recent_tracks(sp):
    try:
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
        return pd.DataFrame(recent_tracks_data)
    except Exception as e:
        logger.error(f"Error fetching recent tracks: {str(e)}")
        raise

def get_followed_artists(sp):
    try:
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
        return pd.DataFrame(followed_artists_data)
    except Exception as e:
        logger.error(f"Error fetching followed artists: {str(e)}")
        raise

def write_to_database(df, table_name, engine):
    try:
        df.astype(str).to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Successfully wrote data to {table_name}")
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {str(e)}")
        raise

def main():
    start_time = time.time()
    logger.info("Starting Spotify ETL process")

    try:
        sp = initialize_spotify_client()
        logger.info("Connected to Spotify API")

        playlists_df = get_playlists(sp)
        pt_df = get_playlist_tracks(sp, sp.current_user_playlists())
        saved_tracks_df = get_saved_tracks(sp)
        recent_tracks_df = get_recent_tracks(sp)
        followed_artists_df = get_followed_artists(sp)

        dfs = [pt_df, saved_tracks_df, recent_tracks_df, followed_artists_df]
        for df in dfs:
            df["ingest_date"] = datetime.now()

        engine = create_engine(DB_CONNECTION_STRING)
        logger.info("Created SQL engine")

        write_to_database(playlists_df, 'playlists', engine)
        write_to_database(pt_df, 'playlists_tracks', engine)
        write_to_database(followed_artists_df, 'followed_artists', engine)
        write_to_database(recent_tracks_df, 'recent_tracks', engine)
        write_to_database(saved_tracks_df, 'saved_tracks', engine)

        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
    finally:
        end_time = time.time()
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()