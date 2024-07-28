import asyncio
import time
import os
import pandas as pd
import aiohttp
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
BASE_URL = 'https://api.spotify.com/v1'

async def get_spotify_token():
    try:
        sp_oauth = SpotifyOAuth(
            client_id=SPOTIPY_CLIENT_ID,
            client_secret=SPOTIPY_CLIENT_SECRET,
            redirect_uri=SPOTIPY_REDIRECT_URI,
            scope=SCOPE
        )
        token_info = sp_oauth.refresh_access_token(REFRESH_TOKEN)
        logger.info(f"Access token retrieved successfuly.")
        return token_info['access_token']
    except Exception as e:
        logger.error(f"Failed to get Spotify token: {str(e)}")
        raise

async def fetch(session, url, headers):
    async with session.get(url, headers=headers) as response:
        return await response.json()

async def get_playlists(session, headers):
    try:
        playlists_data = []
        url = f"{BASE_URL}/me/playlists"
        while url:
            data = await fetch(session, url, headers)
            playlists_data.extend(data['items'])
            url = data.get('next')
        
        return pd.DataFrame([{
            'id': playlist['id'],
            'href': playlist['href'],
            'name': playlist['name'],
            'owner': playlist['owner']['display_name'],
            'public': playlist['public'],
            'collaborative': playlist['collaborative'],
            'tracks': playlist['tracks']['total']
        } for playlist in playlists_data])
    except Exception as e:
        logger.error(f"Error fetching playlists: {str(e)}")
        raise

async def get_playlist_tracks(session, headers, playlist_id):
    try:
        tracks_data = []
        url = f"{BASE_URL}/playlists/{playlist_id}/tracks"
        while url:
            data = await fetch(session, url, headers)
            tracks_data.extend(data['items'])
            url = data.get('next')
        
        return [{
            'id': track['track']['id'],
            'name': track['track']['name'],
            'artist': track['track']['artists'][0]['name'],
            'album': track['track']['album']['name'],
            'playlist_id': playlist_id
        } for track in tracks_data if track['track']]
    except Exception as e:
        logger.warning(f"Error fetching tracks for playlist {playlist_id}: {str(e)}")
        return []

async def get_saved_tracks(session, headers):
    try:
        saved_tracks_data = []
        url = f"{BASE_URL}/me/tracks"
        while url:
            data = await fetch(session, url, headers)
            saved_tracks_data.extend(data['items'])
            url = data.get('next')
        
        return pd.DataFrame([{
            'id': item['track']['id'],
            'name': item['track']['name'],
            'artist': item['track']['artists'][0]['name'],
            'album': item['track']['album']['name'],
            'added_at': item['added_at']
        } for item in saved_tracks_data])
    except Exception as e:
        logger.error(f"Error fetching saved tracks: {str(e)}")
        raise

async def get_recent_tracks(session, headers):
    try:
        url = f"{BASE_URL}/me/player/recently-played"
        data = await fetch(session, url, headers)
        return pd.DataFrame([{
            'id': item['track']['id'],
            'name': item['track']['name'],
            'artist': item['track']['artists'][0]['name'],
            'album': item['track']['album']['name'],
            'played_at': item['played_at']
        } for item in data['items']])
    except Exception as e:
        logger.error(f"Error fetching recent tracks: {str(e)}")
        raise

async def get_followed_artists(session, headers):
    try:
        url = f"{BASE_URL}/me/following?type=artist"
        data = await fetch(session, url, headers)
        return pd.DataFrame([{
            'id': artist['id'],
            'name': artist['name'],
            'genres': ', '.join(artist['genres']),
            'popularity': artist['popularity'],
            'followers': artist['followers']['total']
        } for artist in data['artists']['items']])
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

async def main():
    start_time = time.time()
    logger.info("Starting Spotify ETL process")

    try:
        # Await get_spotify_token() to ensure token is returned before continuing
        access_token = await get_spotify_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        # Instatiate ClientSession
        async with aiohttp.ClientSession() as session:
            # Awaiting gather() will run all coroutine functions and store results in an iterable to unpack
            playlists_df, saved_tracks_df, recent_tracks_df, followed_artists_df = await asyncio.gather(
                get_playlists(session, headers),
                get_saved_tracks(session, headers),
                get_recent_tracks(session, headers),
                get_followed_artists(session, headers)
            )
            logger.info("Executed 1st gather.")
            # Get list of playlist id's to request their tracks
            playlist_ids = playlists_df['id'].tolist()
            # Calling get_playlists_tracks() here doesn't actually execute it but returns coroutines to be gathered
            playlist_tracks_tasks = [get_playlist_tracks(session, headers, playlist_id) for playlist_id in playlist_ids]
            # Await gathered coroutines
            playlist_tracks_results = await asyncio.gather(*playlist_tracks_tasks)
            logger.info("Executed 2nd gather.")
            # Iterate through results and create DataFrame
            pt_df = pd.DataFrame([item for sublist in playlist_tracks_results for item in sublist])

        dfs = [playlists_df, pt_df, saved_tracks_df, recent_tracks_df, followed_artists_df]
        for df in dfs:
            #Add ingest date column
            df["ingest_date"] = datetime.now()

        engine = create_engine(DB_CONNECTION_STRING)
        logger.info("Created SQL engine")

        # Write to database
        for df, table_name in zip(dfs, ['playlists','playlists_tracks', 'saved_tracks', 'recent_tracks', 'followed_artists']):
            write_to_database(df, table_name, engine)
        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
    finally:
        end_time = time.time()
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())