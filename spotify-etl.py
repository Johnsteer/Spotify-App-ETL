import asyncio
import math
import time
import os
import pandas as pd
import aiohttp
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import create_engine
from datetime import datetime
import logging
from credentials import USER, PASSWORD, DB_HOST, DB_PORT, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
USER = USER
PASSWORD = PASSWORD
DB_HOST = DB_HOST
DB_PORT = DB_PORT
SPOTIPY_CLIENT_ID = CLIENT_ID
SPOTIPY_CLIENT_SECRET = CLIENT_SECRET
SPOTIPY_REDIRECT_URI = 'http://localhost:3000/'
SCOPE = 'user-top-read user-read-private user-read-email playlist-read-private user-library-read user-read-recently-played user-follow-read'
DB_CONNECTION_STRING = f'postgresql://{USER}:{PASSWORD}@{DB_HOST}:{DB_PORT}/defaultdb?sslmode=require'
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

# Asyncio semaphore to cap number of concurrent tasks at 5
# Rate limited for frequent requests

async def rate_limited_request(session, url, headers):
    semaphore = asyncio.Semaphore(5)
    async with semaphore:
        response = await session.get(url, headers=headers)
        # Checks for 'TooManyRequests' error
        if response.status == 429:
            # Get retry from response
            retry_after = int(response.headers.get('Retry-After', 1))
            logger.warning(f"Rate limit hit. Waiting for {retry_after} seconds.")
            # Sleep for 'retry_after' seconds
            await asyncio.sleep(retry_after)
            # Recusively call to retry request
            return await rate_limited_request(session, url, headers)
        # Small delay between requests
        await asyncio.sleep(0.1)
        return await response.json()

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
        r = pd.DataFrame([{
            'id': playlist['id'],
            'href': playlist['href'],
            'name': playlist['name'],
            'owner': playlist['owner']['display_name'],
            'public': playlist['public'],
            'collaborative': playlist['collaborative'],
            'tracks': playlist['tracks']['total']
        } for playlist in playlists_data])
        logger.info("Success fetching playlists")
        return r
    except Exception as e:
        logger.error(f"Error fetching playlists: {str(e)}")
        raise

async def get_playlist_tracks(session, headers, playlist_id):
    try:
        tracks_data = []
        url = f"{BASE_URL}/playlists/{playlist_id}/tracks"
        while url:
            # Rate limited request as potentially several pages per playlist
            data = await rate_limited_request(session, url, headers)
            tracks_data.extend(data['items'])
            url = data.get('next')
        r = [{
            'id': track['track']['id'],
            'name': track['track']['name'],
            'artist': track['track']['artists'][0]['name'],
            'album': track['track']['album']['name'],
            'playlist_id': playlist_id
        } for track in tracks_data if track['track']]
        # logger.info(f"Success fetching tracks for playlist {playlist_id}")
        return r
    except Exception as e:
        logger.warning(f"Error fetching tracks for playlist {playlist_id}: {str(e)}")
        return []

async def get_saved_tracks(session, headers):
    try:
        saved_tracks_data = []
        url = f"{BASE_URL}/me/tracks"
        while url:
            data = await rate_limited_request(session, url, headers)
            saved_tracks_data.extend(data['items'])
            url = data.get('next')
        r = pd.DataFrame([{
            'id': item['track']['id'],
            'name': item['track']['name'],
            'artist': item['track']['artists'][0]['name'],
            'album': item['track']['album']['name'],
            'added_at': item['added_at']
        } for item in saved_tracks_data])
        logger.info("Success fetching saved tracks")
        return r
    except Exception as e:
        logger.error(f"Error fetching saved tracks: {str(e)}")
        raise

async def get_audio_features(session, headers, track_ids):
    try:
        audio_features = []
        # Spotify allows up to 100 tracks per request
        """
        for i in range(0, len(track_ids), 100):  
            batch = track_ids[i:i+100]
            batch = [x for x in batch if x != None]
            url = f"{BASE_URL}/audio-features?ids={','.join(batch)}"
            data = await rate_limited_request(session, url, headers)
            if data is None or 'audio_features' not in data:
                logger.info(f"Audio Features - unexpected response format for batch {i} - {i+99}: {data}")    
            else:
                audio_features.extend(data['audio_features'])
            #logger.info(f"Audio Features - success fetching batch {i} - {i+99}")
        """
        
        if len(track_ids) % 100 == 0:
            for i in range(0, len(track_ids), 100):
                batch = track_ids[i:i+100]
                url = f"{BASE_URL}/audio-features?ids={','.join(batch)}"
                data = await rate_limited_request(session, url, headers)
                audio_features.extend(data['audio_features'])
        else:
            for i in range(0, math.floor(len(track_ids)/100) * 100, 100):
                batch = track_ids[i:i+100]
                url = f"{BASE_URL}/audio-features?ids={','.join(batch)}"
                data = await rate_limited_request(session, url, headers)
                audio_features.extend(data['audio_features'])
            batch = track_ids[math.floor(len(track_ids)/100) * 100:]
            url = f"{BASE_URL}/audio-features?ids={','.join(batch)}"
            data = await rate_limited_request(session, url, headers)
            audio_features.extend(data['audio_features'])
        audio_features = [x for x in audio_features if x != None]
        logger.info(f"Success fetching audio features for {len(audio_features)} tracks")
        r = pd.DataFrame(audio_features)
        return r
    except Exception as e:
        logger.error(f"Error fetching audio features: {str(e)}")
        raise

async def get_recent_tracks(session, headers):
    try:
        url = f"{BASE_URL}/me/player/recently-played"
        data = await fetch(session, url, headers)
        r = pd.DataFrame([{
            'id': item['track']['id'],
            'name': item['track']['name'],
            'artist': item['track']['artists'][0]['name'],
            'album': item['track']['album']['name'],
            'played_at': item['played_at']
        } for item in data['items']])
        logger.info("Success fetching recent tracks")
        return r
    except Exception as e:
        logger.error(f"Error fetching recent tracks: {str(e)}")
        raise

async def get_followed_artists(session, headers):
    try:
        url = f"{BASE_URL}/me/following?type=artist"
        data = await fetch(session, url, headers)
        r = pd.DataFrame([{
            'id': artist['id'],
            'name': artist['name'],
            'genres': ', '.join(artist['genres']),
            'popularity': artist['popularity'],
            'followers': artist['followers']['total']
        } for artist in data['artists']['items']])
        logger.info("Success fetching followed artists")
        return r
    except Exception as e:
        logger.error(f"Error fetching followed artists: {str(e)}")
        raise

def write_to_database(df, table_name, engine):
    try:
        df.astype(str).to_sql(table_name, engine, if_exists='replace', index=False)
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
            # Fetch data that doesn't require iteration
            playlists_df, recent_tracks_df, followed_artists_df = await asyncio.gather(
                get_playlists(session, headers),
                get_recent_tracks(session, headers),
                get_followed_artists(session, headers)
            )
            logger.info("Executed Non-iterated requests.")

            # Get list of playlist id's to request their tracks
            playlist_ids = playlists_df['id'].tolist()
            # asyncio.gather() will execute the unpacked list of the coroutine function called for every playlist id
            tasks = [get_playlist_tracks(session, headers, pid) for pid in playlist_ids]
            playlist_tracks_results = await asyncio.gather(*tasks)
            # Create DataFrame
            playlists_tracks_df = pd.DataFrame([item for sublist in playlist_tracks_results for item in sublist])

            # Fetch saved tracks and audio features
            saved_tracks_df = await get_saved_tracks(session, headers)
            # Combine tracks from playlists with saved tracks to create a unique set to avoid duplicate requests
            # Although technically Spotify mobile shows your liked tracks as a playlist and it has playlist functionality
            # it is not retrivable via the playlist endpoint
            all_track_ids = list(set(playlists_tracks_df['id'].tolist() + saved_tracks_df['id'].tolist()))
            all_track_ids = [x for x in all_track_ids if x != None]
            logger.info(f"Length of all_track_ids: {len(all_track_ids)}")
            audio_features_df = await get_audio_features(session, headers, all_track_ids)

        dfs = [
            playlists_df, 
            playlists_tracks_df, 
            saved_tracks_df, 
            recent_tracks_df, 
            followed_artists_df,
            audio_features_df
            ]
        
        for df in dfs:
            # Add ingest date column
            df["ingest_date"] = datetime.now()

        engine = create_engine(DB_CONNECTION_STRING)
        logger.info("Created SQL engine")

        # Write to database
        for df, table_name in zip(dfs, [
            'playlists',
            'playlists_tracks',
            'saved_tracks',
            'recent_tracks',
            'followed_artists',
            'audio_features'
            ]):
            write_to_database(df, table_name, engine)
        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
    finally:
        end_time = time.time()
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())