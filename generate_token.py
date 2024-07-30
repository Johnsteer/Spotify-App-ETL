import spotipy
from spotipy.oauth2 import SpotifyOAuth
from credentials import CLIENT_ID, CLIENT_SECRET

SPOTIPY_REDIRECT_URI = 'http://localhost:3000/'
SCOPE = 'user-top-read user-read-private user-read-email playlist-read-private user-library-read user-read-recently-played user-follow-read'

sp_oauth = SpotifyOAuth(client_id=CLIENT_ID,
                        client_secret=CLIENT_SECRET,
                        redirect_uri=SPOTIPY_REDIRECT_URI,
                        scope=SCOPE)

token_info = sp_oauth.get_access_token()

print(f"Refresh Token: {token_info['refresh_token']}")