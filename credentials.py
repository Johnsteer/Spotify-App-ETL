import os

USER = os.environ.get('DB_USER')
PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
ACCESS_TOKEN = os.environ.get('SPOTIFY_ACCESS_TOKEN')
REFRESH_TOKEN = os.environ.get('SPOTIFY_REFRESH_TOKEN')

print(CLIENT_ID)
print(CLIENT_SECRET)
print(ACCESS_TOKEN)
print(REFRESH_TOKEN)
