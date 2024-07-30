# Spotify ETL Project

This project is an ETL (Extract, Transform, Load) pipeline that extracts data from the Spotify API, transforms it, and loads it into a PostgreSQL database. The project is containerized using Docker for easy deployment and scaling.

## Project Overview

The Spotify ETL project consists of two main Python scripts:

1. `credentials.py`: Manages the secure storage and retrieval of sensitive information.
2. `spotify-etl.py`: The main ETL script that interacts with the Spotify API and loads data into the database.

The ETL process extracts the following data from Spotify:
- User's playlists
- Tracks in playlists
- User's saved tracks
- Recently played tracks
- Followed artists
- Track Audio Features

Data is transformed into several pandas DataFrames and written to the default database of a PostgreSQL server with ingest date column. Tables are replaced they already exist. This can be changed to append to capture historical data.
## Prerequisites

- Docker
- Spotify Developer Account
- PostgreSQL database (hosted on DigitalOcean in this setup)

## Setup and Installation

1. Clone the repository:
2. Create a `.env` file in the root directory with the following content:
```
  DB_USER=your_db_user
  DB_PASSWORD=your_db_password
  DB_HOST=your_db_host
  DB_PORT=your_db_port
  SPOTIFY_CLIENT_ID=your_spotify_client_id
  SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
  SPOTIFY_ACCESS_TOKEN=your_spotify_access_token
  SPOTIFY_REFRESH_TOKEN=your_spotify_refresh_token

```
3. Build the Docker image:
`docker build -t spotify-etl .`
4. Run the Docker container:
`docker run -dit --env-file .env --name spotify-etl-container spotify-etl`
5. Execute Python file in Docker Terminal/Desktop
`python spotify-etl.py`
## Project Structure
```
spotify-etl-project/
│
├── credentials.py
├── spotify-etl.py
├── Dockerfile
├── requirements.txt
├── .dockerignore
└── README.md
```
This project uses environment variables to manage sensitive information. Never commit your `.env` file or any file containing credentials to version control.

## Features

- Combination of synchronous and asynchronous API calls using async and aiohttp depending on use case
- Rate-limiting features such as asyncio.sleep(), asyncio.Semaphore(), and retries to avoid too many requests error #429
- Creates several tables in the defaultdb of a parametrized PostgreSQL server
- Error handling and logging

## To Do
- Implement a scheduling system for regular data updates
- Expand the range of data extracted from Spotify
- Create a web app to visualize data in a dashboard using html/css/javascript framework

## Contributing

Contributions to this project are welcome. Please fork the repository and submit a pull request with your changes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

John Steer - johnsteer3@gmail.com

Project Link: [https://github.com/johnsteer/spotify-etl-project](https://github.com/johnsteer/spotify-etl-project)



