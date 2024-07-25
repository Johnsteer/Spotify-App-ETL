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
  SPOTIFY_CLIENT_ID=your_spotify_client_id
  SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
  SPOTIFY_ACCESS_TOKEN=your_spotify_access_token
  SPOTIFY_REFRESH_TOKEN=your_spotify_refresh_token
```
3. Build the Docker image:
`docker build -t spotify-etl .`
4. Run the Docker container:
`docker run --env-file .env --name spotify-etl-container spotify-etl`
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

## Future Improvements

- Implement asynchronous API calls for improved performance
- Add more robust error handling and logging
- Implement a scheduling system for regular data updates
- Expand the range of data extracted from Spotify

## Contributing

Contributions to this project are welcome. Please fork the repository and submit a pull request with your changes.

## License

[Specify your license here]

## Contact

John Steer - johnsteer3@gmail.com

Project Link: [https://github.com/johnsteer/spotify-etl-project](https://github.com/johnsteer/spotify-etl-project)



