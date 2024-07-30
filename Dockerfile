# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python scripts into the container
COPY generate_token.py .
COPY credentials.py .
COPY spotify-etl.py .

# Run the script when the container launches
# CMD ["python", "spotify-etl.py"]

# Running with -dit to detatch and run script manually in docker terminal for now

