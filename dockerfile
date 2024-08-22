# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 50222 available to the world outside this container
EXPOSE 50222

# Define environment variable
ENV INFLUXDB_URL=""
ENV INFLUXDB_TOKEN=""
ENV INFLUXDB_ORG=""
ENV INFLUXDB_BUCKET=""

# Run app.py when the container launches
CMD ["python", "./app.py"]
