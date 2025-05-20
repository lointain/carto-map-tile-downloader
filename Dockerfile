# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the enhanced_tile_downloader.py script into the container at /app
COPY enhanced_tile_downloader.py .

# Expose any ports if your application were a server (not strictly necessary for this downloader)
# EXPOSE 8080

# Command to run the application when the container starts
# This sets the default command. You will typically override this with specific arguments
# when you run the container (e.g., docker run ... python enhanced_tile_downloader.py --min_zoom 0 ...)
CMD ["python", "enhanced_tile_downloader.py", "--help"]
