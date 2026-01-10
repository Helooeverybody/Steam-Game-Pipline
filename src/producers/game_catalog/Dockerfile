# File: Dockerfile
# This builds the container for the GameCatalogProducer service.

# Use an official lightweight Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY producer.py .

# Define the command to run when the container starts
CMD ["python", "producer.py"]