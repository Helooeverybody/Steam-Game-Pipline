# Use an official lightweight Python image
FROM python:3.9-slim

WORKDIR /app

# Copy the requirements file and install dependencies (kafka, redis, etc.)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer script
COPY producer.py .

# Define the command to run when the container starts
CMD ["python", "producer.py"]