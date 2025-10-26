FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingestion script
COPY ingest_to_iceberg.py .

# Make script executable
RUN chmod +x ingest_to_iceberg.py

# Set entry point
ENTRYPOINT ["python", "ingest_to_iceberg.py"]
