FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install shared handlers library
COPY shared /tmp/shared
RUN pip install --no-cache-dir /tmp/shared && rm -rf /tmp/shared

# Copy requirements and install Python dependencies
COPY iceberg-ingestor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingestion script
COPY iceberg-ingestor/ingest_to_iceberg.py .

# Make script executable
RUN chmod +x ingest_to_iceberg.py

# Set entry point
ENTRYPOINT ["python", "ingest_to_iceberg.py"]
