FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install common handler packages (only what's needed for ingestor)
COPY common/vault-handler /tmp/vault-handler
COPY common/s3-handler /tmp/s3-handler
COPY common/trino-handler /tmp/trino-handler
RUN pip install --no-cache-dir /tmp/vault-handler /tmp/s3-handler /tmp/trino-handler && \
    rm -rf /tmp/vault-handler /tmp/s3-handler /tmp/trino-handler

# Copy requirements and install Python dependencies
COPY iceberg-ingestor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingestion script
COPY iceberg-ingestor/ingest_to_iceberg.py .

# Make script executable
RUN chmod +x ingest_to_iceberg.py

# Set entry point
ENTRYPOINT ["python", "ingest_to_iceberg.py"]
