FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install common handler packages (only what's needed for extractor)
COPY common/vault-handler /tmp/vault-handler
COPY common/s3-handler /tmp/s3-handler
COPY common/postgres-handler /tmp/postgres-handler
RUN pip install --no-cache-dir /tmp/vault-handler /tmp/s3-handler /tmp/postgres-handler && \
    rm -rf /tmp/vault-handler /tmp/s3-handler /tmp/postgres-handler

# Copy requirements and install Python dependencies
COPY extractor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy extraction script
COPY extractor/extract_audit_logs.py .

# Make script executable
RUN chmod +x extract_audit_logs.py

# Set entry point
ENTRYPOINT ["python", "extract_audit_logs.py"]
