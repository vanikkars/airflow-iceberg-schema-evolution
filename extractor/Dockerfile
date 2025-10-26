FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install shared handlers library
COPY shared /tmp/shared
RUN pip install --no-cache-dir /tmp/shared && rm -rf /tmp/shared

# Copy requirements and install Python dependencies
COPY extractor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy extraction script
COPY extractor/extract_audit_logs.py .

# Make script executable
RUN chmod +x extract_audit_logs.py

# Set entry point
ENTRYPOINT ["python", "extract_audit_logs.py"]
