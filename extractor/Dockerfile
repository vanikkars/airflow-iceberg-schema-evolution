FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy extraction script
COPY extract_audit_logs.py .

# Make script executable
RUN chmod +x extract_audit_logs.py

# Set entry point
ENTRYPOINT ["python", "extract_audit_logs.py"]
