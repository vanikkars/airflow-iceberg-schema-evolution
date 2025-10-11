# File: ecommerce-db/ingester/Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency spec first (adjust if requirements file lives here)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingestion code
COPY . .

# Copy the data folder from the repo into the image (adjust path if needed)
# Assumes build context is the repo root: docker build -f ecommerce-db/ingester/Dockerfile .
COPY ./data ./data

#ENTRYPOINT ["python", "ingest_data.py"]
#CMD []