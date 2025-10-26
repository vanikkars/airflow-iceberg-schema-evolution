#!/bin/sh
set -e

echo "Waiting for Vault to be ready..."
sleep 5

echo "Initializing Vault with secrets..."

# Enable KV secrets engine v2 at path 'secret'
vault secrets enable -version=2 -path=secret kv || echo "KV secrets engine already enabled"

# Store PostgreSQL connection secrets
echo "Storing PostgreSQL connection secrets..."
vault kv put secret/postgres/ecommerce \
  host=ecommerce-db \
  port=5432 \
  database=ecom \
  user=ecom \
  password=ecom

# Store S3/MinIO connection secrets
echo "Storing S3/MinIO connection secrets..."
vault kv put secret/s3/minio \
  endpoint=http://minio:9000 \
  access_key=admin \
  secret_key=password \
  bucket=lake \
  region=us-east-1

# Store Trino connection secrets
echo "Storing Trino connection secrets..."
vault kv put secret/trino/iceberg \
  host=trino-coordinator \
  port=8080 \
  user=airflow \
  catalog=iceberg

echo "Vault initialization complete!"
echo "You can access Vault UI at http://localhost:8200 with token: dev-root-token"

# Verify secrets were created
echo "Verifying secrets..."
vault kv get secret/postgres/ecommerce
vault kv get secret/s3/minio
vault kv get secret/trino/iceberg

echo "All secrets verified successfully!"
