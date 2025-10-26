# HashiCorp Vault Integration

This directory contains the Vault configuration for securely managing secrets and connection information.

## Overview

HashiCorp Vault is used to store sensitive credentials and configuration instead of hardcoding them in environment variables or configuration files. This provides better security and centralized secret management.

## Architecture

- **Vault Server**: Runs in development mode on port 8200
- **Vault Init**: Initialization container that populates Vault with secrets on startup
- **Extractor**: Reads secrets from Vault when `VAULT_ENABLED=true`

## Secrets Stored in Vault

The following secrets are stored in Vault's KV v2 secrets engine:

### PostgreSQL Connection (`secret/postgres/ecommerce`)
- `host`: PostgreSQL hostname
- `port`: PostgreSQL port
- `database`: Database name
- `user`: Database user
- `password`: Database password

### S3/MinIO Connection (`secret/s3/minio`)
- `endpoint`: S3/MinIO endpoint URL
- `access_key`: AWS access key / MinIO admin user
- `secret_key`: AWS secret key / MinIO admin password
- `bucket`: S3 bucket name
- `region`: AWS region

### Trino Connection (`secret/trino/iceberg`)
- `host`: Trino coordinator hostname
- `port`: Trino port
- `user`: Trino user
- `catalog`: Trino catalog name


## Usage

### Starting Vault

Vault is automatically started when you run:
```bash
make up
```

The Vault service will:
1. Start in development mode with a root token: `dev-root-token`
2. Wait for the service to be healthy
3. Run the initialization script to populate secrets

### Accessing Vault UI

- **URL**: http://localhost:8200
- **Token**: `dev-root-token`

### Using Vault in the Extractor

The extractor script automatically uses Vault when `VAULT_ENABLED=true` is set:

```python
# Environment variable
VAULT_ENABLED=true
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=dev-root-token
```

When enabled, the extractor will:
1. Connect to Vault using the provided address and token
2. Read secrets from the configured paths
3. Use these secrets instead of environment variables

### Disabling Vault

To disable Vault and use environment variables instead:

```python
VAULT_ENABLED=false
```

The extractor will fall back to reading from environment variables.

## Development vs Production

**⚠️ Important**: This Vault setup is for DEVELOPMENT ONLY.

In production, you should:
- Use Vault in non-dev mode with proper authentication
- Store the root token securely
- Use AppRole or Kubernetes auth instead of token auth
- Enable TLS/SSL for Vault communication
- Use proper secret rotation policies
- Set up audit logging

## Adding New Secrets

To add new secrets to Vault:

1. Update `init-vault.sh` with the new secret path:
```bash
vault kv put secret/your/secret/path \
  key1=value1 \
  key2=value2
```

2. Update the extractor code to read the new secret:
```python
vault = VaultHandler(VAULT_ADDR, VAULT_TOKEN)
secret = vault.get_secret('secret/your/secret/path')
value = secret['key1']
```

3. Rebuild and restart services:
```bash
make down
make up
```

## Troubleshooting

### Vault is not starting
- Check Docker logs: `docker logs vault`
- Ensure port 8200 is not in use

### Secrets not found
- Verify Vault initialization completed: `docker logs vault-init`
- Access Vault UI and check if secrets exist
- Verify secret paths match the code

### Authentication failed
- Ensure `VAULT_TOKEN` matches the root token (`dev-root-token`)
- Check Vault address is correct (`http://vault:8200`)

### Extractor can't connect to Vault
- Ensure Docker containers are on the same network
- Verify Vault is healthy: `docker ps | grep vault`
- Check network connectivity: `docker exec extractor-container ping vault`
