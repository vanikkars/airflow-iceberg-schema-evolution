# Common Handlers

Separate, dependency-isolated handler packages for data pipeline containers.

## Structure

```
common/
├── vault-handler/       # HashiCorp Vault integration
│   └── Dependencies: hvac
├── s3-handler/          # S3/MinIO object storage
│   └── Dependencies: boto3
├── postgres-handler/    # PostgreSQL database
│   └── Dependencies: psycopg2-binary
└── trino-handler/       # Trino query engine
    └── Dependencies: trino
```

## Design Principles

### **Separation of Concerns**
Each handler is a separate package with isolated dependencies. This prevents:
- Dependency bloat (containers only install what they need)
- Version conflicts between unrelated packages
- Unexpected side effects from transitive dependencies

### **Container-Specific Installation**

**Extractor** (`audit-log-extractor`):
- Installs: `vault-handler`, `s3-handler`, `postgres-handler`
- Uses: Vault for secrets, PostgreSQL for source data, S3 for CSV storage

**Iceberg Ingestor** (`iceberg-ingestor`):
- Installs: `vault-handler`, `s3-handler`, `trino-handler`
- Uses: Vault for secrets, S3 for reading CSVs, Trino for loading to Iceberg

## Usage

### In Dockerfiles

```dockerfile
# Install only needed handlers
COPY common/vault-handler /tmp/vault-handler
COPY common/s3-handler /tmp/s3-handler
COPY common/postgres-handler /tmp/postgres-handler
RUN pip install --no-cache-dir \
    /tmp/vault-handler \
    /tmp/s3-handler \
    /tmp/postgres-handler
```

### In Python Scripts

```python
from vault_handler import VaultHandler
from s3_handler import S3Handler
from postgres_handler import PostgresHandler

# Initialize handlers
vault = VaultHandler('http://vault:8200', 'token')
secrets = vault.get_secret('secret/postgres/db')

pg = PostgresHandler(**secrets)
rows = pg.get_records('SELECT * FROM table')
pg.close()
```

## Benefits

✅ **No Dependency Bloat**: Extractor doesn't install Trino, ingestor doesn't install PostgreSQL
✅ **Smaller Images**: Each container only has what it needs
✅ **Faster Builds**: Fewer dependencies = faster pip install
✅ **Version Isolation**: Handlers can upgrade dependencies independently
✅ **Clear Boundaries**: Easy to see what each container uses
✅ **Reusability**: Any new container can pick and choose handlers

## Dependency Matrix

| Handler | Package | Version | Used By |
|---------|---------|---------|---------|
| VaultHandler | hvac | ≥2.1.0 | extractor, ingestor |
| S3Handler | boto3 | ≥1.34.0 | extractor, ingestor |
| PostgresHandler | psycopg2-binary | ≥2.9.0 | extractor |
| TrinoHandler | trino | ≥0.328.0 | ingestor |

## Adding a New Handler

1. Create package directory:
   ```bash
   mkdir -p common/new-handler/new_handler
   ```

2. Add handler code:
   ```python
   # common/new-handler/new_handler/handler.py
   class NewHandler:
       def __init__(self, ...):
           pass
   ```

3. Create `__init__.py`:
   ```python
   # common/new-handler/new_handler/__init__.py
   from .handler import NewHandler
   __all__ = ['NewHandler']
   ```

4. Create `setup.py`:
   ```python
   setup(
       name='new-handler',
       version='0.1.0',
       packages=find_packages(),
       install_requires=['dependency>=1.0.0'],
   )
   ```

5. Install in Dockerfile:
   ```dockerfile
   COPY common/new-handler /tmp/new-handler
   RUN pip install --no-cache-dir /tmp/new-handler
   ```

## Maintenance

- Each handler can be versioned independently
- Update dependencies in individual `setup.py` files
- Test handlers in isolation before deploying
- Consider extracting to separate repos if they grow large
