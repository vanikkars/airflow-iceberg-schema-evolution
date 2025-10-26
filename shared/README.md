# Pipeline Handlers - Shared Library

Common handlers and utilities shared across data pipeline containers (extractor and iceberg-ingestor).

## Overview

This package provides lightweight handler classes for:
- **HashiCorp Vault** - Secure secret management
- **S3/MinIO** - Object storage operations
- **PostgreSQL** - Database queries
- **Trino** - Query execution

## Usage

```python
from handlers import VaultHandler, S3Handler, PostgresHandler, TrinoHandler

# Initialize handlers
vault = VaultHandler(vault_addr='http://vault:8200', vault_token='token')
secrets = vault.get_secret('secret/postgres/ecommerce')

s3 = S3Handler(endpoint_url='http://minio:9000', access_key='admin', secret_key='password')
s3.load_string('data', key='path/to/file.txt', bucket_name='bucket')

pg = PostgresHandler(host='db', port=5432, database='mydb', user='user', password='pass')
rows = pg.get_records('SELECT * FROM table')
pg.close()

trino = TrinoHandler(host='trino', port=8080, user='airflow', catalog='iceberg')
trino.run('INSERT INTO table VALUES (1, 2, 3)')
trino.close()
```

## Installation

This package is installed during Docker image build:

```dockerfile
COPY shared /tmp/shared
RUN pip install /tmp/shared
```

## Structure

```
shared/
├── handlers/
│   ├── __init__.py
│   ├── vault_handler.py
│   ├── s3_handler.py
│   ├── postgres_handler.py
│   └── trino_handler.py
├── setup.py
└── README.md
```

## Benefits

- **DRY Principle**: Single source of truth for common code
- **Maintainability**: Changes propagate to all containers
- **Testability**: Shared handlers can be unit tested once
- **Consistency**: Same behavior across all containers
