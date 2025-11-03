# CDC Pipeline with Iceberg & dbt

Modern data lakehouse implementing Change Data Capture (CDC) from PostgreSQL audit logs to Apache Iceberg using Airflow, dbt, and Trino. Features automatic schema evolution, secure secrets management with Vault, and incremental processing.

## Overview

**Problem**: Keep analytical data warehouse synchronized with operational database changes
**Solution**: Capture INSERT/UPDATE/DELETE operations from audit logs and materialize them incrementally

**Data Flow**: PostgreSQL → Docker Extractor → S3/MinIO → Trino SQL Ingestion → Iceberg (Landing → Staging → Marts) → Analytics

## Key Features

- **Secure by Default**: HashiCorp Vault for credential management
- **Incremental Processing**: Only new changes since last run
- **Schema Evolution**: Automatic column propagation via Iceberg + dbt contracts
- **Dual-Catalog Trino**: Hive for CSV external tables, Iceberg for warehouse
- **CDC Handling**: Custom dbt materialization with INSERT/UPDATE/DELETE logic
- **Type Safety**: Enforced data contracts with validation
- **SCD Type 2**: Full history tracking in `orders_history_scd2`

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Airflow 3.0 | Workflow scheduling |
| **Secrets** | HashiCorp Vault | Credential storage |
| **Warehouse** | Apache Iceberg | ACID lakehouse format |
| **Query Engine** | Trino | Distributed SQL (Iceberg + Hive catalogs) |
| **Transformation** | dbt | Data modeling |
| **Source DB** | PostgreSQL | Operational database |
| **Storage** | MinIO (S3) | Object storage |
| **Extraction** | Python + Docker | Audit log extractor |
| **Ingestion** | Trino SQL | Hive → Iceberg ingestion |

## Architecture

```
PostgreSQL (audit_log_dml)
    ↓
Docker Extractor → S3/MinIO (CSV)
    ↓
Trino: Hive temp tables → Iceberg landing
    ↓
dbt: landing → staging (flatten/dedupe) → marts (MERGE)
    ↓
Analytics/BI Tools
```

**Vault Integration**: All credentials stored in Vault, accessed by extractor and Airflow at runtime.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Make (optional)
- Python 3.10+ (optional, for local development)

### Setup

```bash
# 1. Clone and start services
git clone git@github.com:vanikkars/airflow-iceberg-schema-evolution.git
cd airflow-iceberg-schema-evolution
make up  # Starts Vault, PostgreSQL, MinIO, Trino, Airflow

# 2. Build containers
make extractor-build
make data-generator-build

# 3. Initialize Trino (creates landing, staging, marts schemas)
make trino-init

# 4. Generate and load sample data
make generate-data
make orders-build-insert

# 5. Run Airflow DAG
# Open http://localhost:8080 (airflow/airflow)
# Trigger DAG: extract_audit_logs_ecomm

# 6. Query results
docker exec -it trino-coordinator trino --catalog iceberg --schema marts
# SELECT * FROM orders;
```

### Access UIs
| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| Trino | http://localhost:8081 | - |
| Vault | http://localhost:8200 | dev-root-token |
| MinIO | http://localhost:9001 | admin / password |

## Project Structure

```
airflow/
├── dags/
│   ├── extract_audit_logs_ecomm.py           # Main DAG
│   └── dbt_dwh/                              # dbt project
│       ├── models/staging/ecomm/             # Flatten & dedupe
│       ├── models/marts/ecomm/               # orders, orders_history_scd2
│       └── macros/                           # Custom materializations
├── plugins/dbt_operator/                     # Custom dbt operator
ecommerce-db/data-generator/                  # Synthetic data generator
extractor/                                    # Docker: audit-log-extractor
trino/trino_config/catalog/                   # iceberg.properties, hive.properties
vault/                                        # Vault initialization
```

## Secrets Management (Vault)

All credentials stored in Vault, accessed at runtime by services:

| Secret Path | Contains |
|-------------|----------|
| `secret/postgres/ecommerce` | PostgreSQL connection |
| `secret/s3/minio` | S3/MinIO credentials |
| `secret/trino/iceberg` | Trino connection |

**How it works**: Vault starts → init container populates secrets → extractor/Airflow read at runtime

See `vault/README.md` for details.

## Trino Dual-Catalog Setup

**Iceberg Catalog**: Main warehouse (landing/staging/marts schemas) with ACID, schema evolution, time travel
**Hive Catalog**: Temporary CSV external tables for ingestion

**Why Both?**: Iceberg doesn't support CSV external tables. Hive reads CSV from S3 → Trino performs cross-catalog INSERT into Iceberg.

## Data Flow Details

### PostgreSQL Source
```sql
CREATE TABLE audit_log_dml (
    audit_event_id  SERIAL PRIMARY KEY,
    audit_operation TEXT,              -- 'I', 'U', 'D'
    audit_timestamp TIMESTAMP,
    tbl_schema      TEXT,
    tbl_name        TEXT,
    raw_data        TEXT               -- JSON payload
);
```

### 1. Extraction (Docker)
- Reads Vault secrets (PostgreSQL + S3)
- Extracts by time interval (`data_interval_start` → `data_interval_end`)
- Outputs CSV to S3: `raw/ecommerce/audit_log_dml/YYYY/MM/DD/`
- Returns S3 keys via XCom

### 2. Ingestion (Trino SQL)
- Dynamic task mapping: one SQL task per S3 file (parallel)
- Creates temp Hive external table → CSV on S3
- Cross-catalog INSERT: `hive.default.temp_*` → `iceberg.landing.ecomm_audit_log_dml`
- Type casting: VARCHAR → BIGINT, TIMESTAMP
- Cleanup: DROP temp table

### 3. dbt Transformations

**Staging**:
- `stg_ecomm_audit_log_dml`: Incremental, dedupe by `audit_event_id`
- `stg_ecomm_audit_log_dml_orders_flattened`: JSON parsing, type casting

**Marts**:
- `orders`: Custom MERGE materialization (INSERT/UPDATE/DELETE)
- `orders_history_scd2`: Full history with `valid_from`, `valid_to`, `is_current`

**Data Contracts**: Enforced on all models with type validation, NOT NULL, UNIQUE constraints

## Common Commands

### Environment

```bash
make up                        # Start all services
make down                      # Stop services
make extractor-build           # Build audit extractor
make data-generator-build      # Build data generator
make trino-init                # Initialize Trino schemas
```

### Data Operations

```bash
make generate-data             # Generate synthetic orders
make orders-build-insert       # Load into PostgreSQL
make truncate-audit-logs       # Clear PostgreSQL
make truncate-trino            # Clear Iceberg tables
```

### dbt

```bash
cd airflow/dags/dbt_dwh
dbt run                        # Run all models
dbt run --select staging       # Staging only
dbt run --select marts         # Marts only
dbt test                       # Run tests
dbt compile --select orders    # Validate contracts
```

### Debugging

```bash
# Check Trino tables
docker exec trino-coordinator trino --catalog iceberg --execute "SHOW TABLES FROM marts;"

# Query data
docker exec trino-coordinator trino --catalog iceberg --schema marts --execute "SELECT COUNT(*) FROM orders;"

# Vault secrets
docker exec vault vault kv get secret/postgres/ecommerce

# Logs
docker logs airflow-scheduler
docker logs trino-coordinator
docker logs vault-init
```

## Data Contracts

dbt contracts enforce schema validation on all staging/mart models:

**Benefits**: Type safety, breaking change prevention, quality checks (NOT NULL, UNIQUE, accepted values)

**Covered Models**:
- `stg_ecomm_audit_log_dml`: 8 columns, UNIQUE on `audit_event_id`
- `stg_ecomm_audit_log_dml_orders_flattened`: 13 columns, UNIQUE on `audit_event_id`
- `orders`: 13 columns, UNIQUE + NOT NULL on `order_id`
- `orders_history_scd2`: 16 columns with temporal constraints

**Modifying**: Update model SQL → Update `schema.yml` → `dbt compile` → Deploy

## Example Queries

```sql
-- Orders with multiple updates
SELECT order_id, COUNT(*) as change_count
FROM staging.stg_ecomm_audit_log_dml_orders_flattened
GROUP BY order_id HAVING COUNT(*) > 1;

-- Deleted orders (in staging, not in marts)
SELECT order_id, audit_operation, audit_timestamp
FROM staging.stg_ecomm_audit_log_dml_orders_flattened
WHERE audit_operation = 'D'
  AND order_id NOT IN (SELECT order_id FROM marts.orders);

-- Freshness check
SELECT MAX(ingested_at) as latest_ingestion,
       COUNT(*) as total_orders,
       COUNT(DISTINCT order_id) as unique_orders
FROM marts.orders;
```

## Testing CDC

```bash
# Load initial data
make orders-build-insert && cd airflow/dags/dbt_dwh && dbt run

# Check count
docker exec trino-coordinator trino --catalog iceberg --schema marts --execute "SELECT COUNT(*) FROM orders;"

# Generate batch 2, load, re-run dbt, verify count changed
make generate-data
make orders-build-insert
# Trigger DAG in Airflow UI
```

## Troubleshooting

**Vault**: `docker logs vault-init` | `docker restart vault-init`

**Extractor**: `docker logs <container-id>` | `make extractor-build`

**Trino**: `docker logs trino-coordinator` | `docker restart trino-coordinator` | Verify catalogs: `SHOW CATALOGS`

**Airflow**: `docker logs airflow-scheduler` | Check DAGs: `docker exec -it airflow-scheduler airflow dags list`

**dbt**: `dbt clean && dbt compile` | Full refresh: `dbt run --select orders --full-refresh`

**Data Contracts**: Update `schema.yml` to match actual model output

## 📚 References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Trino Documentation](https://trino.io/docs/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [HashiCorp Vault Documentation](https://www.vaultproject.io/docs)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

## 📝 License

This project is open source and available under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

**Built with ❤️ as a demonstration of modern data engineering patterns**
