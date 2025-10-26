# Change Data Capture (CDC) Pipeline with Iceberg & dbt

A production-ready data pipeline that implements Change Data Capture (CDC) pattern using Apache Iceberg, dbt, Trino, and HashiCorp Vault. This project demonstrates how to build a secure, containerized modern data lakehouse that captures database changes (inserts, updates, deletes) from PostgreSQL audit logs, stores them in S3-compatible object storage, and materializes them into dimensional tables with automatic schema evolution.

## 🎯 Purpose

This project solves the challenge of maintaining synchronized replicas of operational databases in analytical data warehouses. Instead of periodic full dumps, it:

- **Captures incremental changes** from PostgreSQL audit logs
- **Processes CDC events** (INSERT, UPDATE, DELETE operations)
- **Maintains current state tables** in the mart layer
- **Handles schema evolution** automatically when source columns change
- **Uses Iceberg format** for ACID transactions and time-travel capabilities

## 🏗️ Architecture

```
┌──────────────────┐
│  HashiCorp Vault │  Secure secrets management
│  (Credentials)   │  - PostgreSQL credentials
└────────┬─────────┘  - S3/MinIO credentials
         │            - Trino credentials
         ↓
┌─────────────────┐
│   PostgreSQL    │  Source database with audit_log_dml table
│  (audit logs)   │  Captures I/U/D operations as JSON
└────────┬────────┘
         │
         │ Docker: audit-log-extractor (reads from Vault)
         │ Extracts audit logs to CSV on S3/MinIO
         ↓
┌─────────────────┐
│   S3/MinIO      │  Object storage for extracted CSV files
│  (Landing Blobs)│  Partitioned by date: raw/ecommerce/audit_log_dml/YYYY/MM/DD/
└────────┬────────┘
         │
         │ Docker: iceberg-ingestor (reads from Vault)
         │ Loads CSV files into Iceberg via Trino
         ↓
┌─────────────────┐
│  Iceberg Layer  │
│   (Landing)     │  Raw audit events stored in Iceberg format
└────────┬────────┘  Table: landing.ecomm_audit_log_dml
         │
         │ dbt Transformation
         ↓
┌─────────────────┐
│  Iceberg Layer  │
│   (Staging)     │  Flattened JSON + deduplication
└────────┬────────┘  Tables: staging.stg_ecomm_audit_log_dml*
         │
         │ dbt Incremental MERGE (UPDATE/INSERT/DELETE)
         ↓
┌─────────────────┐
│  Iceberg Layer  │
│    (Marts)      │  Current state tables (latest version of each record)
└─────────────────┘  Tables: marts.orders, marts.orders_history_scd2
         │
         │ Queried via Trino
         ↓
    Analytics/BI Tools
```

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 3.0 | Workflow scheduling and monitoring |
| **Secrets Management** | HashiCorp Vault | Secure credential storage and access |
| **Data Warehouse** | Apache Iceberg | ACID-compliant data lakehouse format |
| **Query Engine** | Trino | Distributed SQL query engine |
| **Transformation** | dbt | Data modeling and transformation |
| **Source Database** | PostgreSQL | Operational database with audit logs |
| **Object Storage** | MinIO (S3-compatible) | Raw extracted data storage |
| **Extraction** | Python + Docker | Containerized audit log extraction |
| **Ingestion** | Python + Docker | Containerized Iceberg data loading |
| **Data Generator** | Python + Faker | Synthetic CDC event generation |
| **Container Runtime** | Docker Compose | Local development environment |

## ✨ Key Features

- ✅ **Secure Secrets Management**: HashiCorp Vault for centralized credential storage
- ✅ **Dockerized Extraction & Ingestion**: Containerized, scalable data processing
- ✅ **Incremental CDC Processing**: Only processes new changes since last run
- ✅ **Soft Delete Handling**: Records marked with 'D' operation are removed from marts
- ✅ **Automatic Schema Evolution**: New columns in source automatically appear in target
- ✅ **Idempotent Processing**: Re-running the same data produces the same result
- ✅ **Type Safety**: Proper casting from JSON strings to typed columns
- ✅ **Deduplication**: Keeps only the latest version of each record based on audit_timestamp
- ✅ **Custom dbt Materialization**: Implements MERGE with DELETE logic for CDC
- ✅ **Data Contracts**: Enforced schema contracts on staging and mart layers prevent breaking changes
- ✅ **SCD Type 2**: Full historical tracking with temporal validity in `orders_history_scd2` table

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Make (optional, for convenience commands)
- Python 3.10+ (for local development)

### Setup

1. **Clone and start services**
   ```bash
   git clone git@github.com:vanikkars/airflow-iceberg-schema-evolution.git
   cd airflow-iceberg-schema-evolution

   # Start all services (Airflow, Trino, PostgreSQL, MinIO, Vault)
   make up
   ```

   This will start:
   - HashiCorp Vault (with automatic secret initialization)
   - PostgreSQL (ecommerce source database)
   - MinIO (S3-compatible object storage)
   - Trino (query engine with Iceberg catalog)
   - Apache Airflow (orchestration)

2. **Build Docker containers**
   ```bash
   # Build extraction and ingestion containers
   make build-all-containers

   # Or build individually:
   # make extractor-build          # Audit log extractor
   # make iceberg-ingestor-build   # Iceberg ingestor
   # make data-generator-build     # Data generator
   ```

3. **Initialize Trino schemas**
   ```bash
   make trino-init
   ```

4. **Generate and load sample data**
   ```bash
   # Generate 100 orders with inserts, updates, and deletes
   make generate-data

   # Build ingestor and load data into PostgreSQL
   make orders-build-insert
   ```

5. **Run the Airflow DAG**
   - Open Airflow UI at http://localhost:8080
   - Find the DAG `audit_log_extract_with_data_intervals_dag`
   - Trigger the DAG manually or wait for scheduled run
   - The DAG will:
     1. Extract audit logs from PostgreSQL to S3 (CSV files)
     2. Load CSV files into Iceberg landing layer
     3. Run dbt transformations (staging → marts)

6. **Query the results**
   ```bash
   docker exec -it trino-coordinator trino --catalog iceberg --schema marts

   # In Trino shell:
   SELECT * FROM orders;
   ```

### Access Web UIs

- **Airflow UI**: http://localhost:8080 (username: `airflow`, password: `airflow`)
- **Trino UI**: http://localhost:8081
- **Vault UI**: http://localhost:8200 (token: `dev-root-token`)
- **MinIO Console**: http://localhost:9001 (username: `admin`, password: `password`)

## 📁 Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── dbt_dwh/                           # dbt project
│   │   │   ├── models/
│   │   │   │   ├── staging/                   # Flattening & deduplication
│   │   │   │   │   └── ecomm/
│   │   │   │   │       ├── stg_ecomm_audit_log_dml.sql
│   │   │   │   │       └── stg_ecomm_audit_log_dml_orders_flattened.sql
│   │   │   │   ├── marts/                     # Business logic & current state
│   │   │   │   │   └── ecomm/
│   │   │   │   │       ├── orders.sql         # Current state table
│   │   │   │   │       └── orders_history_scd2.sql  # SCD Type 2
│   │   │   │   └── sources.yml                # Source definitions
│   │   │   └── macros/                        # Custom materializations
│   │   │       └── trino_incremental_always_merge.sql
│   │   └── extract_audit_logs_with_time_interval.py  # Main extraction DAG
│   └── plugins/dbt_operator/                  # Custom Airflow dbt operator
├── ecommerce-db/
│   └── data-generator/
│       ├── generate_data.py                   # Synthetic data generator
│       ├── ingest_data.py                     # PostgreSQL loader
│       └── data/                              # Generated CSV files
├── extractor/                                 # Docker container: audit-log-extractor
│   ├── extract_audit_logs.py                  # Extraction script
│   ├── requirements.txt                       # Python dependencies
│   └── Dockerfile                             # Container definition
├── iceberg-ingestor/                          # Docker container: iceberg-ingestor
│   ├── ingest_to_iceberg.py                   # Ingestion script
│   ├── requirements.txt                       # Python dependencies
│   └── Dockerfile                             # Container definition
├── vault/                                     # HashiCorp Vault configuration
│   ├── init-vault.sh                          # Secret initialization script
│   └── README.md                              # Vault documentation
├── docker-compose.yaml                        # Full stack definition
├── makefile                                   # Convenience commands
├── CLAUDE.md                                  # AI assistant context
└── README.md                                  # This file
```

## 🔐 Secrets Management with Vault

This project uses **HashiCorp Vault** for secure credential management. All database and S3 credentials are stored in Vault instead of being hardcoded.

### Vault Configuration

Secrets are automatically initialized when the stack starts (`vault/init-vault.sh`):

| Secret Path | Contains |
|-------------|----------|
| `secret/postgres/ecommerce` | PostgreSQL connection details (host, port, database, user, password) |
| `secret/s3/minio` | MinIO/S3 connection details (endpoint, access_key, secret_key, bucket) |
| `secret/trino/iceberg` | Trino connection details (host, port, user, catalog) |

### How It Works

1. **Vault starts in dev mode** with root token `dev-root-token`
2. **Init container runs** and populates secrets
3. **Docker containers read secrets** at runtime:
   - `audit-log-extractor` reads PostgreSQL and S3 credentials
   - `iceberg-ingestor` reads S3 and Trino credentials
4. **No hardcoded credentials** in code or configuration

See `vault/README.md` for detailed Vault documentation.

## 🔄 Data Flow Details

### 1. Source Data (PostgreSQL)

```sql
-- audit_log_dml table structure
CREATE TABLE audit_log_dml (
    audit_event_id  SERIAL PRIMARY KEY,
    audit_operation TEXT,              -- 'I', 'U', or 'D'
    audit_timestamp TIMESTAMP,
    tbl_schema      TEXT,
    tbl_name        TEXT,
    raw_data        TEXT               -- JSON payload
);
```

### 2. Extraction (Docker: audit-log-extractor)

Extracts audit logs from PostgreSQL to S3:
- **Reads credentials from Vault** (PostgreSQL + S3)
- Extracts data based on time intervals (`data_interval_start` to `data_interval_end`)
- Outputs CSV files partitioned by date: `raw/ecommerce/audit_log_dml/YYYY/MM/DD/`
- Returns list of S3 keys via XCom for downstream tasks

### 3. Ingestion (Docker: iceberg-ingestor)

Loads CSV files from S3 into Iceberg:
- **Reads credentials from Vault** (S3 + Trino)
- Dynamically mapped tasks (one per S3 file)
- Reads CSV, inserts into Iceberg landing table via Trino
- Batch insertion for performance (configurable batch size)
- Adds metadata columns: `ingested_at`, `source_file`

### 5. Staging Layer (dbt)

**stg_ecomm_audit_log_dml.sql**
- Incremental model based on `ingested_at`
- Deduplicates by `audit_event_id`
- Preserves JSON raw_data
- **Data contract enforced**: Schema validation with NOT NULL, UNIQUE constraints

**stg_ecomm_audit_log_dml_orders_flattened.sql**
- Parses JSON from `raw_data` column
- Extracts individual fields (order_id, order_timestamp, etc.)
- Type casting (VARCHAR → INTEGER, TIMESTAMP, DOUBLE)
- **Data contract enforced**: 13 columns with type safety and validation rules

### 6. Marts Layer (dbt)

**orders.sql**
- Incremental model with custom `trino_incremental_always_merge` materialization
- Keeps only the latest version of each order (by `audit_timestamp`)
- **MERGE logic**:
  - INSERT: New orders not in target
  - UPDATE: Existing orders with newer data
  - DELETE: Orders with `audit_operation = 'D'`
- **Data contract enforced**: 13 columns with UNIQUE/NOT NULL on `order_id`

**orders_history_scd2.sql**
- Slowly Changing Dimension Type 2 table
- Full history of all order changes with temporal validity
- Columns: `valid_from`, `valid_to`, `is_current`, `is_deleted`
- **Data contract enforced**: 16 columns including temporal and flag columns

## 🔧 Common Commands

### Docker Container Management

```bash
# Build all containers
make build-all-containers

# Build individual containers
make extractor-build           # Audit log extractor
make iceberg-ingestor-build    # Iceberg ingestor
make data-generator-build      # Data generator

# View container logs
docker logs audit-log-extractor
docker logs iceberg-ingestor
```

### Development

```bash
# Clean environment
make down

# Full reset and reload
make truncate-trino
make orders-build-insert
cd airflow/dags/dbt_dwh && dbt run

# Run specific dbt models
dbt run --select staging     # Only staging layer
dbt run --select marts       # Only marts layer
dbt run --select orders      # Single model

# Run dbt tests
dbt test
```

### Data Management

```bash
# Truncate PostgreSQL audit logs
make truncate-audit-logs

# Truncate Iceberg tables
make truncate-trino

# Generate new dataset
make generate-data
```

### Vault Management

```bash
# Access Vault UI
open http://localhost:8200
# Token: dev-root-token

# Read a secret from CLI
docker exec vault vault kv get secret/postgres/ecommerce

# List all secrets
docker exec vault vault kv list secret/

# View Vault initialization logs
docker logs vault-init
```

### Debugging

```bash
# Check Trino tables
docker exec trino-coordinator trino --catalog iceberg --execute "SHOW TABLES FROM landing;"

# Query specific table
docker exec trino-coordinator trino --catalog iceberg --schema marts --execute "SELECT * FROM orders LIMIT 10;"

# Check dbt compiled SQL
cat airflow/dags/dbt_dwh/target/compiled/dbt_dwh/models/marts/ecomm/orders.sql
```

## 🛡️ Data Contracts

This project uses dbt's data contracts feature to enforce schema validation and prevent breaking changes. All staging and mart models have contracts defined in their respective `schema.yml` files.

### Benefits

- **Type Safety**: Ensures model outputs match expected data types (INTEGER, TIMESTAMP, VARCHAR, DOUBLE, BOOLEAN)
- **Breaking Change Prevention**: Schema changes must be explicit and intentional
- **Documentation**: Each column includes data type and description
- **Quality Checks**: Built-in NOT NULL, UNIQUE, and accepted values tests

### Contract Structure

```yaml
models:
  - name: stg_ecomm_audit_log_dml_orders_flattened
    config:
      contract:
        enforced: true
    columns:
      - name: audit_event_id
        data_type: bigint
        description: "Unique identifier for the audit event"
        tests:
          - not_null
          - unique
      - name: audit_operation
        data_type: varchar
        description: "Type of CDC operation: I, U, D"
        tests:
          - accepted_values:
              values: ['I', 'U', 'D']
      # ... more columns
```

### Covered Models

| Model | Columns | Key Constraints |
|-------|---------|-----------------|
| `stg_ecomm_audit_log_dml` | 8 | UNIQUE on `audit_event_id` |
| `stg_ecomm_audit_log_dml_orders_flattened` | 13 | UNIQUE on `audit_event_id`, accepted values on `audit_operation` |
| `orders` | 13 | UNIQUE + NOT NULL on `order_id` |
| `orders_history_scd2` | 16 | NOT NULL on temporal columns |

### Modifying Schemas

When adding new columns:

1. Update the model SQL
2. Update the contract in `schema.yml`
3. Run `dbt compile` to validate
4. Deploy changes

```bash
# Validate contracts without executing
dbt compile --select orders

# Run with contract validation
dbt run --select orders
```

## 📊 Example Queries

```sql
-- Check for orders that were updated multiple times
SELECT order_id, COUNT(*) as change_count
FROM staging.stg_ecomm_audit_log_dml_orders_flattened
GROUP BY order_id
HAVING COUNT(*) > 1;

-- View deleted orders (not in marts, but in staging)
SELECT order_id, audit_operation, audit_timestamp
FROM staging.stg_ecomm_audit_log_dml_orders_flattened
WHERE audit_operation = 'D'
  AND order_id NOT IN (SELECT order_id FROM marts.orders);

-- Check mart table freshness
SELECT
    MAX(ingested_at) as latest_ingestion,
    COUNT(*) as total_orders,
    COUNT(DISTINCT order_id) as unique_orders
FROM marts.orders;
```

## 🧪 Testing CDC Logic

```bash
# 1. Load initial dataset
make orders-build-insert
cd airflow/dags/dbt_dwh && dbt run

# 2. Note the count
docker exec trino-coordinator trino --catalog iceberg --schema marts --execute "SELECT COUNT(*) FROM orders;"

# 3. Generate updates (modify generate_data.py to create more updates)
python ecommerce-db/data-generator/generate_data.py --obj-type order --num-records 50 --output-file ecommerce-db/data-generator/data/orders_batch2.csv

# 4. Load new batch
docker run --rm --network airflow-iceberg-schema-evolution_default \
  -v $(PWD)/ecommerce-db/data-generator/data:/app/data \
  -e POSTGRES_HOST=ecommerce-db -e POSTGRES_PORT=5432 \
  -e POSTGRES_USER=ecom -e POSTGRES_PASSWORD=ecom -e POSTGRES_DB=ecom \
  data-generator:latest python ingest_data.py --source-files data/orders_batch2.csv --batch-size 500

# 5. Re-run dbt (should process incrementally)
dbt run --select orders

# 6. Verify updates
docker exec trino-coordinator trino --catalog iceberg --schema marts --execute "SELECT COUNT(*) FROM orders;"
```

## 🎓 Learning Outcomes

This project demonstrates:

1. **Modern Data Lakehouse**: Using Iceberg for ACID transactions on object storage
2. **CDC Pattern**: Capturing and processing database changes incrementally
3. **Secrets Management**: HashiCorp Vault for secure credential storage
4. **Containerized ETL**: Docker containers for extraction and ingestion
5. **Dynamic Task Mapping**: Airflow's `.expand()` for parallel processing
6. **dbt Best Practices**: Incremental models, sources, staging/mart separation
7. **Schema Evolution**: Handling new columns without breaking pipelines
8. **Custom Materializations**: Extending dbt with project-specific logic
9. **Container Orchestration**: Multi-service Docker Compose setup
10. **Object Storage Integration**: S3/MinIO as intermediate landing zone

## 🐛 Troubleshooting

### Vault Issues

```bash
# Vault not starting
docker logs vault

# Secrets not initialized
docker logs vault-init

# Re-initialize Vault secrets
docker restart vault-init

# Access denied errors
# Verify VAULT_TOKEN is set to: dev-root-token
```

### Docker Container Issues

```bash
# Extractor container fails
docker logs <container-id>
# Common issues:
# - Vault connection timeout (check network)
# - Invalid S3 key characters (check timestamps)
# - PostgreSQL connection refused

# Ingestor container fails
docker logs <container-id>
# Common issues:
# - Trino connection timeout
# - CSV parsing errors
# - Vault secret not found

# Rebuild containers after code changes
make build-all-containers
```

### Airflow DAG Issues

```bash
# DAG not appearing
# 1. Check for syntax errors:
docker exec -it airflow-scheduler airflow dags list

# 2. Check Airflow logs:
docker logs airflow-scheduler

# Task stuck in running state
# Check Docker container status:
docker ps | grep extractor
docker ps | grep ingestor
```

### dbt compilation errors
```bash
# Clear dbt cache
rm -rf airflow/dags/dbt_dwh/target
dbt clean
dbt compile
```

### Trino connection issues
```bash
# Restart Trino
docker restart trino-coordinator

# Check Trino logs
docker logs trino-coordinator

# Test Trino connection
docker exec trino-coordinator trino --execute "SELECT 1"
```

### Schema mismatch errors
```bash
# Drop and recreate table
docker exec trino-coordinator trino --catalog iceberg --schema marts --execute "DROP TABLE IF EXISTS orders;"
dbt run --select orders --full-refresh
```

### Data contract violations
```bash
# Error: "Column X has data type Y but contract expects Z"
# Solution: Update the contract in schema.yml to match actual output

# Error: "Contract enforced but column missing"
# Solution: Add the missing column to the model SQL or remove from contract
```

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
