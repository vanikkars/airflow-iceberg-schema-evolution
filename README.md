# Change Data Capture (CDC) Pipeline with Iceberg & dbt

A production-ready data pipeline that implements Change Data Capture (CDC) pattern using Apache Iceberg, dbt, and Trino. This project demonstrates how to build a modern data lakehouse that captures database changes (inserts, updates, deletes) from PostgreSQL audit logs and materializes them into dimensional tables with automatic schema evolution.

## 🎯 Purpose

This project solves the challenge of maintaining synchronized replicas of operational databases in analytical data warehouses. Instead of periodic full dumps, it:

- **Captures incremental changes** from PostgreSQL audit logs
- **Processes CDC events** (INSERT, UPDATE, DELETE operations)
- **Maintains current state tables** in the mart layer
- **Handles schema evolution** automatically when source columns change
- **Uses Iceberg format** for ACID transactions and time-travel capabilities

## 🏗️ Architecture

```
┌─────────────────┐
│   PostgreSQL    │  Source database with audit_log_dml table
│  (audit logs)   │  Captures I/U/D operations as JSON
└────────┬────────┘
         │
         │ Python Ingestor
         ↓
┌─────────────────┐
│  Iceberg Layer  │
│   (Landing)     │  Raw audit events stored in Iceberg format
└────────┬────────┘
         │
         │ dbt Transformation
         ↓
┌─────────────────┐
│  Iceberg Layer  │
│   (Staging)     │  Flattened JSON + deduplication
└────────┬────────┘
         │
         │ dbt Incremental MERGE (UPDATE/INSERT/DELETE)
         ↓
┌─────────────────┐
│  Iceberg Layer  │
│    (Marts)      │  Current state tables (latest version of each record)
└─────────────────┘
         │
         │ Queried via Trino
         ↓
    Analytics/BI Tools
```

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 3.0 | Workflow scheduling and monitoring |
| **Data Warehouse** | Apache Iceberg | ACID-compliant data lakehouse format |
| **Query Engine** | Trino | Distributed SQL query engine |
| **Transformation** | dbt | Data modeling and transformation |
| **Source Database** | PostgreSQL | Operational database with audit logs |
| **Data Generator** | Python + Faker | Synthetic CDC event generation |
| **Container Runtime** | Docker Compose | Local development environment |

## ✨ Key Features

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

   # Start all services (Airflow, Trino, PostgreSQL, MinIO)
   make up
   ```

2. **Initialize Trino schemas**
   ```bash
   make trino-init
   ```

3. **Generate and load sample data**
   ```bash
   # Generate 100 orders with inserts, updates, and deletes
   make generate-data

   # Build ingestor and load data into PostgreSQL
   make orders-build-insert
   ```

4. **Run dbt transformations**
   ```bash
   cd airflow/dags/dbt_dwh
   dbt run
   ```

5. **Query the results**
   ```bash
   docker exec -it trino-coordinator trino --catalog iceberg --schema marts

   # In Trino shell:
   SELECT * FROM orders;
   ```

### Access Web UIs

- **Airflow UI**: http://localhost:8080 (username: `airflow`, password: `airflow`)
- **Trino UI**: http://localhost:8081

## 📁 Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── dbt_dwh/                      # dbt project
│   │   │   ├── models/
│   │   │   │   ├── staging/              # Flattening & deduplication
│   │   │   │   │   └── ecomm/
│   │   │   │   │       ├── stg_ecomm_audit_log_dml.sql
│   │   │   │   │       └── stg_ecomm_audit_log_dml_orders_flattened.sql
│   │   │   │   ├── marts/                # Business logic & current state
│   │   │   │   │   └── ecomm/
│   │   │   │   │       └── orders.sql    # Current state table
│   │   │   │   └── sources.yml           # Source definitions
│   │   │   └── macros/                   # Custom materializations
│   │   │       └── trino_incremental_always_merge.sql
│   │   └── extract_audit_logs*.py        # Airflow DAGs for extraction
│   └── plugins/dbt_operator/             # Custom Airflow dbt operator
├── ecommerce-db/
│   └── ingestor/
│       ├── generate_data.py              # Synthetic data generator
│       ├── ingest_data.py                # PostgreSQL loader
│       └── data/                         # Generated CSV files
├── docker-compose.yaml                   # Full stack definition
├── makefile                              # Convenience commands
├── CLAUDE.md                             # AI assistant context
└── README.md                             # This file
```

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

### 2. Landing Layer (Iceberg)

Raw audit logs ingested via Python script:
- Batch loading from CSV files
- Adds `ingested_at` and `source_file` metadata columns
- Stored in Iceberg format for ACID compliance

### 3. Staging Layer (dbt)

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

### 4. Marts Layer (dbt)

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
python ecommerce-db/ingestor/generate_data.py --obj-type order --num-records 50 --output-file ecommerce-db/ingestor/data/orders_batch2.csv

# 4. Load new batch
docker run --rm --network airflow-iceberg-schema-evolution_default \
  -e POSTGRES_HOST=ecommerce-db -e POSTGRES_PORT=5432 \
  -e POSTGRES_USER=ecom -e POSTGRES_PASSWORD=ecom -e POSTGRES_DB=ecom \
  ingestor:latest python ingest_data.py --source-files data/orders_batch2.csv --batch-size 500

# 5. Re-run dbt (should process incrementally)
dbt run --select orders

# 6. Verify updates
docker exec trino-coordinator trino --catalog iceberg --schema marts --execute "SELECT COUNT(*) FROM orders;"
```

## 🎓 Learning Outcomes

This project demonstrates:

1. **Modern Data Lakehouse**: Using Iceberg for ACID transactions on object storage
2. **CDC Pattern**: Capturing and processing database changes incrementally
3. **dbt Best Practices**: Incremental models, sources, staging/mart separation
4. **Schema Evolution**: Handling new columns without breaking pipelines
5. **Custom Materializations**: Extending dbt with project-specific logic
6. **Container Orchestration**: Multi-service Docker Compose setup

## 🐛 Troubleshooting

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

## 📝 License

This project is open source and available under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

**Built with ❤️ as a demonstration of modern data engineering patterns**
