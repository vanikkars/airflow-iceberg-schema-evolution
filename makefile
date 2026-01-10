
duckdb-init:
	@echo "DuckDB Iceberg tables will be created automatically by Airflow DAG tasks"
	@echo "Run the DAG 'extract_audit_logs_ecomm' or 'extract_audit_logs_ecomm_full' to initialize schemas and tables"

up:
	docker-compose up --build

data-generator-build:
	docker build -t data-generator:latest ./ecommerce-db/data-generator

extractor-build:
	docker build -t audit-log-extractor:latest -f extractor/Dockerfile .

build-all-containers:
	$(MAKE) data-generator-build
	$(MAKE) extractor-build

orders-insert:
	 docker run --rm \
		  --network airflow-iceberg-schema-evolution_default \
		  -v $(PWD)/ecommerce-db/data-generator/data:/app/data \
	   -e POSTGRES_HOST=ecommerce-db  \
	   -e POSTGRES_PORT=5432 \
	   -e POSTGRES_USER=ecom \
	   -e POSTGRES_PASSWORD=ecom \
	   -e POSTGRES_DB=ecom \
	   data-generator:latest \
	  python ingest_data.py \
	  --source-files data/orders.csv \
	  --batch-size 500

orders-insert-new:
	 docker run --rm \
		  --network airflow-iceberg-schema-evolution_default \
		  -v $(PWD)/ecommerce-db/data-generator/data:/app/data \
	   -e POSTGRES_HOST=ecommerce-db  \
	   -e POSTGRES_PORT=5432 \
	   -e POSTGRES_USER=ecom \
	   -e POSTGRES_PASSWORD=ecom \
	   -e POSTGRES_DB=ecom \
	   data-generator:latest \
	  python ingest_data.py \
	  --source-files data/orders-new.csv \
	  --batch-size 500


truncate-audit-logs:
	docker exec -i airflow-iceberg-schema-evolution-ecommerce-db-1 psql -U ecom -d ecom -c "TRUNCATE TABLE audit_logs_dml;"

truncate-duckdb:
	@echo "To truncate DuckDB Iceberg tables, delete the database file: /tmp/iceberg.duckdb"
	@echo "Or use dbt to drop and recreate tables: dbt run --select ... --full-refresh"

orders-insert-clean:
	$(MAKE) truncate-audit-logs || true
	$(MAKE) orders-insert

down:
	docker-compose down -v --remove-orphans

clean:
	find ./airflow -name "__pycache__" -prune -exec rm -rf {} +
	find ./airflow -name "*.pyc" -delete

local-install:
	pip install -r airflow/requirements.txt
	pip install -r airflow/requirements-local.txt
	pip install -r ecommerce-db/data-generator/requirements.txt

generate-data:
	python ecommerce-db/data-generator/generate_data.py --obj-type order --output-file ecommerce-db/data-generator/data/orders.csv