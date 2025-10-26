
trino-init:
	docker exec -it trino-coordinator trino --catalog iceberg --file /etc/trino/init.sql
	@echo "Schema Landing, Staging, Curated are created in Trino Iceberg Catalog"

up:
	docker-compose up --build

data-generator-build:
	docker build -t ingestor:latest ./ecommerce-db/data-generator

extractor-build:
	docker build -t audit-log-extractor:latest ./extractor

iceberg-ingestor-build:
	docker build -t iceberg-ingestor:latest ./iceberg-ingestor

build-all-containers:
	$(MAKE) data-generator-build
	$(MAKE) extractor-build
	$(MAKE) iceberg-ingestor-build

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

truncate-trino:
	docker exec -it trino-coordinator trino --catalog iceberg --schema marts --execute "TRUNCATE TABLE orders;"
	docker exec -it trino-coordinator trino --catalog iceberg --schema staging --execute "TRUNCATE TABLE stg_ecomm_audit_log_dml;"
	docker exec -it trino-coordinator trino --catalog iceberg --schema staging --execute "TRUNCATE TABLE stg_ecomm_audit_log_dml_orders_flattened;"

orders-insert-clean:
	$(MAKE) truncate-audit-logs || true
	$(MAKE) truncate-trino || true
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