
trino-init:
	docker exec -it trino-coordinator trino --catalog iceberg --file /etc/trino/init.sql
	@echo "Schema Landing, Staging, Curated are created in Trino Iceberg Catalog"

up:
	docker-compose up --build

down:
	docker-compose down -v --remove-orphans

clean:
	find ./airflow -name "__pycache__" -prune -exec rm -rf {} +
	find ./airflow -name "*.pyc" -delete