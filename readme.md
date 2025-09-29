# Project Overview

The repository contains an Airflow DAG `uber_rides_iceberg` that orchestrates a layered batch analytics pipeline on Trino + Iceberg: raw ingestion -> normalization -> curated marts for dashboarding.

The data source is uber rides [dataset](https://www.kaggle.com/datasets/yashdevladdha/uber-ride-analytics-dashboard?resource=download)

The dataset captures 148,770 total bookings across multiple vehicle types and provides a complete view of ride-sharing operations including successful rides, cancellations, customer behaviors, and financial metrics.


## Data Flow

1. Landing: Raw Uber rides JSON loaded into Iceberg namespace `landing`.
2. Staging: Cleaning, normalization, incremental transformations in `staging`.
3. Marts: Curated fact / dimension style tables in `marts` for BI and lightweight app use.

## Storage & Query Engine

- Trino catalogs persisted by mounting host directory `trino/trino_config/catalog` (or a persistent `volumes/trino/catalog`) to `/etc/trino/catalog` so definitions survive container restarts.
- Iceberg tables managed via Trino SQL (DDL / DML tasks inside the DAG).

## Orchestration

- Airflow DAG `uber_rides_iceberg` schedules sequential tasks: ingest, transform, publish.
- Failures surfaced in Airflow UI task logs.

## dbt layers
- `landing`: Raw data load (append-only).
- `staging`: Data cleaning, type casting, flattening nested JSON, incremental updates.
- `marts`: Curated tables for analysis (e.g. `uber_rides`, `uber_rides_daily_metrics`)

all the layer are build via `dbt`+`Trino` in Iceberg.

Beware, nessie catalog does not support Views in Iceberg. Thus, the demo uses either tables or incremental tables where appropriate.

## Dashboarding

- Basic Streamlit app provides a minimal exploratory dashboard over curated marts.

## Containers (Highlights)

- Trino coordinator and workers share mounted catalog directory for persistence.
- Streamlit app reading from Trino (via Python client or SQLAlchemy).

## Key Paths

- Trino catalogs: `trino/trino_config/catalog`
- Coordinator config: `trino/trino_config/coordinator/config.properties`
- Worker config: `trino/trino_config/worker/config.properties`
- Compose file: `docker-compose.yml`
- DAG file: (Airflow DAGs directory containing `uber_rides_iceberg`)
- Streamlit app: (project `streamlit` or equivalent folder)

## Summary

End-to-end reproducible analytics stack: ingestion -> modeling -> marts -> lightweight Streamlit Dashboard, all backed by Trino + Iceberg with persistent catalog configuration.


# Setup
<details>
  <summary>Click to expand the Setup </summary>

## Start up the stack
Start the containers:
```shell
make up
````

Create the schemas in Trino:
```shell
make init-trino
```

## Setup Airflow Connections
### 1. Trino connection:
Airflow UI: Admin -> Connections -> Edit trino_conn
Conn Type: Trino

Host: http://trino-coordinator

Port: 8080

Description: A connection to trino query engine

Login: airflow (any non-empty user)

Schema: landing (Trino schema / Iceberg namespace)

Extra (JSON, (adjust catalog name)):
```json 
{"catalog": "iceberg"} 
```
![trino_conn_extras.png](images/trino_conn_extras.png)
![img.png](images/trino_conn_main_settings.png)
### 2. S3 connection:
 Airflow UI: Admin -> Connections -> Edit s3_conn

 Conn Type: Amazon Web Services

 Description: A connection to S3 compatible storage
 Login: admin

 Password: password

 Extra (JSON):
 

```shell
{
  "endpoint_url": "http://minio:9000",
  "region_name": "us-east-1"
}
```
![s3_conn_main_settings.png](images/s3_conn_main_settings.png)
![img.png](images/s3_conn_extras.png)

### Verify if the source service is up and running
To check the output of api server:
1. either open a swagger in the browser http://localhost:8000/docs
2. or use curl
```shell
curl -X GET "http://localhost:8000/rides?start_date=2025-08-01&end_date=2025-08-02" -H "Accept: application/json"
```

</details>

# Demo
## Start the DAG
Go to Airflow UI and find the `uber_rides_iceberg` DAG.
![airflow-iceberg-dag-start.png](images/airflow-iceberg-dag-start.png)
Click on start, then check the logs, verify if all steps were scuccessful by checking the logs.
![airflow-iceberg-dag.png](images/airflow-iceberg-dag.png)

## Check the data in Trino
### Create a connection to Trino 
Create a connection to Trino using your favorite SQL client (e.g. DBeaver, TablePlus, etc.) or use the Trino CLI.

### Run the data discover queries
```sql
-- raw/landing layer
select * from iceberg.landing.rides_raw limit 10;

-- staging layer
select * from iceberg.staging.stg_uber_rides limit 10;

-- mart layer
select * from iceberg.marts.uber_rides limit 10;
```
#### Raw/landing layer
![iceberg_raw_layer.png](images/iceberg/iceberg_raw_layer.png)

#### Staging layer
![staging_layer_flattened.png](images/iceberg/staging_layer_flattened.png)

#### Mart layer
![mart_layer.png](images/iceberg/mart_layer.png)


Next:
- faile the job if dbt fails or has errors
- think about adding separate pipeline for analytics
- add metabase integaration for dashboards
- create slides