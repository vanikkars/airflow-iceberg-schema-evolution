to check the output of api server:
1. either open a swagger in the browser http://localhost:8080/docs
2. or use curl
```shell
curl -X GET "http://localhost:8000/rides?start_date=2025-08-01&end_date=2025-08-02" -H "Accept: application/json"
```

# Setup
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
