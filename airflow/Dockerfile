ARG AIRFLOW_VERSION=3.0.6

FROM apache/airflow:${AIRFLOW_VERSION}
ADD ./requirements.txt requirements.txt
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt