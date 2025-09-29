import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "marts")

SQLALCHEMY_URL = f"trino://{TRINO_USER}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}/{TRINO_SCHEMA}"

engine = create_engine(SQLALCHEMY_URL)

st.title("Uber Rides Daily Metrics (Trino / Iceberg)")

@st.cache_data(ttl=300)
def load_metrics():
    # Table produced by dbt: mart.uber_rides_daily_metrics (Iceberg)
    sql = text("""
        SELECT
          ride_date,
          rides_count,
          avg_distance_km,
          max_distance_km,
          min_distance_km
        FROM uber_rides_daily_metrics
        ORDER BY ride_date
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

df = load_metrics()

st.subheader("KPIs")
st.metric("Total rides", f"{int(df.rides_count.sum()):,}")
st.metric("Avg distance (km)", f"{df.avg_distance_km.mean():.2f}")
st.metric("Max distance (km)", f"{df.max_distance_km.max():.2f}")
st.metric("Min distance (km)", f"{df.min_distance_km.min():.2f}")

st.subheader("Rides per day")
st.line_chart(df.set_index("ride_date")[["rides_count"]])

st.subheader("Average distance per day")
st.bar_chart(df.set_index("ride_date")[["avg_distance_km"]])

with st.expander("Raw data"):
    st.dataframe(df, use_container_width=True)