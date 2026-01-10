import os
import pandas as pd
import streamlit as st
import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/tmp/iceberg.duckdb")
DUCKDB_SCHEMA = os.getenv("DUCKDB_SCHEMA", "marts")

# Create DuckDB connection
conn = duckdb.connect(DUCKDB_PATH, read_only=True)

st.title("Ecommerce Orders Analytics (DuckDB / Iceberg)")

@st.cache_data(ttl=300)
def load_orders():
    # Table produced by dbt: marts.orders (Iceberg)
    sql = """
        SELECT
          order_id,
          customer_id,
          order_timestamp,
          created_at,
          updated_at,
          order_sum,
          description,
          audit_operation
        FROM marts.orders
        ORDER BY order_timestamp DESC
        LIMIT 1000
    """
    try:
        return conn.execute(sql).df()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_order_summary():
    # Aggregate orders summary
    sql = """
        SELECT
          DATE(order_timestamp) as order_date,
          COUNT(*) as order_count,
          AVG(order_sum) as avg_order_sum,
          SUM(order_sum) as total_sum,
          MAX(order_sum) as max_order_sum,
          MIN(order_sum) as min_order_sum
        FROM marts.orders
        WHERE audit_operation <> 'D'
        GROUP BY DATE(order_timestamp)
        ORDER BY order_date DESC
    """
    try:
        return conn.execute(sql).df()
    except Exception as e:
        st.error(f"Error loading summary: {e}")
        return pd.DataFrame()

try:
    summary_df = load_order_summary()

    if not summary_df.empty:
        st.subheader("KPIs")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Orders", f"{summary_df['order_count'].sum():,}")
        with col2:
            st.metric("Avg Order Sum", f"${summary_df['avg_order_sum'].mean():.2f}")
        with col3:
            st.metric("Max Order Sum", f"${summary_df['max_order_sum'].max():.2f}")
        with col4:
            st.metric("Total Revenue", f"${summary_df['total_sum'].sum():,.2f}")

        st.subheader("Orders per day")
        st.line_chart(summary_df.set_index("order_date")[["order_count"]])

        st.subheader("Average order sum per day")
        st.bar_chart(summary_df.set_index("order_date")[["avg_order_sum"]])

        with st.expander("Summary data"):
            st.dataframe(summary_df, use_container_width=True)
    else:
        st.info("No orders data available. Run the Airflow DAG to load data.")

except Exception as e:
    st.error(f"Failed to load dashboard: {e}")
    st.info("Make sure the DuckDB Iceberg database is initialized and contains data in the marts.orders table.")

# Detailed orders view
st.subheader("Recent Orders")
try:
    orders_df = load_orders()
    if not orders_df.empty:
        with st.expander("View detailed orders"):
            st.dataframe(orders_df, use_container_width=True)
    else:
        st.info("No orders available yet.")
except Exception as e:
    st.error(f"Error loading orders: {e}")