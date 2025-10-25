import logging
import os
import csv
import sys
import json
import argparse
import psycopg2
from psycopg2.extras import execute_batch


DB_HOST = os.getenv("POSTGRES_HOST", os.getenv("PGHOST", "ecommerce-db"))
DB_PORT = int(os.getenv("POSTGRES_PORT", os.getenv("PGPORT", "5432")))
DB_USER = os.getenv("POSTGRES_USER", os.getenv("PGUSER", "ecom"))
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("PGPASSWORD", "ecom"))
DB_NAME = os.getenv("POSTGRES_DB", os.getenv("PGDATABASE", "ecom"))

logger = logging.getLogger(__name__)

def ingest_single_file(
        csv_path: str,
        conn,
        batch_size=500,
) -> int:
    logger.info(f"Starting ingestion of {csv_path} (batch_size={batch_size})")
    insert_sql = """
    INSERT INTO audit_log_dml (
        audit_operation, 
        audit_timestamp,
        tbl_schema,
        tbl_name, 
        raw_data
    )
    VALUES (%s, %s, %s, %s, %s);
    """

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        batch = []
        rows = 0
        batches = 0

        with conn.cursor() as cur:
            for row in reader:
                audit_operation = row.pop('audit_operation')
                audit_timestamp = row.pop('audit_timestamp')
                schema = row.pop('schema')
                table = row.pop('table')

                batch.append((audit_operation, audit_timestamp, schema, table, json.dumps(row, ensure_ascii=False)))
                if len(batch) >= batch_size:
                    execute_batch(cur, insert_sql, batch, page_size=len(batch))
                    batch.clear()
                    batches += 1
                    logger.info(f"{csv_path}: committed batch #{batches}")
                rows += 1
            if batch:
                execute_batch(cur, insert_sql, batch, page_size=len(batch))
        conn.commit()

    logger.info(f"Finished {csv_path}: rows={rows}, batches={batches}")
    return rows

def ingest_to_db(args, csv_paths):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
    )
    total = 0
    try:
        for path in csv_paths:
            total += ingest_single_file(path, conn, batch_size=args.batch_size)
        print(f"Inserted {total} rows from {len(csv_paths)} file(s) into audit_log_dml")
    finally:
        conn.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    ap = argparse.ArgumentParser(description="Load one or more order CSV files into audit_log_dml.")
    ap.add_argument(
        "--source-files",
        action="append",
        help="Path to a CSV file (repeatable)."
    )
    ap.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "500")))
    ap.add_argument("--seed", type=int)

    args = ap.parse_args()

    csv_paths = args.source_files if isinstance(args.source_files, list) else [args.source_files]
    for p in csv_paths:
        if not os.path.isfile(p):
            raise FileNotFoundError(f"CSV not found: {p}")

    ingest_to_db(args, csv_paths)


if __name__ == "__main__":
    main()