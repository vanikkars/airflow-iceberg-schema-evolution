"""
PostgreSQL handler for database operations.
"""

import logging
import psycopg2

logger = logging.getLogger(__name__)


class PostgresHandler:
    """
    Lightweight PostgreSQL handler for standalone scripts.
    Provides a clean interface around psycopg2.
    """

    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._conn = None

    def get_conn(self):
        """Get or create database connection."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")
        return self._conn

    def get_records(self, sql, parameters=None):
        """
        Execute SQL query and return all records.

        Args:
            sql: SQL query string
            parameters: Query parameters (tuple)

        Returns:
            List of tuples (query results)
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, parameters)
            return cursor.fetchall()
        finally:
            cursor.close()

    def close(self):
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Closed PostgreSQL connection")
