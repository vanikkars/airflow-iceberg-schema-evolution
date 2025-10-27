"""
Trino handler for executing queries.
"""

import logging
from trino.dbapi import connect

logger = logging.getLogger(__name__)


class TrinoHandler:
    """Lightweight Trino handler for executing queries."""

    def __init__(self, host, port, user, catalog):
        self.host = host
        self.port = int(port)
        self.user = user
        self.catalog = catalog
        self._conn = None

    def get_conn(self):
        """Get or create Trino connection."""
        if self._conn is None:
            self._conn = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                http_scheme='http'
            )
            logger.info(f"Connected to Trino at {self.host}:{self.port}/{self.catalog}")
        return self._conn

    def run(self, sql):
        """
        Execute SQL query without returning results.

        Args:
            sql: SQL query string
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            logger.debug(f"Executing SQL: {sql[:200]}...")
            cursor.execute(sql)
            cursor.fetchall()  # Consume results
            logger.debug("SQL execution completed")
        finally:
            cursor.close()

    def close(self):
        """Close Trino connection."""
        if self._conn:
            self._conn.close()
            logger.info("Closed Trino connection")
