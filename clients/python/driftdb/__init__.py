"""
DriftDB Python Client

An idiomatic Python client for DriftDB - an append-only database with time-travel capabilities.

Example usage:
    ```python
    import asyncio
    from driftdb import Client

    async def main():
        # Connect to DriftDB
        client = await Client.connect("localhost:5432")

        # Execute queries
        results = await client.query("SELECT * FROM users WHERE active = ?", [True])
        for row in results:
            print(f"User: {row['name']}")

        # Time-travel query
        historical = await client.query_at_seq(
            "SELECT * FROM users",
            sequence=1000
        )

        # Close connection
        await client.close()

    asyncio.run(main())
    ```
"""

from .client import Client, Connection, ConnectionPool
from .query import Query, QueryBuilder, QueryResult
from .exceptions import (
    DriftDBError,
    ConnectionError,
    QueryError,
    TimeoutError,
    AuthenticationError,
)
from .types import Row, Column, DataType

__version__ = "0.9.0"
__all__ = [
    "Client",
    "Connection",
    "ConnectionPool",
    "Query",
    "QueryBuilder",
    "QueryResult",
    "DriftDBError",
    "ConnectionError",
    "QueryError",
    "TimeoutError",
    "AuthenticationError",
    "Row",
    "Column",
    "DataType",
]
