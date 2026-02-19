"""
DriftDB Client Implementation
"""

import asyncio
import json
import socket
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from .exceptions import ConnectionError, QueryError, AuthenticationError, TimeoutError
from .query import QueryResult
from .types import Row


class Connection:
    """A single connection to DriftDB server."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False

    async def connect(self, timeout: float = 10.0) -> None:
        """Establish connection to DriftDB server."""
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=timeout
            )
            self._connected = True
        except asyncio.TimeoutError:
            raise TimeoutError(f"Connection timeout after {timeout}s")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    async def execute(self, query: str, params: Optional[List[Any]] = None) -> QueryResult:
        """Execute a query and return results."""
        if not self._connected:
            raise ConnectionError("Not connected to server")

        try:
            # Build query message
            message = {
                "type": "query",
                "query": query,
                "params": params or []
            }

            # Send query
            self._writer.write(json.dumps(message).encode() + b"\n")
            await self._writer.drain()

            # Read response
            response_data = await self._reader.readline()
            response = json.loads(response_data.decode())

            if response.get("type") == "error":
                raise QueryError(response.get("message", "Unknown error"))

            # Parse result
            rows = [Row(row) for row in response.get("rows", [])]
            return QueryResult(
                rows=rows,
                row_count=len(rows),
                columns=response.get("columns", []),
                execution_time_ms=response.get("execution_time_ms")
            )

        except json.JSONDecodeError as e:
            raise QueryError(f"Invalid response from server: {e}")
        except Exception as e:
            raise QueryError(f"Query execution failed: {e}")

    async def close(self) -> None:
        """Close the connection."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
        self._connected = False

    def __del__(self):
        """Cleanup on deletion."""
        if self._connected and self._writer:
            try:
                self._writer.close()
            except:
                pass


class ConnectionPool:
    """
    Connection pool for managing multiple connections to DriftDB.

    Example:
        ```python
        pool = ConnectionPool("localhost", 5432, min_size=2, max_size=10)
        await pool.initialize()

        async with pool.acquire() as conn:
            results = await conn.execute("SELECT * FROM users")
        ```
    """

    def __init__(
        self,
        host: str,
        port: int,
        min_size: int = 2,
        max_size: int = 10,
        timeout: float = 10.0
    ):
        self.host = host
        self.port = port
        self.min_size = min_size
        self.max_size = max_size
        self.timeout = timeout
        self._pool: List[Connection] = []
        self._available: asyncio.Queue = asyncio.Queue()
        self._size = 0
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize the connection pool."""
        for _ in range(self.min_size):
            conn = await self._create_connection()
            await self._available.put(conn)

    async def _create_connection(self) -> Connection:
        """Create a new connection."""
        conn = Connection(self.host, self.port)
        await conn.connect(self.timeout)
        self._size += 1
        return conn

    async def acquire(self) -> Connection:
        """Acquire a connection from the pool."""
        try:
            # Try to get an available connection
            conn = await asyncio.wait_for(
                self._available.get(),
                timeout=1.0
            )
            return conn
        except asyncio.TimeoutError:
            # Create new connection if under max_size
            async with self._lock:
                if self._size < self.max_size:
                    return await self._create_connection()
            # Wait for an available connection
            return await self._available.get()

    async def release(self, conn: Connection) -> None:
        """Release a connection back to the pool."""
        await self._available.put(conn)

    async def close(self) -> None:
        """Close all connections in the pool."""
        while not self._available.empty():
            try:
                conn = await asyncio.wait_for(self._available.get(), timeout=0.1)
                await conn.close()
            except:
                pass

    def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


class Client:
    """
    High-level DriftDB client with connection pooling and convenience methods.

    Example:
        ```python
        client = await Client.connect("localhost:5432")

        # Simple query
        users = await client.query("SELECT * FROM users WHERE age > ?", [18])

        # Time-travel query
        historical = await client.query_at_seq("SELECT * FROM orders", sequence=1000)

        # Transaction
        async with client.transaction() as tx:
            await tx.execute("INSERT INTO users VALUES (?, ?)", [1, "Alice"])
            await tx.execute("INSERT INTO users VALUES (?, ?)", [2, "Bob"])
            await tx.commit()

        await client.close()
        ```
    """

    def __init__(self, pool: ConnectionPool):
        self._pool = pool

    @classmethod
    async def connect(
        cls,
        address: str,
        min_connections: int = 2,
        max_connections: int = 10,
        timeout: float = 10.0
    ) -> "Client":
        """
        Connect to DriftDB server.

        Args:
            address: Server address in format "host:port"
            min_connections: Minimum pool size
            max_connections: Maximum pool size
            timeout: Connection timeout in seconds

        Returns:
            Connected Client instance
        """
        host, port_str = address.rsplit(":", 1)
        port = int(port_str)

        pool = ConnectionPool(
            host=host,
            port=port,
            min_size=min_connections,
            max_size=max_connections,
            timeout=timeout
        )
        await pool.initialize()

        return cls(pool)

    async def query(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        timeout: Optional[float] = None
    ) -> QueryResult:
        """
        Execute a SQL query.

        Args:
            sql: SQL query string
            params: Query parameters (for parameterized queries)
            timeout: Query timeout in seconds

        Returns:
            QueryResult with rows and metadata
        """
        conn = await self._pool.acquire()
        try:
            result = await conn.execute(sql, params)
            return result
        finally:
            await self._pool.release(conn)

    async def query_at_seq(
        self,
        sql: str,
        sequence: int,
        params: Optional[List[Any]] = None
    ) -> QueryResult:
        """
        Execute a time-travel query at a specific sequence number.

        Args:
            sql: SQL query string
            sequence: Sequence number to query at
            params: Query parameters

        Returns:
            QueryResult with historical data
        """
        # Add AS OF clause
        time_travel_sql = f"{sql} FOR SYSTEM_TIME AS OF @SEQ:{sequence}"
        return await self.query(time_travel_sql, params)

    async def query_at_time(
        self,
        sql: str,
        timestamp: Union[str, datetime],
        params: Optional[List[Any]] = None
    ) -> QueryResult:
        """
        Execute a time-travel query at a specific timestamp.

        Args:
            sql: SQL query string
            timestamp: ISO timestamp string or datetime object
            params: Query parameters

        Returns:
            QueryResult with historical data
        """
        if isinstance(timestamp, datetime):
            timestamp = timestamp.isoformat()

        time_travel_sql = f"{sql} FOR SYSTEM_TIME AS OF '{timestamp}'"
        return await self.query(time_travel_sql, params)

    async def execute(self, sql: str, params: Optional[List[Any]] = None) -> int:
        """
        Execute a non-query SQL statement (INSERT, UPDATE, DELETE).

        Args:
            sql: SQL statement
            params: Statement parameters

        Returns:
            Number of affected rows
        """
        result = await self.query(sql, params)
        return result.row_count

    def transaction(self) -> "Transaction":
        """
        Start a new transaction.

        Returns:
            Transaction context manager
        """
        return Transaction(self._pool)

    async def close(self) -> None:
        """Close all connections."""
        await self._pool.close()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


class Transaction:
    """Transaction context manager."""

    def __init__(self, pool: ConnectionPool):
        self._pool = pool
        self._conn: Optional[Connection] = None

    async def __aenter__(self):
        """Start transaction."""
        self._conn = await self._pool.acquire()
        await self._conn.execute("BEGIN TRANSACTION")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Commit or rollback transaction."""
        try:
            if exc_type is None:
                await self._conn.execute("COMMIT")
            else:
                await self._conn.execute("ROLLBACK")
        finally:
            if self._conn:
                await self._pool.release(self._conn)

    async def execute(self, sql: str, params: Optional[List[Any]] = None) -> QueryResult:
        """Execute a query within the transaction."""
        if not self._conn:
            raise QueryError("Transaction not started")
        return await self._conn.execute(sql, params)

    async def commit(self) -> None:
        """Explicitly commit the transaction."""
        if not self._conn:
            raise QueryError("Transaction not started")
        await self._conn.execute("COMMIT")

    async def rollback(self) -> None:
        """Explicitly rollback the transaction."""
        if not self._conn:
            raise QueryError("Transaction not started")
        await self._conn.execute("ROLLBACK")
