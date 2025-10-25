"""Query building and result handling"""

from typing import Any, Dict, List, Optional, Iterator
from .types import Row, Column


class QueryResult:
    """
    Result of a database query.

    Provides both list-like and iterator access to rows.

    Example:
        ```python
        result = await client.query("SELECT * FROM users")

        # List-like access
        first_row = result[0]
        num_rows = len(result)

        # Iteration
        for row in result:
            print(row['name'])

        # Metadata
        print(f"Query took {result.execution_time_ms}ms")
        print(f"Columns: {result.columns}")
        ```
    """

    def __init__(
        self,
        rows: List[Row],
        row_count: int,
        columns: List[str],
        execution_time_ms: Optional[float] = None
    ):
        self.rows = rows
        self.row_count = row_count
        self.columns = columns
        self.execution_time_ms = execution_time_ms

    def __len__(self) -> int:
        """Number of rows"""
        return self.row_count

    def __getitem__(self, index: int) -> Row:
        """Get row by index"""
        return self.rows[index]

    def __iter__(self) -> Iterator[Row]:
        """Iterate over rows"""
        return iter(self.rows)

    def __repr__(self) -> str:
        return f"QueryResult({self.row_count} rows, {len(self.columns)} columns)"

    def first(self) -> Optional[Row]:
        """Get first row or None"""
        return self.rows[0] if self.rows else None

    def all(self) -> List[Row]:
        """Get all rows as list"""
        return self.rows

    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Convert all rows to list of dictionaries"""
        return [row.to_dict() for row in self.rows]


class Query:
    """Represents a SQL query (for internal use)"""

    def __init__(self, sql: str, params: Optional[List[Any]] = None):
        self.sql = sql
        self.params = params or []

    def __repr__(self) -> str:
        return f"Query({self.sql}, {self.params})"


class QueryBuilder:
    """
    Fluent query builder for constructing SQL queries programmatically.

    Example:
        ```python
        query = (QueryBuilder("users")
            .select("id", "name", "email")
            .where("age", ">", 18)
            .where("active", "=", True)
            .order_by("name", "ASC")
            .limit(10))

        result = await client.query(query.build(), query.params())
        ```
    """

    def __init__(self, table: str):
        self.table = table
        self._select_columns: List[str] = []
        self._where_clauses: List[tuple] = []
        self._order_by_clauses: List[tuple] = []
        self._limit_value: Optional[int] = None
        self._offset_value: Optional[int] = None
        self._params: List[Any] = []

    def select(self, *columns: str) -> "QueryBuilder":
        """Select specific columns"""
        self._select_columns.extend(columns)
        return self

    def where(self, column: str, operator: str, value: Any) -> "QueryBuilder":
        """Add WHERE clause"""
        self._where_clauses.append((column, operator, value))
        self._params.append(value)
        return self

    def order_by(self, column: str, direction: str = "ASC") -> "QueryBuilder":
        """Add ORDER BY clause"""
        self._order_by_clauses.append((column, direction))
        return self

    def limit(self, limit: int) -> "QueryBuilder":
        """Add LIMIT clause"""
        self._limit_value = limit
        return self

    def offset(self, offset: int) -> "QueryBuilder":
        """Add OFFSET clause"""
        self._offset_value = offset
        return self

    def build(self) -> str:
        """Build the SQL query string"""
        # SELECT clause
        if self._select_columns:
            columns = ", ".join(self._select_columns)
        else:
            columns = "*"

        sql = f"SELECT {columns} FROM {self.table}"

        # WHERE clause
        if self._where_clauses:
            where_parts = [f"{col} {op} ?" for col, op, _ in self._where_clauses]
            sql += " WHERE " + " AND ".join(where_parts)

        # ORDER BY clause
        if self._order_by_clauses:
            order_parts = [f"{col} {direction}" for col, direction in self._order_by_clauses]
            sql += " ORDER BY " + ", ".join(order_parts)

        # LIMIT clause
        if self._limit_value is not None:
            sql += f" LIMIT {self._limit_value}"

        # OFFSET clause
        if self._offset_value is not None:
            sql += f" OFFSET {self._offset_value}"

        return sql

    def params(self) -> List[Any]:
        """Get query parameters"""
        return self._params

    def __repr__(self) -> str:
        return f"QueryBuilder(table={self.table})"
