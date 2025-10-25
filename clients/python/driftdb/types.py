"""Type definitions for DriftDB"""

from typing import Any, Dict, List, Optional
from enum import Enum


class DataType(Enum):
    """SQL data types supported by DriftDB"""
    INTEGER = "INTEGER"
    TEXT = "TEXT"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    JSON = "JSON"


class Row:
    """A database row with dict-like access"""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def __getitem__(self, key: str) -> Any:
        """Get column value by name"""
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        """Check if column exists"""
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        """Get column value with default"""
        return self._data.get(key, default)

    def keys(self) -> List[str]:
        """Get all column names"""
        return list(self._data.keys())

    def values(self) -> List[Any]:
        """Get all values"""
        return list(self._data.values())

    def items(self) -> List[tuple]:
        """Get all (key, value) pairs"""
        return list(self._data.items())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self._data.copy()

    def __repr__(self) -> str:
        return f"Row({self._data})"


class Column:
    """Database column metadata"""

    def __init__(self, name: str, data_type: str, nullable: bool = True):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable

    def __repr__(self) -> str:
        null_str = "NULL" if self.nullable else "NOT NULL"
        return f"Column({self.name}: {self.data_type} {null_str})"
