"""DriftDB exceptions"""


class DriftDBError(Exception):
    """Base exception for all DriftDB errors"""
    pass


class ConnectionError(DriftDBError):
    """Error connecting to DriftDB server"""
    pass


class QueryError(DriftDBError):
    """Error executing a query"""
    pass


class TimeoutError(DriftDBError):
    """Operation timed out"""
    pass


class AuthenticationError(DriftDBError):
    """Authentication failed"""
    pass


class TransactionError(DriftDBError):
    """Transaction operation failed"""
    pass
