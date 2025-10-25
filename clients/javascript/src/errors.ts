/**
 * DriftDB error classes
 */

/** Base error for all DriftDB errors */
export class DriftDBError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DriftDBError';
  }
}

/** Error connecting to DriftDB server */
export class ConnectionError extends DriftDBError {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

/** Error executing a query */
export class QueryError extends DriftDBError {
  constructor(message: string) {
    super(message);
    this.name = 'QueryError';
  }
}

/** Operation timed out */
export class TimeoutError extends DriftDBError {
  constructor(message: string) {
    super(message);
    this.name = 'TimeoutError';
  }
}

/** Authentication failed */
export class AuthenticationError extends DriftDBError {
  constructor(message: string) {
    super(message);
    this.name = 'AuthenticationError';
  }
}

/** Transaction operation failed */
export class TransactionError extends DriftDBError {
  constructor(message: string) {
    super(message);
    this.name = 'TransactionError';
  }
}
