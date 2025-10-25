/**
 * DriftDB JavaScript/TypeScript Client
 *
 * @packageDocumentation
 */

export { Client } from './client';
export {
  DriftDBError,
  ConnectionError,
  QueryError,
  TimeoutError,
  AuthenticationError,
  TransactionError,
} from './errors';
export {
  DataType,
  Column,
  Row,
  QueryResult,
  ConnectionConfig,
  PoolConfig,
  ClientConfig,
  IsolationLevel,
  QueryParams,
} from './types';
