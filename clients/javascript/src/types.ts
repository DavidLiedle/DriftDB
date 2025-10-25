/**
 * Type definitions for DriftDB
 */

/** SQL data types supported by DriftDB */
export enum DataType {
  INTEGER = 'INTEGER',
  TEXT = 'TEXT',
  DECIMAL = 'DECIMAL',
  BOOLEAN = 'BOOLEAN',
  TIMESTAMP = 'TIMESTAMP',
  JSON = 'JSON',
}

/** Column metadata */
export interface Column {
  name: string;
  dataType: DataType;
  nullable: boolean;
}

/** A single row with typed access */
export interface Row {
  [key: string]: any;
  get<T = any>(column: string): T;
  get<T = any>(column: string, defaultValue: T): T;
}

/** Query result with metadata */
export interface QueryResult {
  rows: Row[];
  rowCount: number;
  columns: string[];
  executionTimeMs?: number;
}

/** Connection configuration */
export interface ConnectionConfig {
  host: string;
  port: number;
  timeout?: number;
  user?: string;
  password?: string;
  database?: string;
}

/** Connection pool configuration */
export interface PoolConfig extends ConnectionConfig {
  minConnections?: number;
  maxConnections?: number;
}

/** Client configuration */
export interface ClientConfig {
  address: string;
  minConnections?: number;
  maxConnections?: number;
  timeout?: number;
}

/** Transaction isolation levels */
export enum IsolationLevel {
  READ_UNCOMMITTED = 'READ UNCOMMITTED',
  READ_COMMITTED = 'READ COMMITTED',
  REPEATABLE_READ = 'REPEATABLE READ',
  SERIALIZABLE = 'SERIALIZABLE',
}

/** Query parameters */
export type QueryParams = (string | number | boolean | null)[];
