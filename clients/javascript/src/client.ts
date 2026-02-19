/**
 * DriftDB Client Implementation
 */

import { Socket } from 'net';
import { ConnectionError, QueryError, TimeoutError, TransactionError } from './errors';
import { ClientConfig, QueryParams, QueryResult, Row } from './types';

/** Single connection to DriftDB */
class Connection {
  private socket: Socket | null = null;
  private connected = false;
  private readonly host: string;
  private readonly port: number;

  constructor(host: string, port: number) {
    this.host = host;
    this.port = port;
  }

  /** Connect to DriftDB server */
  async connect(timeout: number = 10000): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.socket?.destroy();
        reject(new TimeoutError(`Connection timeout after ${timeout}ms`));
      }, timeout);

      this.socket = new Socket();

      this.socket.on('connect', () => {
        clearTimeout(timer);
        this.connected = true;
        resolve();
      });

      this.socket.on('error', (err) => {
        clearTimeout(timer);
        reject(new ConnectionError(`Failed to connect: ${err.message}`));
      });

      this.socket.connect(this.port, this.host);
    });
  }

  /** Execute a query */
  async execute(query: string, params: QueryParams = []): Promise<QueryResult> {
    if (!this.connected || !this.socket) {
      throw new ConnectionError('Not connected to server');
    }

    return new Promise((resolve, reject) => {
      const message = JSON.stringify({
        type: 'query',
        query,
        params,
      });

      let responseData = '';

      const handleData = (data: Buffer) => {
        responseData += data.toString();

        // Check if we have a complete message (ends with newline)
        if (responseData.endsWith('\n')) {
          try {
            const response = JSON.parse(responseData);

            if (response.type === 'error') {
              reject(new QueryError(response.message || 'Unknown error'));
              return;
            }

            // Parse result
            const rows: Row[] = (response.rows || []).map((row: any) => ({
              ...row,
              get<T = any>(column: string, defaultValue?: T): T {
                return column in row ? row[column] : defaultValue as T;
              },
            }));

            resolve({
              rows,
              rowCount: rows.length,
              columns: response.columns || [],
              executionTimeMs: response.execution_time_ms,
            });
          } catch (err) {
            reject(new QueryError(`Invalid response: ${err}`));
          }
        }
      };

      this.socket!.once('data', handleData);
      this.socket!.write(message + '\n');
    });
  }

  /** Close the connection */
  async close(): Promise<void> {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }
}

/** Connection pool for managing multiple connections */
class ConnectionPool {
  private readonly host: string;
  private readonly port: number;
  private readonly minSize: number;
  private readonly maxSize: number;
  private readonly timeout: number;
  private pool: Connection[] = [];
  private available: Connection[] = [];
  private size = 0;

  constructor(
    host: string,
    port: number,
    minSize: number = 2,
    maxSize: number = 10,
    timeout: number = 10000
  ) {
    this.host = host;
    this.port = port;
    this.minSize = minSize;
    this.maxSize = maxSize;
    this.timeout = timeout;
  }

  /** Initialize the connection pool */
  async initialize(): Promise<void> {
    const promises = [];
    for (let i = 0; i < this.minSize; i++) {
      promises.push(this.createConnection());
    }
    await Promise.all(promises);
  }

  /** Create a new connection */
  private async createConnection(): Promise<Connection> {
    const conn = new Connection(this.host, this.port);
    await conn.connect(this.timeout);
    this.pool.push(conn);
    this.available.push(conn);
    this.size++;
    return conn;
  }

  /** Acquire a connection from the pool */
  async acquire(): Promise<Connection> {
    // Try to get an available connection
    if (this.available.length > 0) {
      return this.available.pop()!;
    }

    // Create new connection if under max size
    if (this.size < this.maxSize) {
      return await this.createConnection();
    }

    // Wait for an available connection
    return new Promise((resolve) => {
      const check = setInterval(() => {
        if (this.available.length > 0) {
          clearInterval(check);
          resolve(this.available.pop()!);
        }
      }, 100);
    });
  }

  /** Release a connection back to the pool */
  release(conn: Connection): void {
    if (conn.isConnected()) {
      this.available.push(conn);
    }
  }

  /** Close all connections */
  async close(): Promise<void> {
    await Promise.all(this.pool.map(conn => conn.close()));
    this.pool = [];
    this.available = [];
    this.size = 0;
  }
}

/** Transaction context */
class Transaction {
  private conn: Connection | null = null;
  private committed = false;
  private rolledBack = false;

  constructor(private pool: ConnectionPool) {}

  /** Start the transaction */
  async begin(): Promise<void> {
    this.conn = await this.pool.acquire();
    await this.conn.execute('BEGIN TRANSACTION');
  }

  /** Execute a query within the transaction */
  async execute(query: string, params: QueryParams = []): Promise<QueryResult> {
    if (!this.conn) {
      throw new TransactionError('Transaction not started');
    }
    return await this.conn.execute(query, params);
  }

  /** Commit the transaction */
  async commit(): Promise<void> {
    if (!this.conn) {
      throw new TransactionError('Transaction not started');
    }
    if (this.committed) {
      throw new TransactionError('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new TransactionError('Transaction already rolled back');
    }

    await this.conn.execute('COMMIT');
    this.committed = true;
    this.pool.release(this.conn);
  }

  /** Rollback the transaction */
  async rollback(): Promise<void> {
    if (!this.conn) {
      throw new TransactionError('Transaction not started');
    }
    if (this.committed) {
      throw new TransactionError('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new TransactionError('Transaction already rolled back');
    }

    await this.conn.execute('ROLLBACK');
    this.rolledBack = true;
    this.pool.release(this.conn);
  }
}

/**
 * High-level DriftDB client with connection pooling
 *
 * @example
 * ```typescript
 * const client = await Client.connect('localhost:5432');
 *
 * // Simple query
 * const users = await client.query('SELECT * FROM users WHERE age > ?', [18]);
 *
 * // Time-travel query
 * const historical = await client.queryAtSeq('SELECT * FROM orders', 1000);
 *
 * // Transaction
 * const tx = await client.transaction();
 * await tx.begin();
 * try {
 *   await tx.execute('INSERT INTO users VALUES (?, ?)', [1, 'Alice']);
 *   await tx.commit();
 * } catch (err) {
 *   await tx.rollback();
 *   throw err;
 * }
 *
 * await client.close();
 * ```
 */
export class Client {
  private constructor(private pool: ConnectionPool) {}

  /**
   * Connect to DriftDB server
   *
   * @param address - Server address in format "host:port"
   * @param config - Optional configuration
   * @returns Connected Client instance
   */
  static async connect(
    address: string,
    config?: Partial<ClientConfig>
  ): Promise<Client> {
    const [host, portStr] = address.split(':');
    const port = parseInt(portStr, 10);

    const pool = new ConnectionPool(
      host,
      port,
      config?.minConnections ?? 2,
      config?.maxConnections ?? 10,
      config?.timeout ?? 10000
    );

    await pool.initialize();
    return new Client(pool);
  }

  /**
   * Execute a SQL query
   *
   * @param sql - SQL query string
   * @param params - Query parameters
   * @returns Query result with rows and metadata
   */
  async query(sql: string, params: QueryParams = []): Promise<QueryResult> {
    const conn = await this.pool.acquire();
    try {
      return await conn.execute(sql, params);
    } finally {
      this.pool.release(conn);
    }
  }

  /**
   * Execute a time-travel query at a specific sequence number
   *
   * @param sql - SQL query string
   * @param sequence - Sequence number to query at
   * @param params - Query parameters
   * @returns Query result with historical data
   */
  async queryAtSeq(
    sql: string,
    sequence: number,
    params: QueryParams = []
  ): Promise<QueryResult> {
    const timeTravelSql = `${sql} FOR SYSTEM_TIME AS OF @SEQ:${sequence}`;
    return await this.query(timeTravelSql, params);
  }

  /**
   * Execute a time-travel query at a specific timestamp
   *
   * @param sql - SQL query string
   * @param timestamp - ISO timestamp string or Date object
   * @param params - Query parameters
   * @returns Query result with historical data
   */
  async queryAtTime(
    sql: string,
    timestamp: string | Date,
    params: QueryParams = []
  ): Promise<QueryResult> {
    const ts = timestamp instanceof Date ? timestamp.toISOString() : timestamp;
    const timeTravelSql = `${sql} FOR SYSTEM_TIME AS OF '${ts}'`;
    return await this.query(timeTravelSql, params);
  }

  /**
   * Execute a non-query SQL statement (INSERT, UPDATE, DELETE)
   *
   * @param sql - SQL statement
   * @param params - Statement parameters
   * @returns Number of affected rows
   */
  async execute(sql: string, params: QueryParams = []): Promise<number> {
    const result = await this.query(sql, params);
    return result.rowCount;
  }

  /**
   * Start a new transaction
   *
   * @returns Transaction instance
   */
  transaction(): Transaction {
    return new Transaction(this.pool);
  }

  /**
   * Close all connections
   */
  async close(): Promise<void> {
    await this.pool.close();
  }
}
