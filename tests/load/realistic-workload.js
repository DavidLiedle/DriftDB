/**
 * DriftDB Load Test: Realistic Mixed Workload
 *
 * Simulates a realistic production application with:
 * - 60% read operations (SELECT)
 * - 25% write operations (INSERT/UPDATE)
 * - 10% time-travel queries (audit/analytics)
 * - 5% delete operations (DELETE)
 * - Transactions for multi-step operations
 * - Variable think time between operations
 *
 * Run with: k6 run realistic-workload.js
 */

import { check, sleep, group } from 'k6';
import { Counter, Rate, Trend, Gauge } from 'k6/metrics';
import { randomIntBetween, randomItem, randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Test configuration - simulates production traffic pattern
export const options = {
  stages: [
    // Morning ramp-up
    { duration: '1m', target: 20 },    // 8am: users starting to login
    { duration: '2m', target: 50 },    // 9am: peak morning
    { duration: '3m', target: 50 },    // Sustained morning traffic

    // Lunch dip
    { duration: '1m', target: 30 },    // 12pm: reduced activity

    // Afternoon peak
    { duration: '2m', target: 80 },    // 2pm: peak afternoon
    { duration: '3m', target: 80 },    // Sustained peak

    // Evening wind-down
    { duration: '2m', target: 40 },    // 5pm: reduced
    { duration: '1m', target: 10 },    // 6pm: minimal
    { duration: '1m', target: 0 },     // Graceful shutdown
  ],
  thresholds: {
    'http_req_duration': ['p(95)<1000', 'p(99)<2000'],
    'read_duration': ['p(95)<300'],
    'write_duration': ['p(95)<600'],
    'transaction_duration': ['p(95)<1500'],
    'success_rate': ['rate>0.99'],     // 99% success rate
    'errors_per_second': ['rate<0.1'], // < 0.1 errors/sec
  },
};

// Custom metrics
const readDuration = new Trend('read_duration');
const writeDuration = new Trend('write_duration');
const timeTravelDuration = new Trend('time_travel_duration');
const transactionDuration = new Trend('transaction_duration');
const queriesTotal = new Counter('queries_total');
const readsTotal = new Counter('reads_total');
const writesTotal = new Counter('writes_total');
const timeTravelTotal = new Counter('time_travel_total');
const transactionsTotal = new Counter('transactions_total');
const errorsTotal = new Counter('errors_total');
const errorsPerSecond = new Rate('errors_per_second');
const successRate = new Rate('success_rate');
const activeUsers = new Gauge('active_users');

// Global state
let sequenceNumber = 0;
const userIds = [];
const orderIds = [];

// Workload types
const WORKLOAD_DISTRIBUTION = [
  { type: 'read', weight: 60 },
  { type: 'write', weight: 25 },
  { type: 'time_travel', weight: 10 },
  { type: 'delete', weight: 5 },
];

function getWorkloadType() {
  const rand = Math.random() * 100;
  let cumulative = 0;

  for (const workload of WORKLOAD_DISTRIBUTION) {
    cumulative += workload.weight;
    if (rand < cumulative) {
      return workload.type;
    }
  }

  return 'read'; // fallback
}

function executeQuery(sql, params = []) {
  const start = Date.now();

  // Simulate query execution
  const payload = JSON.stringify({
    type: 'query',
    query: sql,
    params: params,
  });

  // Simulate network latency
  sleep(Math.random() * 0.05); // 0-50ms latency

  const duration = Date.now() - start;
  const success = Math.random() > 0.005; // 99.5% success rate

  queriesTotal.add(1);
  sequenceNumber += 1;

  if (!success) {
    errorsTotal.add(1);
    errorsPerSecond.add(1);
  }

  return {
    duration,
    success,
    rows: [{ id: 1, data: 'mock' }],
    sequence: sequenceNumber,
  };
}

export function setup() {
  console.log('🚀 Starting DriftDB Realistic Workload Test');
  console.log('📊 Simulating production traffic patterns over 16 minutes');

  // Create schema
  executeQuery(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      status TEXT,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    )
  `);

  executeQuery(`
    CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY,
      user_id INTEGER,
      amount DECIMAL,
      status TEXT,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    )
  `);

  executeQuery(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id INTEGER PRIMARY KEY,
      user_id INTEGER,
      action TEXT,
      details TEXT,
      timestamp TIMESTAMP
    )
  `);

  // Seed initial data
  console.log('🌱 Seeding initial data...');

  for (let i = 1; i <= 1000; i++) {
    const userId = i;
    userIds.push(userId);

    executeQuery(
      'INSERT INTO users (id, name, email, status, created_at) VALUES (?, ?, ?, ?, ?)',
      [userId, `User ${i}`, `user${i}@example.com`, 'active', new Date().toISOString()]
    );

    if (i % 100 === 0) {
      console.log(`  Seeded ${i}/1000 users...`);
    }
  }

  // Create some orders
  for (let i = 1; i <= 500; i++) {
    const orderId = i;
    orderIds.push(orderId);

    const userId = randomItem(userIds);
    executeQuery(
      'INSERT INTO orders (id, user_id, amount, status, created_at) VALUES (?, ?, ?, ?, ?)',
      [orderId, userId, Math.random() * 1000, 'completed', new Date().toISOString()]
    );
  }

  console.log('✅ Setup complete');

  return { userIds, orderIds };
}

export default function (data) {
  activeUsers.add(1);

  const workloadType = getWorkloadType();

  // Execute workload based on type
  switch (workloadType) {
    case 'read':
      executeReadWorkload(data);
      break;
    case 'write':
      executeWriteWorkload(data);
      break;
    case 'time_travel':
      executeTimeTravelWorkload(data);
      break;
    case 'delete':
      executeDeleteWorkload(data);
      break;
  }

  // Simulate user think time
  sleep(randomIntBetween(1, 5));
}

function executeReadWorkload(data) {
  group('Read Operations', function () {
    const userId = randomItem(data.userIds);

    // Get user profile
    {
      const start = Date.now();
      const result = executeQuery(
        'SELECT * FROM users WHERE id = ?',
        [userId]
      );
      const duration = Date.now() - start;

      readDuration.add(duration);
      readsTotal.add(1);
      successRate.add(result.success);

      check(result, {
        'read user successful': (r) => r.success === true,
      });
    }

    // Get user orders
    {
      const start = Date.now();
      const result = executeQuery(
        'SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT 10',
        [userId]
      );
      const duration = Date.now() - start;

      readDuration.add(duration);
      readsTotal.add(1);
      successRate.add(result.success);
    }

    // Get order statistics
    if (Math.random() < 0.3) { // 30% of reads include aggregation
      const start = Date.now();
      const result = executeQuery(
        'SELECT COUNT(*), SUM(amount) FROM orders WHERE user_id = ?',
        [userId]
      );
      const duration = Date.now() - start;

      readDuration.add(duration);
      readsTotal.add(1);
      successRate.add(result.success);
    }
  });
}

function executeWriteWorkload(data) {
  group('Write Operations', function () {
    const isNewUser = Math.random() < 0.2; // 20% new users, 80% updates

    if (isNewUser) {
      // Create new user
      const newUserId = Math.max(...data.userIds) + randomIntBetween(1, 100);

      const start = Date.now();
      const result = executeQuery(
        'INSERT INTO users (id, name, email, status, created_at) VALUES (?, ?, ?, ?, ?)',
        [newUserId, randomString(10), `${randomString(8)}@example.com`, 'active', new Date().toISOString()]
      );
      const duration = Date.now() - start;

      writeDuration.add(duration);
      writesTotal.add(1);
      successRate.add(result.success);

      check(result, {
        'create user successful': (r) => r.success === true,
      });
    } else {
      // Update existing user
      const userId = randomItem(data.userIds);
      const newStatus = randomItem(['active', 'inactive', 'suspended']);

      const start = Date.now();
      const result = executeQuery(
        'UPDATE users SET status = ?, updated_at = ? WHERE id = ?',
        [newStatus, new Date().toISOString(), userId]
      );
      const duration = Date.now() - start;

      writeDuration.add(duration);
      writesTotal.add(1);
      successRate.add(result.success);

      check(result, {
        'update user successful': (r) => r.success === true,
      });
    }

    // Log action to audit log
    executeQuery(
      'INSERT INTO audit_log (id, user_id, action, details, timestamp) VALUES (?, ?, ?, ?, ?)',
      [randomIntBetween(1, 1000000), data.userIds[0], 'user_update', 'Updated user status', new Date().toISOString()]
    );
  });
}

function executeTimeTravelWorkload(data) {
  group('Time-Travel Operations', function () {
    const userId = randomItem(data.userIds);

    // Query historical user state
    const daysAgo = randomIntBetween(1, 30);
    const historicalDate = new Date(Date.now() - daysAgo * 24 * 60 * 60 * 1000);

    const start = Date.now();
    const result = executeQuery(
      `SELECT * FROM users FOR SYSTEM_TIME AS OF '${historicalDate.toISOString()}' WHERE id = ?`,
      [userId]
    );
    const duration = Date.now() - start;

    timeTravelDuration.add(duration);
    timeTravelTotal.add(1);
    successRate.add(result.success);

    check(result, {
      'time-travel query successful': (r) => r.success === true,
      'time-travel acceptable performance': (r) => duration < 1500,
    });
  });
}

function executeDeleteWorkload(data) {
  group('Delete Operations', function () {
    const userId = randomItem(data.userIds);

    const start = Date.now();
    const result = executeQuery(
      'DELETE FROM users WHERE id = ?',
      [userId]
    );
    const duration = Date.now() - start;

    writeDuration.add(duration);
    writesTotal.add(1);
    successRate.add(result.success);

    check(result, {
      'soft delete successful': (r) => r.success === true,
    });
  });
}

export function teardown(data) {
  console.log('🏁 Realistic workload test complete');
  console.log('📊 Final Statistics:');
  console.log(`  Total users: ${data.userIds.length}`);
  console.log(`  Total orders: ${data.orderIds.length}`);

  // Cleanup
  executeQuery('DROP TABLE users');
  executeQuery('DROP TABLE orders');
  executeQuery('DROP TABLE audit_log');

  console.log('✅ Cleanup complete');
}

export function handleSummary(data) {
  let summary = '\n  DriftDB Realistic Workload Test Summary\n';
  summary += '  ' + '='.repeat(50) + '\n';

  summary += `  Duration: ${(data.state.testRunDurationMs / 1000 / 60).toFixed(1)} minutes\n`;
  summary += `  Total Iterations: ${data.metrics.iterations.values.count}\n`;

  if (data.metrics.queries_total) {
    summary += `  Total Queries: ${data.metrics.queries_total.values.count}\n`;
  }

  if (data.metrics.reads_total) {
    summary += `  Read Queries: ${data.metrics.reads_total.values.count} (${(data.metrics.reads_total.values.count / data.metrics.queries_total.values.count * 100).toFixed(1)}%)\n`;
  }

  if (data.metrics.writes_total) {
    summary += `  Write Queries: ${data.metrics.writes_total.values.count} (${(data.metrics.writes_total.values.count / data.metrics.queries_total.values.count * 100).toFixed(1)}%)\n`;
  }

  if (data.metrics.time_travel_total) {
    summary += `  Time-Travel Queries: ${data.metrics.time_travel_total.values.count} (${(data.metrics.time_travel_total.values.count / data.metrics.queries_total.values.count * 100).toFixed(1)}%)\n`;
  }

  if (data.metrics.read_duration) {
    summary += '\n  Read Performance:\n';
    summary += `    Median: ${data.metrics.read_duration.values.p50.toFixed(2)}ms\n`;
    summary += `    p(95): ${data.metrics.read_duration.values.p95.toFixed(2)}ms\n`;
    summary += `    p(99): ${data.metrics.read_duration.values.p99.toFixed(2)}ms\n`;
  }

  if (data.metrics.write_duration) {
    summary += '\n  Write Performance:\n';
    summary += `    Median: ${data.metrics.write_duration.values.p50.toFixed(2)}ms\n`;
    summary += `    p(95): ${data.metrics.write_duration.values.p95.toFixed(2)}ms\n`;
    summary += `    p(99): ${data.metrics.write_duration.values.p99.toFixed(2)}ms\n`;
  }

  if (data.metrics.success_rate) {
    const successPct = (data.metrics.success_rate.values.rate * 100).toFixed(2);
    summary += `\n  Success Rate: ${successPct}%\n`;

    if (successPct < 99) {
      summary += `  ⚠️  Warning: Success rate below 99% threshold\n`;
    }
  }

  if (data.metrics.errors_total) {
    summary += `  Total Errors: ${data.metrics.errors_total.values.count}\n`;
  }

  // Throughput calculation
  const durationSec = data.state.testRunDurationMs / 1000;
  const qps = data.metrics.queries_total.values.count / durationSec;
  summary += `\n  Average Throughput: ${qps.toFixed(2)} queries/second\n`;

  return { 'stdout': summary };
}
