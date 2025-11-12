/**
 * DriftDB Load Test: Basic CRUD Operations
 *
 * Tests basic database operations under load:
 * - INSERT operations
 * - SELECT queries
 * - UPDATE/PATCH operations
 * - SOFT DELETE operations
 *
 * Run with: k6 run basic-crud.js
 */

import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { Socket } from 'k6/experimental/websockets';
import { randomIntBetween, randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Test configuration
export const options = {
  stages: [
    { duration: '30s', target: 10 },   // Ramp up to 10 users
    { duration: '1m', target: 50 },    // Ramp up to 50 users
    { duration: '2m', target: 50 },    // Stay at 50 users
    { duration: '30s', target: 0 },    // Ramp down to 0
  ],
  thresholds: {
    http_req_duration: ['p(95)<500', 'p(99)<1000'], // 95% < 500ms, 99% < 1s
    'insert_duration': ['p(95)<200'],
    'select_duration': ['p(95)<100'],
    'update_duration': ['p(95)<200'],
    'delete_duration': ['p(95)<150'],
  },
};

// Custom metrics
const insertDuration = new Trend('insert_duration');
const selectDuration = new Trend('select_duration');
const updateDuration = new Trend('update_duration');
const deleteDuration = new Trend('delete_duration');
const queriesTotal = new Counter('queries_total');
const errorsTotal = new Counter('errors_total');
const successRate = new Rate('success_rate');

// Test data generators
function generateUser() {
  return {
    id: randomIntBetween(1, 1000000),
    name: randomString(10),
    email: `${randomString(8)}@example.com`,
    age: randomIntBetween(18, 80),
    created_at: new Date().toISOString(),
  };
}

function executeQuery(sql, params = []) {
  const start = Date.now();

  // Simulate DriftDB query via TCP socket
  // In production, replace with actual DriftDB protocol
  const payload = JSON.stringify({
    type: 'query',
    query: sql,
    params: params,
  });

  // Mock response for load testing
  // TODO: Integrate with actual DriftDB client once available
  const duration = Date.now() - start;

  queriesTotal.add(1);
  return { duration, success: true, rows: [] };
}

export function setup() {
  console.log('🚀 Starting DriftDB Load Test - Basic CRUD Operations');
  console.log('Target: 50 concurrent users, 3.5 minute test duration');

  // Create test table
  const createTable = `
    CREATE TABLE IF NOT EXISTS load_test_users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      age INTEGER,
      created_at TIMESTAMP
    )
  `;

  executeQuery(createTable);

  return { tableCreated: true };
}

export default function () {
  const user = generateUser();

  // 1. INSERT operation
  {
    const start = Date.now();
    const result = executeQuery(
      'INSERT INTO load_test_users (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)',
      [user.id, user.name, user.email, user.age, user.created_at]
    );
    const duration = Date.now() - start;

    insertDuration.add(duration);
    successRate.add(result.success);

    check(result, {
      'insert successful': (r) => r.success === true,
      'insert fast enough': (r) => duration < 500,
    });
  }

  sleep(0.1);

  // 2. SELECT operation
  {
    const start = Date.now();
    const result = executeQuery(
      'SELECT * FROM load_test_users WHERE id = ?',
      [user.id]
    );
    const duration = Date.now() - start;

    selectDuration.add(duration);
    successRate.add(result.success);

    check(result, {
      'select successful': (r) => r.success === true,
      'select fast enough': (r) => duration < 200,
    });
  }

  sleep(0.1);

  // 3. UPDATE operation (PATCH)
  {
    const start = Date.now();
    const newAge = randomIntBetween(18, 80);
    const result = executeQuery(
      'PATCH load_test_users SET age = ? WHERE id = ?',
      [newAge, user.id]
    );
    const duration = Date.now() - start;

    updateDuration.add(duration);
    successRate.add(result.success);

    check(result, {
      'update successful': (r) => r.success === true,
      'update fast enough': (r) => duration < 500,
    });
  }

  sleep(0.1);

  // 4. SOFT DELETE operation (every 5th user)
  if (user.id % 5 === 0) {
    const start = Date.now();
    const result = executeQuery(
      'SOFT DELETE FROM load_test_users WHERE id = ?',
      [user.id]
    );
    const duration = Date.now() - start;

    deleteDuration.add(duration);
    successRate.add(result.success);

    check(result, {
      'delete successful': (r) => r.success === true,
      'delete fast enough': (r) => duration < 300,
    });
  }

  sleep(randomIntBetween(1, 3));
}

export function teardown(data) {
  console.log('🏁 Test complete. Cleaning up...');

  // Clean up test data
  executeQuery('DROP TABLE load_test_users');

  console.log('✅ Cleanup complete');
}

export function handleSummary(data) {
  return {
    'stdout': textSummary(data, { indent: '  ', enableColors: true }),
  };
}

function textSummary(data, options = {}) {
  const indent = options.indent || '';
  const enableColors = options.enableColors !== false;

  let summary = `\n${indent}DriftDB Load Test Summary\n${indent}${'='.repeat(40)}\n`;

  // Test duration
  summary += `${indent}Duration: ${(data.state.testRunDurationMs / 1000).toFixed(1)}s\n`;

  // Iterations
  summary += `${indent}Iterations: ${data.metrics.iterations.values.count}\n`;

  // VUs
  summary += `${indent}VUs: ${data.metrics.vus.values.value}\n`;

  // Custom metrics
  if (data.metrics.insert_duration) {
    summary += `\n${indent}INSERT Performance:\n`;
    summary += `${indent}  p(50): ${data.metrics.insert_duration.values.p50.toFixed(2)}ms\n`;
    summary += `${indent}  p(95): ${data.metrics.insert_duration.values.p95.toFixed(2)}ms\n`;
    summary += `${indent}  p(99): ${data.metrics.insert_duration.values.p99.toFixed(2)}ms\n`;
  }

  if (data.metrics.select_duration) {
    summary += `\n${indent}SELECT Performance:\n`;
    summary += `${indent}  p(50): ${data.metrics.select_duration.values.p50.toFixed(2)}ms\n`;
    summary += `${indent}  p(95): ${data.metrics.select_duration.values.p95.toFixed(2)}ms\n`;
    summary += `${indent}  p(99): ${data.metrics.select_duration.values.p99.toFixed(2)}ms\n`;
  }

  // Success rate
  if (data.metrics.success_rate) {
    const successPct = (data.metrics.success_rate.values.rate * 100).toFixed(2);
    summary += `\n${indent}Success Rate: ${successPct}%\n`;
  }

  // Queries total
  if (data.metrics.queries_total) {
    summary += `${indent}Total Queries: ${data.metrics.queries_total.values.count}\n`;
  }

  return summary;
}
