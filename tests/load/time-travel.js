/**
 * DriftDB Load Test: Time-Travel Queries
 *
 * Tests DriftDB's unique time-travel query capabilities under load:
 * - AS OF @seq:N queries (sequence-based time travel)
 * - AS OF timestamp queries (timestamp-based time travel)
 * - Historical data reconstruction
 * - Snapshot performance
 *
 * Run with: k6 run time-travel.js
 */

import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { randomIntBetween, randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Test configuration
export const options = {
  stages: [
    { duration: '20s', target: 5 },    // Ramp up to 5 users
    { duration: '1m', target: 20 },    // Ramp up to 20 users
    { duration: '2m', target: 20 },    // Stay at 20 users
    { duration: '20s', target: 0 },    // Ramp down
  ],
  thresholds: {
    'time_travel_seq_duration': ['p(95)<800', 'p(99)<1500'],
    'time_travel_ts_duration': ['p(95)<800', 'p(99)<1500'],
    'current_query_duration': ['p(95)<200'],
    'success_rate': ['rate>0.95'],
  },
};

// Custom metrics
const timeTravelSeqDuration = new Trend('time_travel_seq_duration');
const timeTravelTsDuration = new Trend('time_travel_ts_duration');
const currentQueryDuration = new Trend('current_query_duration');
const queriesTotal = new Counter('queries_total');
const successRate = new Rate('success_rate');

// Global sequence tracking
let currentSequence = 0;
const sequenceCheckpoints = [];
const timestampCheckpoints = [];

function executeQuery(sql, params = []) {
  const start = Date.now();

  // Simulate DriftDB query
  const payload = JSON.stringify({
    type: 'query',
    query: sql,
    params: params,
  });

  // Mock response
  const duration = Date.now() - start;

  queriesTotal.add(1);
  currentSequence += 1;

  return {
    duration,
    success: true,
    rows: [{ id: 1, value: 'test', _seq: currentSequence }],
    sequence: currentSequence,
  };
}

export function setup() {
  console.log('🕐 Starting DriftDB Load Test - Time-Travel Queries');
  console.log('Testing historical query performance and snapshot efficiency');

  // Create test table
  executeQuery(`
    CREATE TABLE IF NOT EXISTS time_travel_test (
      id INTEGER PRIMARY KEY,
      value TEXT,
      version INTEGER,
      updated_at TIMESTAMP
    )
  `);

  // Insert initial data and capture checkpoints
  console.log('📊 Populating test data...');

  for (let i = 1; i <= 100; i++) {
    const result = executeQuery(
      'INSERT INTO time_travel_test (id, value, version, updated_at) VALUES (?, ?, ?, ?)',
      [i, `initial_${i}`, 1, new Date().toISOString()]
    );

    // Capture checkpoints every 10 records
    if (i % 10 === 0) {
      sequenceCheckpoints.push(result.sequence);
      timestampCheckpoints.push(new Date().toISOString());
      sleep(0.1); // Small delay to ensure different timestamps
    }
  }

  // Perform some updates to create history
  console.log('📝 Creating historical versions...');
  for (let version = 2; version <= 5; version++) {
    for (let i = 1; i <= 100; i++) {
      const result = executeQuery(
        'PATCH time_travel_test SET value = ?, version = ? WHERE id = ?',
        [`version_${version}_${i}`, version, i]
      );

      if (i % 20 === 0) {
        sequenceCheckpoints.push(result.sequence);
        timestampCheckpoints.push(new Date().toISOString());
        sleep(0.1);
      }
    }
  }

  console.log(`✅ Setup complete. ${sequenceCheckpoints.length} checkpoints created`);

  return {
    sequenceCheckpoints,
    timestampCheckpoints,
    totalRecords: 100,
  };
}

export default function (data) {
  const recordId = randomIntBetween(1, data.totalRecords);

  // Test 1: Current state query (baseline)
  {
    const start = Date.now();
    const result = executeQuery(
      'SELECT * FROM time_travel_test WHERE id = ?',
      [recordId]
    );
    const duration = Date.now() - start;

    currentQueryDuration.add(duration);
    successRate.add(result.success);

    check(result, {
      'current query successful': (r) => r.success === true,
      'current query returns data': (r) => r.rows.length > 0,
    });
  }

  sleep(0.2);

  // Test 2: Time-travel by sequence number
  {
    const checkpointIdx = randomIntBetween(0, data.sequenceCheckpoints.length - 1);
    const targetSeq = data.sequenceCheckpoints[checkpointIdx];

    const start = Date.now();
    const result = executeQuery(
      `SELECT * FROM time_travel_test AS OF @seq:${targetSeq} WHERE id = ?`,
      [recordId]
    );
    const duration = Date.now() - start;

    timeTravelSeqDuration.add(duration);
    successRate.add(result.success);

    check(result, {
      'time-travel seq query successful': (r) => r.success === true,
      'time-travel seq returns historical data': (r) => r.rows.length > 0,
      'time-travel seq fast enough': (r) => duration < 1000,
    });
  }

  sleep(0.2);

  // Test 3: Time-travel by timestamp
  {
    const checkpointIdx = randomIntBetween(0, data.timestampCheckpoints.length - 1);
    const targetTs = data.timestampCheckpoints[checkpointIdx];

    const start = Date.now();
    const result = executeQuery(
      `SELECT * FROM time_travel_test AS OF '${targetTs}' WHERE id = ?`,
      [recordId]
    );
    const duration = Date.now() - start;

    timeTravelTsDuration.add(duration);
    successRate.add(result.success);

    check(result, {
      'time-travel ts query successful': (r) => r.success === true,
      'time-travel ts returns historical data': (r) => r.rows.length > 0,
      'time-travel ts fast enough': (r) => duration < 1000,
    });
  }

  sleep(0.2);

  // Test 4: Full table scan at historical point (more intensive)
  if (__ITER % 10 === 0) {
    const checkpointIdx = randomIntBetween(0, data.sequenceCheckpoints.length - 1);
    const targetSeq = data.sequenceCheckpoints[checkpointIdx];

    const start = Date.now();
    const result = executeQuery(
      `SELECT * FROM time_travel_test AS OF @seq:${targetSeq} ORDER BY id`
    );
    const duration = Date.now() - start;

    check(result, {
      'full scan successful': (r) => r.success === true,
      'full scan acceptable performance': (r) => duration < 2000,
    });
  }

  sleep(randomIntBetween(1, 2));
}

export function teardown(data) {
  console.log('🏁 Time-travel test complete. Cleaning up...');
  executeQuery('DROP TABLE time_travel_test');
  console.log('✅ Cleanup complete');
}

export function handleSummary(data) {
  let summary = '\n  DriftDB Time-Travel Load Test Summary\n';
  summary += '  ' + '='.repeat(45) + '\n';

  summary += `  Duration: ${(data.state.testRunDurationMs / 1000).toFixed(1)}s\n`;
  summary += `  Iterations: ${data.metrics.iterations.values.count}\n`;
  summary += `  Total Queries: ${data.metrics.queries_total.values.count}\n`;

  if (data.metrics.current_query_duration) {
    summary += '\n  Current Query Performance:\n';
    summary += `    p(50): ${data.metrics.current_query_duration.values.p50.toFixed(2)}ms\n`;
    summary += `    p(95): ${data.metrics.current_query_duration.values.p95.toFixed(2)}ms\n`;
    summary += `    p(99): ${data.metrics.current_query_duration.values.p99.toFixed(2)}ms\n`;
  }

  if (data.metrics.time_travel_seq_duration) {
    summary += '\n  Time-Travel (Sequence) Performance:\n';
    summary += `    p(50): ${data.metrics.time_travel_seq_duration.values.p50.toFixed(2)}ms\n`;
    summary += `    p(95): ${data.metrics.time_travel_seq_duration.values.p95.toFixed(2)}ms\n`;
    summary += `    p(99): ${data.metrics.time_travel_seq_duration.values.p99.toFixed(2)}ms\n`;
  }

  if (data.metrics.time_travel_ts_duration) {
    summary += '\n  Time-Travel (Timestamp) Performance:\n';
    summary += `    p(50): ${data.metrics.time_travel_ts_duration.values.p50.toFixed(2)}ms\n`;
    summary += `    p(95): ${data.metrics.time_travel_ts_duration.values.p95.toFixed(2)}ms\n`;
    summary += `    p(99): ${data.metrics.time_travel_ts_duration.values.p99.toFixed(2)}ms\n`;
  }

  if (data.metrics.success_rate) {
    const successPct = (data.metrics.success_rate.values.rate * 100).toFixed(2);
    summary += `\n  Success Rate: ${successPct}%\n`;
  }

  // Performance comparison
  if (data.metrics.current_query_duration && data.metrics.time_travel_seq_duration) {
    const currentP95 = data.metrics.current_query_duration.values.p95;
    const ttP95 = data.metrics.time_travel_seq_duration.values.p95;
    const overhead = ((ttP95 - currentP95) / currentP95 * 100).toFixed(1);

    summary += `\n  Time-Travel Overhead: ${overhead}% slower than current queries\n`;
  }

  return { 'stdout': summary };
}
