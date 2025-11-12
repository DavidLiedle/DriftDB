# DriftDB Load Testing Suite

Comprehensive load testing for DriftDB using [k6](https://k6.io/), a modern open-source load testing tool.

## Overview

This suite tests DriftDB's performance under various workload scenarios:

- **basic-crud.js**: Basic CRUD operations (INSERT, SELECT, UPDATE, DELETE)
- **time-travel.js**: Time-travel query performance (AS OF @seq:N, AS OF timestamp)
- **realistic-workload.js**: Production-like mixed workload with realistic traffic patterns

## Prerequisites

### Install k6

**macOS:**
```bash
brew install k6
```

**Linux:**
```bash
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /usr/share/keyrings/k6-archive-keyring.list
sudo apt-get update
sudo apt-get install k6
```

**Windows:**
```powershell
choco install k6
```

### Start DriftDB Server

Before running load tests, ensure DriftDB server is running:

```bash
# Start server (default port 5432)
cargo run --release -p driftdb-server

# Or with custom configuration
driftdb-server --port 5432 --data-dir ./data
```

## Running Tests

### Basic CRUD Test

Tests fundamental database operations under load.

```bash
cd tests/load
k6 run basic-crud.js
```

**Configuration:**
- Ramp-up: 30s (0 → 10 users)
- Peak: 1 minute (10 → 50 users)
- Sustained: 2 minutes (50 users)
- Ramp-down: 30s (50 → 0 users)

**Expected Performance:**
- p(95) INSERT: < 200ms
- p(95) SELECT: < 100ms
- p(95) UPDATE: < 200ms
- p(95) DELETE: < 150ms

### Time-Travel Query Test

Tests DriftDB's unique time-travel capabilities.

```bash
k6 run time-travel.js
```

**Configuration:**
- Ramp-up: 20s (0 → 5 users)
- Peak: 1 minute (5 → 20 users)
- Sustained: 2 minutes (20 users)
- Ramp-down: 20s (20 → 0 users)

**Expected Performance:**
- p(95) Current Query: < 200ms
- p(95) Time-Travel (Seq): < 800ms
- p(95) Time-Travel (Timestamp): < 800ms

**Overhead:**
- Expected: 200-400% slower than current queries
- Acceptable: < 500% overhead

### Realistic Workload Test

Simulates production traffic patterns.

```bash
k6 run realistic-workload.js
```

**Configuration:**
- Total duration: 16 minutes
- Simulates: Morning ramp-up, peak, lunch dip, afternoon peak, evening wind-down
- Peak load: 80 concurrent users
- Workload mix:
  - 60% reads
  - 25% writes
  - 10% time-travel
  - 5% deletes

**Expected Performance:**
- p(95) Reads: < 300ms
- p(95) Writes: < 600ms
- Success rate: > 99%
- Throughput: Varies by hardware

## Advanced Usage

### Custom Test Parameters

You can override test parameters using environment variables:

```bash
# Custom duration and virtual users
k6 run -e DURATION=5m -e VUS=100 basic-crud.js

# Custom thresholds
k6 run --no-thresholds basic-crud.js

# Generate detailed HTML report
k6 run --out json=results.json basic-crud.js
```

### Running Against Remote Server

Update the server address in scripts (future enhancement):

```javascript
// At top of script
const SERVER_URL = __ENV.SERVER_URL || 'localhost:5432';
```

Then run:
```bash
k6 run -e SERVER_URL=prod-server:5432 basic-crud.js
```

### Integration with CI/CD

Run tests in CI pipeline:

```bash
# Exit with error if thresholds fail
k6 run --quiet basic-crud.js

# Check exit code
if [ $? -eq 0 ]; then
  echo "✅ Load tests passed"
else
  echo "❌ Load tests failed - performance regression detected"
  exit 1
fi
```

### Cloud Execution

Run tests on k6 Cloud for global load testing:

```bash
# Login to k6 Cloud
k6 login cloud

# Run test in cloud
k6 cloud basic-crud.js
```

## Interpreting Results

### Key Metrics

**Response Times:**
- **p(50)**: Median response time - typical user experience
- **p(95)**: 95th percentile - worst case for most users
- **p(99)**: 99th percentile - worst case outliers

**Throughput:**
- **iterations/s**: Number of complete user sessions per second
- **queries/s**: Number of database queries per second

**Success Rate:**
- Target: > 99%
- Warning: 95-99%
- Critical: < 95%

### Understanding Output

Example output:
```
✓ insert successful
  ✓ 100% — 2453 / 2453

✓ insert fast enough
  ✓ 98.5% — 2416 / 2453

checks.........................: 99.25% ✓ 7935      ✗ 60
data_received..................: 2.4 MB 120 kB/s
data_sent......................: 1.8 MB 90 kB/s
http_req_duration..............: avg=142.5ms min=12ms med=98ms max=987ms p(90)=245ms p(95)=312ms
http_reqs......................: 8000   400/s
insert_duration................: avg=89.2ms  min=8ms  med=67ms max=456ms p(90)=145ms p(95)=189ms
iterations.....................: 2000   100/s
success_rate...................: 99.7%  ✓ 7950      ✗ 25
vus............................: 1      min=1       max=50
vus_max........................: 50     min=50      max=50
```

**Analysis:**
- ✅ 99.25% of checks passed (excellent)
- ✅ p(95) = 312ms (within 500ms threshold)
- ✅ 400 requests/second throughput
- ⚠️ 1.5% of inserts exceeded 200ms threshold (investigate)

### Performance Benchmarks

Based on hardware:

**Laptop / Dev Environment:**
- Single core
- Throughput: 100-500 queries/s
- p(95): 200-500ms

**Server / 4 cores:**
- Throughput: 1,000-5,000 queries/s
- p(95): 50-200ms

**Server / 16 cores:**
- Throughput: 5,000-20,000 queries/s
- p(95): 20-100ms

## Troubleshooting

### Connection Refused

**Error:** `dial tcp [::1]:5432: connect: connection refused`

**Solution:** Start DriftDB server before running tests:
```bash
cargo run --release -p driftdb-server
```

### High Error Rate

**Error:** Success rate < 95%

**Possible Causes:**
1. Server overloaded - reduce VUs
2. Connection pool exhausted - increase pool size
3. Disk I/O bottleneck - use SSD storage
4. Memory pressure - increase available RAM

**Debug:**
```bash
# Run with fewer users
k6 run -e VUS=10 basic-crud.js

# Check server logs
tail -f driftdb-server.log
```

### Slow Performance

**Symptom:** p(95) > threshold

**Investigation:**
1. Check CPU usage: `top`
2. Check disk I/O: `iostat -x 1`
3. Check memory: `free -h`
4. Enable query logging in DriftDB
5. Run profiler: `perf record -F 99 -p <pid>`

### Out of Memory

**Error:** `fatal error: runtime: out of memory`

**Solutions:**
1. Reduce data set size in setup()
2. Add cleanup between iterations
3. Increase server memory
4. Enable snapshot compression

## Best Practices

### 1. Baseline First

Always establish baseline performance before optimization:

```bash
# Record baseline
k6 run basic-crud.js > baseline.txt

# After optimization
k6 run basic-crud.js > optimized.txt

# Compare
diff baseline.txt optimized.txt
```

### 2. Incremental Load

Start with low load and increase gradually:

```bash
# Stage 1: Light load
k6 run -e VUS=10 -e DURATION=1m basic-crud.js

# Stage 2: Medium load
k6 run -e VUS=50 -e DURATION=3m basic-crud.js

# Stage 3: Heavy load
k6 run -e VUS=100 -e DURATION=5m basic-crud.js
```

### 3. Monitor Server

Monitor server metrics during tests:

```bash
# Terminal 1: Run test
k6 run realistic-workload.js

# Terminal 2: Monitor server
watch -n 1 'ps aux | grep driftdb-server'

# Terminal 3: Monitor system
htop
```

### 4. Isolate Tests

Run tests in isolation to avoid interference:

```bash
# Fresh server for each test
pkill driftdb-server
cargo run --release -p driftdb-server &
k6 run basic-crud.js
```

## Continuous Performance Testing

### GitHub Actions Integration

Add to `.github/workflows/performance.yml`:

```yaml
name: Performance Tests

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  load-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install k6
        run: |
          sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
          echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
          sudo apt-get update
          sudo apt-get install k6

      - name: Build DriftDB
        run: cargo build --release

      - name: Start DriftDB Server
        run: cargo run --release -p driftdb-server &

      - name: Run Load Tests
        run: |
          k6 run tests/load/basic-crud.js
          k6 run tests/load/time-travel.js

      - name: Upload Results
        uses: actions/upload-artifact@v3
        with:
          name: load-test-results
          path: |
            results.json
```

## Contributing

To add new load tests:

1. Create new test file: `tests/load/my-test.js`
2. Follow k6 best practices
3. Add documentation to this README
4. Set appropriate thresholds
5. Test locally before committing

## Resources

- [k6 Documentation](https://k6.io/docs/)
- [k6 Examples](https://k6.io/docs/examples/)
- [DriftDB Documentation](https://driftdb.io/docs)
- [Performance Testing Best Practices](https://k6.io/docs/testing-guides/)

## License

MIT License - see [LICENSE](../../LICENSE) for details.
