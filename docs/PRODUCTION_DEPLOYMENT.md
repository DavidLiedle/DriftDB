# DriftDB Production Deployment Guide

## 🚀 Production Readiness Checklist

### ✅ Core Features
- [x] PostgreSQL Wire Protocol compatibility
- [x] ACID transactions (BEGIN, COMMIT)
- [x] TLS/SSL encryption with PostgreSQL SSL negotiation
- [x] Structured error handling and recovery
- [x] Performance monitoring and optimization
- [x] Advanced connection pooling
- [x] Rate limiting and DDoS protection
- [x] Production monitoring endpoints
- [ ] Complete ROLLBACK implementation (partial)
- [ ] Column ordering consistency (known issue)

## 📦 Installation

### From Source
```bash
# Clone repository
git clone https://github.com/your-org/DriftDB.git
cd DriftDB/driftdb

# Build release version
cargo build --release

# Binary will be at: target/release/driftdb-server
```

### Docker
```bash
# Build Docker image
docker build -t driftdb:latest .

# Run container
docker run -p 5433:5433 -p 8080:8080 \
  -v /data/driftdb:/data \
  driftdb:latest
```

## ⚙️ Configuration

### Environment Variables
```bash
# Core Settings
DRIFTDB_DATA_PATH=/var/lib/driftdb/data
DRIFTDB_LISTEN=0.0.0.0:5433
DRIFTDB_HTTP_LISTEN=0.0.0.0:8080

# Connection Pool
DRIFTDB_MAX_CONNECTIONS=100
DRIFTDB_MIN_IDLE_CONNECTIONS=10
DRIFTDB_CONNECTION_TIMEOUT=30
DRIFTDB_IDLE_TIMEOUT=600

# TLS/SSL
DRIFTDB_TLS_ENABLED=true
DRIFTDB_TLS_CERT_PATH=/etc/driftdb/tls/server.crt
DRIFTDB_TLS_KEY_PATH=/etc/driftdb/tls/server.key
DRIFTDB_TLS_REQUIRED=false

# Security
DRIFTDB_AUTH_METHOD=scram-sha-256
DRIFTDB_REQUIRE_AUTH=true
DRIFTDB_MAX_AUTH_ATTEMPTS=3
DRIFTDB_AUTH_LOCKOUT_DURATION=300

# Rate Limiting
DRIFTDB_RATE_LIMIT_CONNECTIONS=30
DRIFTDB_RATE_LIMIT_QUERIES=100
DRIFTDB_RATE_LIMIT_BURST_SIZE=1000
DRIFTDB_RATE_LIMIT_GLOBAL=10000
DRIFTDB_RATE_LIMIT_ADAPTIVE=true

# Performance
DRIFTDB_PERFORMANCE_MONITORING=true
DRIFTDB_MAX_CONCURRENT_REQUESTS=10000
DRIFTDB_QUERY_CACHE_SIZE=1000
```

### Command Line Arguments
```bash
driftdb-server \
  --data-path /var/lib/driftdb/data \
  --listen 0.0.0.0:5433 \
  --http-listen 0.0.0.0:8080 \
  --max-connections 100 \
  --tls-enabled \
  --tls-cert-path /etc/tls/server.crt \
  --tls-key-path /etc/tls/server.key
```

## 🔒 Security

### TLS/SSL Setup
```bash
# Generate self-signed certificate (for testing)
openssl req -x509 -newkey rsa:4096 \
  -keyout server.key -out server.crt \
  -days 365 -nodes

# Production: Use certificates from trusted CA
cp /path/to/ca-cert.crt /etc/driftdb/tls/
cp /path/to/server.crt /etc/driftdb/tls/
cp /path/to/server.key /etc/driftdb/tls/

# Set proper permissions
chmod 600 /etc/driftdb/tls/server.key
chmod 644 /etc/driftdb/tls/server.crt
```

### Authentication Methods
- **trust**: No authentication (development only)
- **md5**: MD5 password authentication
- **scram-sha-256**: SCRAM-SHA-256 (recommended)

## 📊 Monitoring

### Health Check Endpoints
```bash
# Liveness probe
curl http://localhost:8080/health/live

# Readiness probe
curl http://localhost:8080/health/ready

# Startup probe
curl http://localhost:8080/health/startup
```

### Performance Monitoring
```bash
# Overall performance
curl http://localhost:8080/performance

# Query performance
curl http://localhost:8080/performance/queries

# Connection pool
curl http://localhost:8080/performance/connections

# Memory usage
curl http://localhost:8080/performance/memory

# Optimization suggestions
curl http://localhost:8080/performance/optimization
```

### Advanced Pool Analytics
```bash
# Comprehensive analytics
curl http://localhost:8080/pool/analytics

# Connection affinity
curl http://localhost:8080/pool/affinity

# Health metrics
curl http://localhost:8080/pool/health

# Load balancing
curl http://localhost:8080/pool/loadbalancing

# Resource usage
curl http://localhost:8080/pool/resources
```

### Prometheus Metrics
```bash
# Metrics endpoint
curl http://localhost:8080/metrics
```

## 🎯 Performance Tuning

### Connection Pool
```bash
# For high-traffic applications
DRIFTDB_MAX_CONNECTIONS=500
DRIFTDB_MIN_IDLE_CONNECTIONS=50

# For low-traffic applications
DRIFTDB_MAX_CONNECTIONS=50
DRIFTDB_MIN_IDLE_CONNECTIONS=5
```

### Query Optimization
- Enable query caching: `DRIFTDB_QUERY_CACHE_SIZE=5000`
- Monitor slow queries via `/performance/queries`
- Review optimization suggestions at `/performance/optimization`

### Resource Limits
```bash
# System limits (Linux)
ulimit -n 65535  # File descriptors
ulimit -u 32768  # Processes/threads

# systemd service limits
[Service]
LimitNOFILE=65535
LimitNPROC=32768
```

## 🔄 High Availability

### Load Balancing
```nginx
upstream driftdb {
    least_conn;
    server db1.example.com:5433 weight=1;
    server db2.example.com:5433 weight=1;
    server db3.example.com:5433 weight=1;
}
```

### Health Checks
```yaml
# Kubernetes readiness probe
readinessProbe:
  httpGet:
    path: /health/ready
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5

# Kubernetes liveness probe
livenessProbe:
  httpGet:
    path: /health/live
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
```

## 🚨 Troubleshooting

### Common Issues

#### High Memory Usage
```bash
# Check memory stats
curl http://localhost:8080/performance/memory

# Force garbage collection (if needed)
curl -X POST http://localhost:8080/admin/gc
```

#### Connection Pool Exhaustion
```bash
# Check pool stats
curl http://localhost:8080/pool/analytics

# Increase pool size
export DRIFTDB_MAX_CONNECTIONS=200
```

#### Slow Queries
```bash
# Identify slow queries
curl http://localhost:8080/performance/queries

# Get optimization suggestions
curl http://localhost:8080/performance/optimization
```

### Logging
```bash
# Enable debug logging
export RUST_LOG=driftdb_server=debug

# Enable tracing
export RUST_LOG=driftdb_server=trace

# Log to file
driftdb-server 2>&1 | tee /var/log/driftdb/server.log
```

## 📈 Capacity Planning

### Sizing Guidelines
| Workload | Connections | Memory | CPU | Storage |
|----------|------------|--------|-----|---------|
| Small    | 50         | 2GB    | 2   | 100GB   |
| Medium   | 200        | 8GB    | 4   | 500GB   |
| Large    | 500        | 16GB   | 8   | 2TB     |
| X-Large  | 1000       | 32GB   | 16  | 10TB    |

### Monitoring Metrics
- Query throughput: `/metrics` → `driftdb_queries_total`
- Connection usage: `/pool/analytics` → `active_connections`
- Memory pressure: `/performance/memory` → `heap_used_mb`
- Error rates: `/metrics` → `driftdb_errors_total`

## 🔧 Maintenance

### Backup
```bash
# Stop writes (if possible)
# Copy data directory
tar -czf backup.tar.gz /var/lib/driftdb/data/

# Or use rsync for incremental
rsync -av /var/lib/driftdb/data/ /backup/driftdb/
```

### Updates
```bash
# Rolling update (with multiple instances)
1. Update one instance
2. Verify health: curl http://instance:8080/health/ready
3. Repeat for other instances
```

## ⚠️ Known Limitations

1. **ROLLBACK**: Partial implementation - transactions can BEGIN and COMMIT but ROLLBACK is not fully functional
2. **Column Ordering**: HashMap-based storage may return columns in different order than defined
3. **Replication**: Not yet implemented
4. **Hot Backup**: Requires stopping writes

## 📞 Support

- GitHub Issues: https://github.com/your-org/DriftDB/issues
- Documentation: https://docs.driftdb.io
- Community: https://discord.gg/driftdb

## 📄 License

See LICENSE file in the repository root.