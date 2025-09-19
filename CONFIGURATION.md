# DriftDB Configuration Guide

## Database Configuration

DriftDB can be configured through environment variables, configuration files, or command-line arguments.

### Configuration File (driftdb.toml)

```toml
[database]
data_dir = "/var/lib/driftdb/data"
wal_dir = "/var/lib/driftdb/wal"
max_segment_size = 134217728  # 128MB
snapshot_threshold = 100000    # Create snapshot every 100k events

[connection]
min_connections = 10
max_connections = 100
connection_timeout_ms = 5000
idle_timeout_ms = 300000
health_check_interval_ms = 30000

[rate_limiting]
enabled = true
requests_per_second = 1000
max_concurrent_per_client = 10
max_queue_size = 1000

[transaction]
default_isolation_level = "repeatable_read"  # read_uncommitted, read_committed, repeatable_read, serializable
max_transaction_duration_ms = 30000
deadlock_detection_enabled = true

[encryption]
encrypt_at_rest = true
encrypt_in_transit = true
cipher_suite = "aes_256_gcm"  # aes_256_gcm, chacha20_poly1305
key_rotation_days = 30

[tls]
enabled = true
cert_path = "/etc/driftdb/certs/server.crt"
key_path = "/etc/driftdb/certs/server.key"
ca_path = "/etc/driftdb/certs/ca.crt"
min_version = "tls1.3"

[backup]
enabled = true
schedule = "0 2 * * *"  # Daily at 2 AM
retention_days = 30
compression = "zstd"
incremental_enabled = true

[monitoring]
metrics_enabled = true
metrics_port = 9090
tracing_enabled = true
tracing_level = "info"  # trace, debug, info, warn, error
health_check_port = 8080

[migration]
auto_migrate = false
migration_dir = "/etc/driftdb/migrations"
dry_run = false

[performance]
query_cache_size = 1000
query_cache_ttl_ms = 60000
index_cache_size_mb = 512
streaming_buffer_size = 10000
max_state_memory_mb = 512
```

## Environment Variables

All configuration options can be overridden using environment variables:

```bash
# Database settings
export DRIFTDB_DATA_DIR="/var/lib/driftdb/data"
export DRIFTDB_WAL_DIR="/var/lib/driftdb/wal"
export DRIFTDB_MAX_SEGMENT_SIZE="134217728"

# Connection settings
export DRIFTDB_MAX_CONNECTIONS="100"
export DRIFTDB_CONNECTION_TIMEOUT_MS="5000"

# Security settings
export DRIFTDB_ENCRYPT_AT_REST="true"
export DRIFTDB_TLS_CERT_PATH="/etc/driftdb/certs/server.crt"
export DRIFTDB_TLS_KEY_PATH="/etc/driftdb/certs/server.key"

# Performance settings
export DRIFTDB_QUERY_CACHE_SIZE="1000"
export DRIFTDB_INDEX_CACHE_SIZE_MB="512"
```

## Command-Line Arguments

```bash
# Start with custom configuration
driftdb server \
  --config /etc/driftdb/config.toml \
  --data-dir /var/lib/driftdb/data \
  --port 5432 \
  --max-connections 200

# Override specific settings
driftdb server \
  --config /etc/driftdb/config.toml \
  --encrypt-at-rest \
  --tls-enabled \
  --metrics-port 9090
```

## Connection String

Clients can connect using a connection string:

```
driftdb://username:password@hostname:port/database?options
```

Examples:
```bash
# Basic connection
driftdb://localhost:5432/mydb

# With TLS
driftdb://localhost:5432/mydb?sslmode=require

# With connection pool settings
driftdb://localhost:5432/mydb?pool_size=50&timeout=10000

# With transaction settings
driftdb://localhost:5432/mydb?isolation=serializable&autocommit=false
```

## Performance Tuning

### Memory Configuration

```toml
[memory]
# Maximum memory for query execution
query_memory_limit_mb = 1024

# Cache sizes
page_cache_size_mb = 2048
index_cache_size_mb = 512
query_plan_cache_size = 1000

# Streaming buffers
event_buffer_size = 10000
max_state_memory_mb = 512
```

### I/O Configuration

```toml
[io]
# Segment rotation
max_segment_size = 134217728  # 128MB
segment_rotation_check_ms = 1000

# WAL settings
wal_sync_method = "fsync"  # fsync, fdatasync, none
wal_rotation_size = 104857600  # 100MB
wal_compression = true

# Snapshot settings
snapshot_interval_events = 100000
snapshot_interval_bytes = 134217728
snapshot_compression_level = 3  # 1-22 for zstd
```

### Query Optimization

```toml
[optimizer]
enable_cost_based_optimizer = true
enable_plan_cache = true
collect_statistics = true
statistics_sample_rate = 0.1  # Sample 10% of data

# Cost model tuning
seq_page_cost = 1.0
random_page_cost = 4.0
cpu_tuple_cost = 0.01
cpu_operator_cost = 0.005
```

## Security Configuration

### Encryption Keys

```toml
[encryption.keys]
# Master key management
master_key_provider = "file"  # file, hsm, kms
master_key_path = "/etc/driftdb/keys/master.key"

# Key rotation
auto_rotate = true
rotation_interval_days = 30
retain_old_keys_days = 90
```

### Access Control

```toml
[security]
# Authentication
auth_method = "password"  # password, certificate, ldap
password_min_length = 12
password_require_special = true

# Authorization
enable_rbac = true
default_role = "reader"

# Audit
audit_enabled = true
audit_log_path = "/var/log/driftdb/audit.log"
audit_log_rotation = "daily"
```

### Network Security

```toml
[network]
# Bind addresses
bind_address = "0.0.0.0"
bind_port = 5432

# Allowed clients
allow_list = ["192.168.1.0/24", "10.0.0.0/8"]
deny_list = []

# Connection limits
max_connections_per_ip = 10
connection_rate_limit_per_ip = 100  # per second
```

## Monitoring Configuration

### Metrics

```toml
[metrics]
enabled = true
export_format = "prometheus"  # prometheus, json
export_interval_ms = 10000
include_histograms = true

# Metric filters
include_metrics = ["*"]
exclude_metrics = ["debug.*"]
```

### Logging

```toml
[logging]
level = "info"  # trace, debug, info, warn, error
format = "json"  # json, pretty, compact
output = "stdout"  # stdout, file
file_path = "/var/log/driftdb/driftdb.log"
rotation = "daily"  # daily, size, never
max_size_mb = 100
max_age_days = 30
max_backups = 10
```

### Health Checks

```toml
[health]
enabled = true
port = 8080
path = "/health"

# Checks to perform
check_disk_space = true
check_memory = true
check_wal = true
check_connections = true

# Thresholds
disk_space_warning_gb = 5
disk_space_critical_gb = 1
memory_usage_warning_percent = 80
memory_usage_critical_percent = 95
```

## Deployment Examples

### Docker

```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/target/release/driftdb /usr/local/bin/
COPY config.toml /etc/driftdb/

EXPOSE 5432 9090 8080
CMD ["driftdb", "server", "--config", "/etc/driftdb/config.toml"]
```

### Docker Compose

```yaml
version: '3.8'
services:
  driftdb:
    image: driftdb:latest
    ports:
      - "5432:5432"    # Database
      - "9090:9090"    # Metrics
      - "8080:8080"    # Health
    volumes:
      - ./data:/var/lib/driftdb/data
      - ./config:/etc/driftdb
      - ./certs:/etc/driftdb/certs
    environment:
      - DRIFTDB_ENCRYPT_AT_REST=true
      - DRIFTDB_MAX_CONNECTIONS=200
    restart: unless-stopped
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: driftdb
spec:
  serviceName: driftdb
  replicas: 1
  template:
    spec:
      containers:
      - name: driftdb
        image: driftdb:latest
        ports:
        - containerPort: 5432
        - containerPort: 9090
        - containerPort: 8080
        volumeMounts:
        - name: data
          mountPath: /var/lib/driftdb/data
        - name: config
          mountPath: /etc/driftdb
        env:
        - name: DRIFTDB_ENCRYPT_AT_REST
          value: "true"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
```

## Best Practices

### Production Settings

For production deployments, ensure these settings:

```toml
[production]
# Security
encrypt_at_rest = true
encrypt_in_transit = true
tls.enabled = true
tls.min_version = "tls1.3"

# Durability
wal_sync_method = "fsync"
backup.enabled = true
backup.incremental_enabled = true

# Monitoring
metrics_enabled = true
tracing_enabled = true
health.enabled = true
audit_enabled = true

# Performance
query_cache_size = 10000
index_cache_size_mb = 2048
max_connections = 500

# Reliability
transaction.deadlock_detection_enabled = true
connection.health_check_interval_ms = 30000
migration.dry_run = true  # Test migrations first
```

### Development Settings

For development environments:

```toml
[development]
# Relaxed security
encrypt_at_rest = false
tls.enabled = false

# Verbose logging
logging.level = "debug"
tracing_level = "debug"

# Smaller resource limits
max_connections = 50
query_cache_size = 100
index_cache_size_mb = 128

# Faster feedback
wal_sync_method = "none"
snapshot_interval_events = 1000
```