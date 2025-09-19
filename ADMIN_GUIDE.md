# DriftDB Admin Guide

## Installation

```bash
# Install the admin tool
cargo install driftdb-admin

# Or build from source
cargo build --release --bin driftdb-admin
```

## Usage

### Status Monitoring

```bash
# Show database status
driftdb-admin status

# Monitor real-time metrics
driftdb-admin monitor --interval 1

# Show health status
driftdb-admin health --verbose
```

### Backup Management

```bash
# Create a full backup
driftdb-admin backup create ./backups/$(date +%Y%m%d) --compress

# Create an incremental backup
driftdb-admin backup create ./backups/inc --incremental

# List available backups
driftdb-admin backup list

# Restore from backup
driftdb-admin backup restore ./backups/20240115 --verify

# Verify backup integrity
driftdb-admin backup verify ./backups/20240115
```

### Replication Control

```bash
# Show replication status
driftdb-admin replication status

# Show replication lag
driftdb-admin replication lag

# Promote replica to master
driftdb-admin replication promote --force

# Add a new replica
driftdb-admin replication add replica2.example.com:5433

# Initiate manual failover
driftdb-admin replication failover --target replica2 --reason "Maintenance"
```

### Table Management

```bash
# Show all tables
driftdb-admin tables --verbose

# Analyze table statistics
driftdb-admin analyze --table users

# Compact storage
driftdb-admin compact --table users --progress

# Show indexes
driftdb-admin indexes --table users

# Verify data integrity
driftdb-admin verify --table users --checksums
```

### Migration Management

```bash
# Show migration status
driftdb-admin migrate status

# Apply pending migrations
driftdb-admin migrate up --dry-run
driftdb-admin migrate up

# Rollback migrations
driftdb-admin migrate down 1

# Create a new migration
driftdb-admin migrate create add_user_preferences
```

### Connection Management

```bash
# Show connection pool status
driftdb-admin connections

# Show active transactions
driftdb-admin transactions --active

# Show all transactions with details
driftdb-admin transactions
```

### Configuration

```bash
# Show all configuration
driftdb-admin config

# Show specific configuration section
driftdb-admin config --section database
driftdb-admin config --section performance
driftdb-admin config --section security
```

### Interactive Dashboard

```bash
# Launch interactive TUI dashboard
driftdb-admin dashboard
```

The dashboard provides:
- Real-time metrics
- Query performance
- Replication status
- Resource usage
- Alert notifications

## Performance Monitoring

### Key Metrics

Monitor these critical metrics:

```bash
# Query performance
driftdb-admin monitor | grep -E "(QPS|Latency)"

# Resource usage
driftdb-admin monitor | grep -E "(Memory|Disk|CPU)"

# Replication health
driftdb-admin replication lag
```

### Alerts

Set up alerting for:
- Replication lag > 5 seconds
- Disk usage > 90%
- Memory usage > 80%
- Connection pool exhaustion
- Transaction timeouts

## Troubleshooting

### Common Issues

#### High Replication Lag

```bash
# Check network connectivity
ping replica-host

# Check replication status
driftdb-admin replication status

# Verify WAL streaming
driftdb-admin replication lag
```

#### Performance Issues

```bash
# Analyze slow queries
driftdb-admin analyze --table problematic_table

# Check index usage
driftdb-admin indexes --table problematic_table

# Compact if needed
driftdb-admin compact --table problematic_table
```

#### Recovery Procedures

```bash
# Verify data integrity after crash
driftdb-admin verify --checksums

# Restore from backup if needed
driftdb-admin backup restore ./last-good-backup --verify

# Rebuild indexes if corrupted
driftdb-admin analyze --table affected_table
```

## Best Practices

### Daily Operations

1. **Monitor health status**
   ```bash
   driftdb-admin health
   ```

2. **Check replication lag**
   ```bash
   driftdb-admin replication lag
   ```

3. **Review metrics**
   ```bash
   driftdb-admin monitor --interval 60
   ```

### Weekly Tasks

1. **Analyze tables for statistics**
   ```bash
   driftdb-admin analyze
   ```

2. **Review backup status**
   ```bash
   driftdb-admin backup list
   ```

3. **Check disk usage**
   ```bash
   driftdb-admin status --format json | jq .storage_size_bytes
   ```

### Monthly Tasks

1. **Compact storage**
   ```bash
   driftdb-admin compact --progress
   ```

2. **Verify backup integrity**
   ```bash
   for backup in ./backups/*; do
     driftdb-admin backup verify $backup
   done
   ```

3. **Review and optimize slow queries**
   ```bash
   driftdb-admin analyze --verbose
   ```

## Security

### Access Control

The admin tool requires appropriate permissions:

```bash
# Run with specific credentials
DRIFTDB_ADMIN_USER=admin \
DRIFTDB_ADMIN_PASSWORD=secret \
driftdb-admin status
```

### Audit Logging

All admin operations are logged:

```bash
# View audit log
tail -f /var/log/driftdb/admin.log

# Search for specific operations
grep "backup create" /var/log/driftdb/admin.log
```

### Encryption

Ensure encrypted connections:

```bash
# Connect with TLS
driftdb-admin --tls-cert /etc/driftdb/certs/client.crt \
             --tls-key /etc/driftdb/certs/client.key \
             status
```

## Automation

### Cron Jobs

```bash
# Daily backup
0 2 * * * /usr/local/bin/driftdb-admin backup create /backups/daily/$(date +\%Y\%m\%d)

# Hourly health check
0 * * * * /usr/local/bin/driftdb-admin health || alert-team

# Weekly compaction
0 3 * * 0 /usr/local/bin/driftdb-admin compact --progress
```

### Monitoring Integration

Export metrics to Prometheus:

```bash
# Run metrics exporter
driftdb-admin monitor --format prometheus --port 9090
```

### Alerting

Integrate with PagerDuty/Slack:

```bash
# Check health and alert on failure
if ! driftdb-admin health; then
  curl -X POST https://hooks.slack.com/services/YOUR/WEBHOOK/URL \
    -d '{"text":"DriftDB health check failed!"}'
fi
```