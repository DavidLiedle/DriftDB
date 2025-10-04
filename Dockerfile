# DriftDB Docker Image
# Multi-stage build for minimal production image

# Build stage
FROM rust:1.75-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev

# Create app directory
WORKDIR /usr/src/driftdb

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# Build release binaries
RUN cargo build --release --bin driftdb --bin driftdb-server

# Runtime stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache libgcc

# Create data directory
RUN mkdir -p /var/lib/driftdb

# Copy binaries from builder
COPY --from=builder /usr/src/driftdb/target/release/driftdb /usr/local/bin/
COPY --from=builder /usr/src/driftdb/target/release/driftdb-server /usr/local/bin/

# Set environment variables
ENV DRIFTDB_DATA_PATH=/var/lib/driftdb
ENV DRIFTDB_LISTEN=0.0.0.0:5433

# Expose PostgreSQL-compatible port
EXPOSE 5433

# Volume for persistent data
VOLUME ["/var/lib/driftdb"]

# Default command starts the server
CMD ["driftdb-server"]

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z localhost 5433 || exit 1