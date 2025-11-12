# Benchmark Hardware Specifications

## Primary Benchmark System

**Date:** 2025-11-12

### Hardware
- **Model:** MacBook Air (2024)
- **Chip:** Apple M3 (8-core: 4 performance + 4 efficiency)
- **Memory:** 16 GB unified memory
- **Storage:** NVMe SSD (Apple-integrated)
- **OS:** macOS 14.x (Darwin 25.1.0)

### Software Environment
- **Rust:** rustc 1.82+
- **Cargo:** 1.82+
- **Target:** aarch64-apple-darwin

### Expected Performance Characteristics
Based on hardware class:
- **Single-threaded Operations:** 50-100K ops/sec
- **Multi-threaded Operations:** 200-500K ops/sec
- **Disk I/O:** ~3 GB/s sequential read/write
- **Memory Bandwidth:** ~100 GB/s

### Notes
- Apple Silicon M3 has exceptional single-threaded performance
- Unified memory architecture reduces copy overhead
- NVMe SSD provides consistent low-latency I/O
- All benchmarks run in release mode with optimizations enabled
