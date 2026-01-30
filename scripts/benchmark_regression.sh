#!/bin/bash
#
# Performance Regression Detection Script
#
# This script compares benchmark results against saved baselines and fails
# if any benchmark shows a regression exceeding the threshold.
#
# Usage:
#   ./scripts/benchmark_regression.sh [--save-baseline] [--threshold PERCENT]
#
# Options:
#   --save-baseline   Save current results as the new baseline
#   --threshold       Regression threshold percentage (default: 10)
#

set -e

THRESHOLD=10
SAVE_BASELINE=false
BASELINE_DIR="benchmarks/baseline"
CRITERION_DIR="target/criterion"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --save-baseline)
            SAVE_BASELINE=true
            shift
            ;;
        --threshold)
            THRESHOLD="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== DriftDB Performance Regression Detection ==="
echo "Threshold: ${THRESHOLD}%"
echo ""

# Ensure we're in the project root
if [ ! -f "Cargo.toml" ]; then
    echo "Error: Must run from project root directory"
    exit 1
fi

# Run benchmarks
echo "Running benchmarks..."
cargo bench --bench simple_benchmarks -- --noplot 2>&1 | tee benchmark_output.txt
cargo bench --bench core_operations -- --noplot 2>&1 | tee -a benchmark_output.txt

# Create baseline directory if it doesn't exist
mkdir -p "$BASELINE_DIR"

if [ "$SAVE_BASELINE" = true ]; then
    echo ""
    echo "=== Saving baseline results ==="

    # Copy criterion results to baseline directory
    if [ -d "$CRITERION_DIR" ]; then
        rm -rf "$BASELINE_DIR"/*
        cp -r "$CRITERION_DIR"/* "$BASELINE_DIR"/ 2>/dev/null || true
        echo "Baseline saved to $BASELINE_DIR"
    else
        echo "Warning: No criterion results found at $CRITERION_DIR"
    fi

    # Parse and save summary
    echo "Extracting benchmark summaries..."
    grep -E "^(test |bench:|time:)" benchmark_output.txt > "$BASELINE_DIR/summary.txt" 2>/dev/null || true

    echo "Baseline saved successfully!"
    rm -f benchmark_output.txt
    exit 0
fi

# Compare against baseline
echo ""
echo "=== Checking for regressions ==="

if [ ! -f "$BASELINE_DIR/summary.txt" ]; then
    echo "Warning: No baseline found at $BASELINE_DIR/summary.txt"
    echo "Run with --save-baseline first to create a baseline."
    rm -f benchmark_output.txt
    exit 0
fi

# Create comparison report
REGRESSION_FOUND=false
REPORT_FILE="benchmark_report.txt"

echo "Performance Regression Report" > "$REPORT_FILE"
echo "=============================" >> "$REPORT_FILE"
echo "Threshold: ${THRESHOLD}%" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

# Parse criterion estimate files for comparison
if [ -d "$CRITERION_DIR" ]; then
    for estimate_file in $(find "$CRITERION_DIR" -name "estimates.json" 2>/dev/null); do
        bench_name=$(dirname "$estimate_file" | sed "s|$CRITERION_DIR/||" | sed 's|/new||')

        baseline_file="$BASELINE_DIR/${bench_name}/new/estimates.json"

        if [ -f "$baseline_file" ] && [ -f "$estimate_file" ]; then
            # Extract mean times (in nanoseconds)
            current_mean=$(grep -o '"point_estimate":[0-9.e+-]*' "$estimate_file" | head -1 | cut -d: -f2)
            baseline_mean=$(grep -o '"point_estimate":[0-9.e+-]*' "$baseline_file" | head -1 | cut -d: -f2)

            if [ -n "$current_mean" ] && [ -n "$baseline_mean" ] && [ "$baseline_mean" != "0" ]; then
                # Calculate percentage change using awk for floating point
                change=$(awk "BEGIN {printf \"%.2f\", (($current_mean - $baseline_mean) / $baseline_mean) * 100}")

                # Check if regression exceeds threshold
                is_regression=$(awk "BEGIN {print ($change > $THRESHOLD) ? 1 : 0}")

                if [ "$is_regression" = "1" ]; then
                    echo "REGRESSION: $bench_name: +${change}% (threshold: ${THRESHOLD}%)" >> "$REPORT_FILE"
                    echo "  Baseline: ${baseline_mean}ns, Current: ${current_mean}ns" >> "$REPORT_FILE"
                    REGRESSION_FOUND=true
                elif [ "$(awk "BEGIN {print ($change < -$THRESHOLD) ? 1 : 0}")" = "1" ]; then
                    echo "IMPROVEMENT: $bench_name: ${change}%" >> "$REPORT_FILE"
                else
                    echo "OK: $bench_name: ${change}%" >> "$REPORT_FILE"
                fi
            fi
        fi
    done
fi

echo "" >> "$REPORT_FILE"
cat "$REPORT_FILE"

# Cleanup
rm -f benchmark_output.txt

if [ "$REGRESSION_FOUND" = true ]; then
    echo ""
    echo "!!! PERFORMANCE REGRESSION DETECTED !!!"
    echo "One or more benchmarks exceeded the ${THRESHOLD}% regression threshold."
    echo "See benchmark_report.txt for details."
    exit 1
else
    echo ""
    echo "All benchmarks within acceptable performance range."
    rm -f "$REPORT_FILE"
    exit 0
fi
