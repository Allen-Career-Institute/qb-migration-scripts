#!/bin/bash

# Retry Failed Ranges Script
# This script helps you retry failed ranges from the failed_ranges.txt file

FAILED_FILE="failed_ranges.txt"
AUTH_TOKEN=""
CONCURRENCY=5
API_WORKERS=10
API_BATCH_SIZE=5

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --auth-token)
            AUTH_TOKEN="$2"
            shift 2
            ;;
        --concurrency)
            CONCURRENCY="$2"
            shift 2
            ;;
        --api-workers)
            API_WORKERS="$2"
            shift 2
            ;;
        --api-batch-size)
            API_BATCH_SIZE="$2"
            shift 2
            ;;
        --failed-file)
            FAILED_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ -z "$AUTH_TOKEN" ]; then
    echo "Error: --auth-token is required"
    echo "Usage: $0 --auth-token <token> [--concurrency 5] [--api-workers 10] [--api-batch-size 5] [--failed-file failed_ranges.txt]"
    exit 1
fi

if [ ! -f "$FAILED_FILE" ]; then
    echo "No failed ranges file found: $FAILED_FILE"
    exit 1
fi

echo "Reading failed ranges from: $FAILED_FILE"
RANGE_COUNT=$(wc -l < "$FAILED_FILE")
echo "Found $RANGE_COUNT failed ranges to retry"

if [ "$RANGE_COUNT" -eq 0 ]; then
    echo "No failed ranges to retry"
    exit 0
fi

echo "Retrying with reduced concurrency settings:"
echo "  Concurrency: $CONCURRENCY"
echo "  API Workers: $API_WORKERS"
echo "  API Batch Size: $API_BATCH_SIZE"
echo ""

# Read each range and retry
while IFS= read -r range; do
    if [ -n "$range" ]; then
        # Parse range (format: start-end)
        START=$(echo "$range" | cut -d'-' -f1)
        END=$(echo "$range" | cut -d'-' -f2)
        
        echo "Retrying range: $START-$END"
        
        # Run the main script for this specific range
        go run . --start="$START" --end="$END" --range-size=100 \
                 --concurrency="$CONCURRENCY" --api-workers="$API_WORKERS" \
                 --api-batch-size="$API_BATCH_SIZE" --auth-token="$AUTH_TOKEN"
        
        # Add a small delay between retries
        sleep 2
    fi
done < "$FAILED_FILE"

echo "Retry completed!"
