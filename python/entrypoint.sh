#!/bin/bash

# Startup script for Python inference service
# Supports both regular gRPC mode and Lambda-triggered mode

set -e

echo "Starting Python inference service..."
echo "PROCESSING_MODE: ${PROCESSING_MODE:-GRPC_SERVER}"

# Check if we're in Lambda-triggered mode
if [ "${PROCESSING_MODE}" = "LAMBDA_TRIGGERED" ]; then
    echo "Running in Lambda-triggered mode"
    echo "Correlation ID: ${CORRELATION_ID:-N/A}"
    echo "Target Symbol: ${TARGET_SYMBOL:-N/A}"
    
    # Run the lambda inference script
    python src/lambda_inference.py
else
    echo "Running in gRPC server mode"
    echo "gRPC Host: ${GRPC_HOST:-0.0.0.0}"
    echo "gRPC Port: ${GRPC_PORT:-50051}"
    echo "SQS Data Source: ${USE_SQS_DATA_SOURCE:-true}"
    
    # Run the main gRPC server
    python src/main.py
fi