#!/bin/bash
export PROCESSING_MODE=LAMBDA_TRIGGERED
export CORRELATION_ID=test-123
export TARGET_SYMBOL=JNJ
export MARKET_DATA='{"data":{"symbol":"JNJ","close":176.19,"high":177.16,"low":173.33,"open":174.69,"volume":25617300,"previous_close":174.16,"timestamp":"2025-09-22T02:28:58.708056619Z"}}'

python src/lambda_inference.py