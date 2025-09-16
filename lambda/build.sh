#!/bin/bash

set -e

echo "Building Rust Lambda function..."

# Install cargo-lambda if not present
if ! command -v cargo-lambda &> /dev/null; then
    echo "Installing cargo-lambda..."
    cargo install cargo-lambda
fi

# Clean previous builds
rm -rf target/

# Build the Lambda function
echo "Building for Lambda runtime..."
cargo lambda build --release

echo "Build completed successfully!"
echo "Lambda artifact location: target/lambda/market-lambda-processor/"

# Optional: Package for deployment
if [ "$1" = "package" ]; then
    echo "Creating deployment package..."
    cd target/lambda/market-lambda-processor/
    zip -r ../../../market-lambda-processor.zip .
    cd -
    echo "Deployment package created: market-lambda-processor.zip"
fi