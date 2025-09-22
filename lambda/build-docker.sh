#!/bin/bash

set -e

echo "Building Rust Lambda function with Docker..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "Using Docker to build Lambda function (bypasses Zig installation issues)..."

# Build using Docker with AWS Lambda runtime environment
docker run --rm \
  -v "$(pwd)":/code \
  -w /code \
  public.ecr.aws/lambda/provided:al2-x86_64 \
  bash -c "
    echo 'Setting up build environment...' && \
    yum update -y && \
    yum install -y gcc git && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    source ~/.cargo/env && \
    echo 'Installing cargo-lambda...' && \
    cargo install cargo-lambda && \
    echo 'Building Lambda function...' && \
    cargo lambda build --release && \
    echo 'Setting proper permissions...' && \
    chmod +x target/lambda/*/bootstrap
  "

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

echo "✅ Lambda function ready for deployment!"
echo "📦 To package: ./build-docker.sh package"