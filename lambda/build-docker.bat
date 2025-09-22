@echo off
setlocal enabledelayedexpansion

echo Building Rust Lambda function with Docker...

:: Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo Error: Docker is not running. Please start Docker Desktop.
    exit /b 1
)

echo Using Docker to build Lambda function (bypasses Zig installation issues)...

:: Build using Docker with simple cross-compilation
docker run --rm ^
  -v "%cd%":/code ^
  -w /code ^
  rust:1.86-slim ^
  bash -c "set -e && apt-get update && apt-get install -y pkg-config libssl-dev && rustup target add x86_64-unknown-linux-gnu && echo 'Building Lambda function...' && cargo build --release --target x86_64-unknown-linux-gnu && mkdir -p target/lambda/bootstrap && cp target/x86_64-unknown-linux-gnu/release/bootstrap target/lambda/bootstrap/bootstrap"

if errorlevel 1 (
    echo Build failed
    exit /b 1
)

echo Build completed successfully!
echo Lambda artifact location: target/lambda/bootstrap/

:: Optional: Package for deployment
if "%1"=="package" (
    echo Creating deployment package...
    cd target\lambda\bootstrap\
    powershell Compress-Archive -Path .\bootstrap -DestinationPath ..\market-lambda-processor.zip -Force
    cd ..\..\..
    echo Deployment package created: market-lambda-processor.zip
)

echo ✅ Lambda function ready for deployment!
echo 📦 To package: build-docker.bat package