@echo off
setlocal enabledelayedexpansion

echo Building Rust Lambda function...

:: Check if cargo-lambda is installed
cargo lambda --version >nul 2>&1
if errorlevel 1 (
    echo Installing cargo-lambda...
    cargo install cargo-lambda
    if errorlevel 1 (
        echo Failed to install cargo-lambda
        exit /b 1
    )
)

:: Clean previous builds
if exist target (
    echo Cleaning previous builds...
    rmdir /s /q target
)

:: Build the Lambda function
echo Building for Lambda runtime...
cargo lambda build --release
if errorlevel 1 (
    echo Build failed
    exit /b 1
)

echo Build completed successfully!
echo Lambda artifact location: target/lambda/market-lambda-processor/

:: Optional: Package for deployment
if "%1"=="package" (
    echo Creating deployment package...
    cd target\lambda\market-lambda-processor\
    powershell Compress-Archive -Path .\* -DestinationPath ..\..\..\market-lambda-processor.zip -Force
    cd ..\..\..
    echo Deployment package created: market-lambda-processor.zip
)

echo Done!