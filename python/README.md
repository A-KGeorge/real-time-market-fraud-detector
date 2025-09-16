# Market Surveillance Inference Service

A Python-based machine learning inference service for real-time market anomaly detection. This service consumes market data from AWS SQS (provided by the Rust ingestion service) and performs anomaly detection using trained TensorFlow models.

## Architecture

The service operates in the following flow:

1. **Data Source**: Receives real-time market data from AWS SQS FIFO queue
2. **Fallback**: Uses yfinance as backup data source if SQS is unavailable
3. **Processing**: Applies feature engineering and technical indicators
4. **Inference**: Uses trained autoencoder and classifier models for anomaly detection
5. **Output**: Provides gRPC API for real-time anomaly detection results

## System Requirements

### Windows Users (WSL2 Required)

This service requires **WSL2 (Windows Subsystem for Linux)** on Windows systems due to:

- TensorFlow compatibility requirements
- Unix socket support needed by gRPC
- Better Docker integration

#### WSL2 Setup for Windows:

1. Install WSL2: `wsl --install`
2. Install Ubuntu distribution: `wsl --install -d Ubuntu`
3. Clone this repository inside WSL2: `/home/username/projects/ML`
4. Run all commands from within WSL2 environment

### Linux/Mac Users

No additional setup required - run directly on the host system.

## Installation

### 1. Python Environment Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Linux/Mac/WSL2
# venv\Scripts\activate     # Windows PowerShell (not recommended)

# Install dependencies
pip install -r requirements.txt
```

### 2. Environment Variables

Create a `.env` file or set the following environment variables:

```bash
# AWS Configuration (required for SQS)
export AWS_ACCESS_KEY_ID="your_aws_access_key"
export AWS_SECRET_ACCESS_KEY="your_aws_secret_key"
export AWS_REGION="us-east-1"

# SQS Configuration
export SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/296062571299/market_queue.fifo"
export USE_SQS_DATA_SOURCE="true"
export SQS_POLLING_INTERVAL="10"
export SQS_MAX_MESSAGES="10"
export SQS_WAIT_TIME="20"

# Service Configuration
export GRPC_PORT="50051"
export GRPC_HOST="0.0.0.0"
export LOG_LEVEL="INFO"

# Fallback Configuration
export FALLBACK_TO_YFINANCE="true"
```

### 3. Model Files

Ensure the following trained model files are present in `models/`:

- `market_autoencoder.tflite` - TensorFlow Lite autoencoder model
- `market_classifier.tflite` - TensorFlow Lite classifier model
- `market_scaler.pkl` - Scikit-learn feature scaler
- `market_surveillance_config.json` - Model configuration
- `market_features.json` - Feature engineering configuration

## Usage

### Running the Inference Service

#### Development Mode (Local)

```bash
cd python/src
python main.py
```

#### Production Mode (Docker)

```bash
# Build Docker image
cd python
docker build -t market-surveillance-inference .

# Run container
docker run -d \
  --name market-inference \
  -p 50051:50051 \
  -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  -e AWS_REGION="us-east-1" \
  -e SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/296062571299/market_queue.fifo" \
  market-surveillance-inference
```

### Testing the Service

#### Health Check

```bash
# Check if gRPC service is running
grpcurl -plaintext localhost:50051 describe
```

#### Real-time Data Stream

```bash
# Test anomaly detection for specific symbols
grpcurl -plaintext -d '{"symbols": ["AAPL", "GOOGL", "MSFT"]}' \
  localhost:50051 MarketSurveillanceService/DetectAnomalies
```

## Configuration

### Data Sources

- **Primary**: AWS SQS FIFO queue (real-time data from Rust ingestion service)
- **Fallback**: Yahoo Finance API via yfinance library

### SQS Message Format

Expected message format from the Rust ingestion service:

```json
{
  "symbol": "AAPL",
  "price": 150.25,
  "volume": 1000000,
  "timestamp": "2024-01-15T10:30:00Z",
  "change": 2.5,
  "change_percent": 1.69
}
```

### Model Performance

- **Autoencoder**: Detects price/volume anomalies using reconstruction error
- **Classifier**: Categorizes anomaly types (volume spike, price gap, etc.)
- **Latency**: < 100ms per inference request
- **Throughput**: ~1000 requests/second

## Monitoring

### Logs

- Service logs available via stdout/stderr
- Log level configurable via `LOG_LEVEL` environment variable
- Structured logging for better observability

### Metrics

The service exposes the following operational metrics:

- SQS message processing rate
- Model inference latency
- Anomaly detection rate
- Fallback usage frequency

## Troubleshooting

### Common Issues

#### SQS Connection Issues

```bash
# Check AWS credentials
aws sts get-caller-identity

# Test SQS access
aws sqs get-queue-attributes --queue-url "$SQS_QUEUE_URL"
```

#### Model Loading Issues

```bash
# Verify model files exist
ls -la models/

# Check TensorFlow installation
python -c "import tensorflow as tf; print(tf.__version__)"
```

#### WSL2 Issues on Windows

```bash
# Restart WSL2
wsl --shutdown
wsl

# Check WSL2 version
wsl --list --verbose
```

### Performance Tuning

- Increase `SQS_MAX_MESSAGES` for higher throughput
- Adjust `SQS_POLLING_INTERVAL` based on data frequency
- Use GPU acceleration by installing `tensorflow-gpu`

## Development

### Running Tests

```bash
cd python
python -m pytest tests/ -v
```

### Code Style

```bash
# Format code
black src/
isort src/

# Lint code
flake8 src/
```

## Dependencies

### Core Dependencies

- `tensorflow>=2.20.0` - Machine learning models
- `boto3>=1.35.82` - AWS SQS integration
- `grpcio>=1.74.0` - gRPC server
- `pandas>=2.3.2` - Data manipulation
- `scikit-learn>=1.7.2` - Feature scaling
- `yfinance>=0.2.65` - Fallback data source

### Development Dependencies

- `pytest` - Testing framework
- `black` - Code formatting
- `isort` - Import sorting
- `flake8` - Linting

## API Reference

### gRPC Service: MarketSurveillanceService

#### DetectAnomalies

Detects anomalies in real-time market data for specified symbols.

**Request:**

```protobuf
message AnomalyDetectionRequest {
  repeated string symbols = 1;
  optional int32 lookback_period = 2;
}
```

**Response:**

```protobuf
message AnomalyDetectionResponse {
  repeated Anomaly anomalies = 1;
  int64 timestamp = 2;
}
```

#### StreamAnomalies

Provides continuous streaming of anomaly detection results.

**Request:**

```protobuf
message StreamRequest {
  repeated string symbols = 1;
  int32 interval_seconds = 2;
}
```

**Response:**

```protobuf
stream AnomalyDetectionResponse
```

For complete API documentation, see the Protocol Buffer definitions in `../proto/market_surveillance.proto`.
