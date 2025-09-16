# Rust Market Ingestion Service

A high-performance Rust service for ingesting real-time stock market data from Yahoo Finance and publishing it to AWS SQS for downstream ML processing.

## Features

- Real-time data ingestion for 29 stock symbols
- Rate-limited Yahoo Finance API calls
- AWS SQS FIFO queue publishing with deduplication
- Environment-based configuration
- Comprehensive logging and error handling
- Docker support for ECS deployment

## Configuration

The service is configured via environment variables:

```bash
# Required
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/your-account/market_queue.fifo

# Optional (with defaults)
AWS_REGION=us-east-1
MARKET_DATA_SYMBOLS=AAPL,GOOGL,MSFT,... # 29 symbols by default
INGESTION_INTERVAL_SECONDS=60
YAHOO_FINANCE_BASE_URL=https://query1.finance.yahoo.com
RUST_LOG=info
```

## Running Locally

1. **Set up environment variables:**

   ```powershell
   $env:SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/296062571299/market_queue.fifo"
   $env:AWS_REGION="us-east-1"
   ```

2. **Run the service:**
   ```powershell
   cargo run
   ```

## AWS Credentials

The service automatically uses AWS credentials from:

- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM roles (when running on ECS)

Required IAM permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["sqs:SendMessage", "sqs:GetQueueAttributes"],
      "Resource": "arn:aws:sqs:*:*:market_queue.fifo"
    }
  ]
}
```

## Building for Production

```bash
cargo build --release
```

## Docker Deployment

```bash
docker build -t market-ingestion-service .
docker run -e SQS_QUEUE_URL=your-queue-url market-ingestion-service
```

## Message Format

The service publishes JSON messages to SQS with the following structure:

```json
{
  "symbol": "AAPL",
  "timestamp": "2025-09-15T14:30:00Z",
  "data": {
    "symbol": "AAPL",
    "timestamp": "2025-09-15T14:30:00Z",
    "open": 150.25,
    "high": 152.75,
    "low": 149.8,
    "close": 151.5,
    "volume": 1234567,
    "previous_close": 150.0
  },
  "message_type": "market_data",
  "source": "rust_ingestion_service"
}
```

## Monitoring

The service logs key metrics:

- Symbols processed per cycle
- Processing duration
- API response times
- Error rates and failures
