# Multi-stage build for Python ML Service
FROM python:3.11-slim AS builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements-inference.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app/src"

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r appuser \
    && useradd -r -g appuser appuser

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Set working directory
WORKDIR /app

# Copy only necessary application code (exclude training)
COPY src/inference/ ./src/inference/
COPY src/shared/ ./src/shared/
COPY src/generated/ ./src/generated/
COPY src/main.py ./src/
COPY src/lambda_inference.py ./src/
COPY entrypoint.sh ./

# Copy only TensorFlow Lite models and associated files
COPY models/*.tflite ./models/
COPY models/*.pkl ./models/
COPY models/*.json ./models/
# Note: Exclude any large training datasets or full TensorFlow models

# Copy proto generated files (ensure directory exists first)
RUN mkdir -p ./src/generated/
COPY src/generated/ ./src/generated/

# Create necessary directories and set permissions
RUN mkdir -p /app/logs \
    && chmod +x /app/entrypoint.sh \
    && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Expose gRPC port and health check port
EXPOSE 50051 8080

# Set default environment variables for container
ENV USE_SQS_DATA_SOURCE=true \
    FALLBACK_TO_YFINANCE=true \
    AWS_REGION=us-east-1 \
    GRPC_HOST=0.0.0.0 \
    GRPC_PORT=50051 \
    LOG_LEVEL=INFO

# Start the service
CMD ["./entrypoint.sh"]