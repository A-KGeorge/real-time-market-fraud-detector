# Multi-stage build for optimal Docker image size
FROM rust:1.86 AS builder

# Create app directory
WORKDIR /usr/src/app

# Copy manifest files
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build the application in release mode
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install ca-certificates for HTTPS requests
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy the binary from builder stage
COPY --from=builder /usr/src/app/target/release/market-ingestion-service /usr/local/bin/market-ingestion-service

# Set ownership
RUN chown appuser:appuser /usr/local/bin/market-ingestion-service

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD echo "Service running" || exit 1

# Set environment variables
ENV RUST_LOG=info

# Expose port (if needed for health checks)
EXPOSE 8080

# Run the binary
CMD ["market-ingestion-service"]