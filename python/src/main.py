#!/usr/bin/env python3
"""
Market Surveillance Inference Service - Main Entry Point

This service provides real-time fraud detection for stock market data.
It consumes data from AWS SQS (or fallback to yfinance) and serves
anomaly detection results via gRPC API.

Usage:
    python main.py

Environment Variables:
    AWS_ACCESS_KEY_ID       - AWS access key for SQS
    AWS_SECRET_ACCESS_KEY   - AWS secret key for SQS  
    AWS_REGION             - AWS region (default: us-east-1)
    SQS_QUEUE_URL          - SQS FIFO queue URL
    USE_SQS_DATA_SOURCE    - Enable SQS consumption (default: true)
    FALLBACK_TO_YFINANCE   - Enable yfinance fallback (default: true)
    GRPC_PORT              - gRPC server port (default: 50051)
    GRPC_HOST              - gRPC server host (default: 0.0.0.0)
    LOG_LEVEL              - Logging level (default: INFO)
"""

import sys
import os
import logging

# Add the src directory to the path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from inference.grpc_server import serve
from inference.config import Config
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def setup_logging():
    """Configure logging for the service"""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )

def validate_environment():
    """Validate required environment variables and configuration"""
    logger = logging.getLogger(__name__)
    
    # Check if SQS is enabled and validate AWS credentials
    if Config.USE_SQS_DATA_SOURCE:
        if not Config.SQS_QUEUE_URL:
            logger.error("SQS_QUEUE_URL environment variable is required when USE_SQS_DATA_SOURCE=true")
            logger.info("Please set: export SQS_QUEUE_URL='https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT/market_queue.fifo'")
            return False
            
        # Use boto3 session to check for AWS credentials
        # This will check environment variables, ~/.aws/credentials, IAM roles, etc.
        try:
            session = boto3.Session(region_name=Config.AWS_REGION)
            credentials = session.get_credentials()
            if credentials is None:
                raise NoCredentialsError()
            
            # Test if credentials are valid by trying to get caller identity
            sts = session.client('sts')
            identity = sts.get_caller_identity()
            logger.info(f"AWS credentials validated successfully for account: {identity.get('Account', 'Unknown')}")
            
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error("AWS credentials are required when USE_SQS_DATA_SOURCE=true")
            logger.info("AWS credentials can be provided via:")
            logger.info("  1. Environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            logger.info("  2. AWS credentials file: ~/.aws/credentials (configured via 'aws configure')")
            logger.info("  3. IAM roles (when running on EC2)")
            logger.info("  4. AWS SSO or other AWS credential providers")
            return False
        except Exception as e:
            logger.error(f"Failed to validate AWS credentials: {e}")
            return False
            
        logger.info(f"SQS configuration valid: {Config.SQS_QUEUE_URL}")
    else:
        logger.info("SQS disabled, using yfinance fallback only")
    
    # Check model files
    if not os.path.exists(Config.AUTOENCODER_MODEL_PATH):
        logger.error(f"Autoencoder model not found: {Config.AUTOENCODER_MODEL_PATH}")
        return False
        
    if not os.path.exists(Config.CLASSIFIER_MODEL_PATH):
        logger.error(f"Classifier model not found: {Config.CLASSIFIER_MODEL_PATH}")
        return False
        
    if not os.path.exists(Config.SCALER_PATH):
        logger.error(f"Scaler not found: {Config.SCALER_PATH}")
        return False
        
    logger.info("All model files found and accessible")
    return True

def print_startup_info():
    """Print service startup information"""
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("Market Surveillance Inference Service")
    logger.info("="*60)
    logger.info(f"Service Version: {Config.SERVICE_VERSION}")
    logger.info(f"gRPC Server: {Config.GRPC_HOST}:{Config.GRPC_PORT}")
    logger.info(f"Log Level: {Config.LOG_LEVEL}")
    logger.info(f"AWS Region: {Config.AWS_REGION}")
    logger.info(f"SQS Enabled: {Config.USE_SQS_DATA_SOURCE}")
    if Config.USE_SQS_DATA_SOURCE:
        logger.info(f"SQS Queue: {Config.SQS_QUEUE_URL}")
        logger.info(f"SQS Polling Interval: {Config.SQS_POLLING_INTERVAL}s")
    logger.info(f"Fallback to yfinance: {Config.FALLBACK_TO_YFINANCE}")
    logger.info(f"Models Directory: {Config.MODELS_DIR}")
    logger.info("="*60)

def main():
    """Main entry point for the Market Surveillance Inference Service"""
    
    # Setup logging first
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Print startup information
        print_startup_info()
        
        # Validate environment and configuration
        if not validate_environment():
            logger.error("Environment validation failed. Exiting.")
            sys.exit(1)
        
        # Start the gRPC server
        logger.info("Starting gRPC server...")
        serve()
        
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()