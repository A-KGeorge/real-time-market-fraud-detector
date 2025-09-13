"""
Configuration for market surveillance inference service
"""

import os
from typing import List

class Config:
    # Server Configuration
    GRPC_PORT = int(os.getenv('GRPC_PORT', '50051'))
    GRPC_HOST = os.getenv('GRPC_HOST', '0.0.0.0')
    
    # Model paths
    MODELS_DIR = os.getenv('MODELS_DIR', '/app/models')
    AUTOENCODER_MODEL_PATH = os.path.join(MODELS_DIR, 'market_autoencoder.tflite')
    CLASSIFIER_MODEL_PATH = os.path.join(MODELS_DIR, 'market_classifier.tflite')
    SCALER_PATH = os.path.join(MODELS_DIR, 'market_scaler.pkl')
    CONFIG_PATH = os.path.join(MODELS_DIR, 'market_surveillance_config.json')
    FEATURES_PATH = os.path.join(MODELS_DIR, 'market_features.json')
    
    # Default symbols for testing
    DEFAULT_SYMBOLS: List[str] = [
        'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 
        'NVDA', 'META', 'NFLX', 'AMD', 'INTC'
    ]
    
    # Data fetching configuration
    YFINANCE_PERIOD = '5d'  # Period for historical data
    YFINANCE_INTERVAL = '1m'  # 1-minute intervals
    
    # Feature engineering windows
    TECHNICAL_INDICATOR_WINDOWS = {
        'sma_10': 10,
        'sma_20': 20,
        'ema_12': 12,
        'ema_26': 26,
        'rsi_period': 14,
        'volatility_10d': 10,
        'volatility_20d': 20,
        'volume_ma': 20
    }
    
    # Streaming configuration  
    DEFAULT_STREAM_INTERVAL = 60  # seconds
    MAX_STREAM_INTERVAL = 300     # seconds
    MIN_STREAM_INTERVAL = 10      # seconds
    
    # SQS Consumer Configuration
    USE_SQS_DATA_SOURCE = os.getenv('USE_SQS_DATA_SOURCE', 'true').lower() == 'true'
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    SQS_QUEUE_NAME = os.getenv('SQS_QUEUE_NAME', 'market-surveillance-data.fifo')
    SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
    SQS_POLLING_INTERVAL = int(os.getenv('SQS_POLLING_INTERVAL', '10'))  # seconds
    SQS_MAX_MESSAGES = int(os.getenv('SQS_MAX_MESSAGES', '10'))  # max messages per poll
    SQS_WAIT_TIME = int(os.getenv('SQS_WAIT_TIME', '20'))  # long polling wait time
    SQS_VISIBILITY_TIMEOUT = int(os.getenv('SQS_VISIBILITY_TIMEOUT', '300'))  # 5 minutes
    
    # Fallback configuration (when SQS is not available)
    FALLBACK_TO_YFINANCE = os.getenv('FALLBACK_TO_YFINANCE', 'true').lower() == 'true'
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Service metadata
    SERVICE_VERSION = '1.0.0'
    SERVICE_NAME = 'market-surveillance-service'