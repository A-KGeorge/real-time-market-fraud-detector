"""
 Configuration for Market Surveillance SQS Producer
"""

import os
from typing import Optional

class Config:
    # AWS Credentials (will use IAM roles in ECS, or env vars for local)
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')  # Optional - use IAM roles when possible
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')  # Optional
    
    # SQS Configuration
    SQS_QUEUE_NAME = os.getenv('SQS_QUEUE_NAME', 'market-surveillance-data.fifo')
    SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')  # Will be auto-discovered if not provided
    
    # SQS FIFO Settings
    MESSAGE_GROUP_ID_PREFIX = os.getenv('MESSAGE_GROUP_ID_PREFIX', 'market-data')
    MESSAGE_DEDUPLICATION_ENABLED = True
    FIFO_THROUGHPUT_LIMIT = os.getenv('FIFO_THROUGHPUT_LIMIT', 'perMessageGroupId')  # or 'perQueue'
    
    # Message Settings
    MESSAGE_RETENTION_PERIOD = int(os.getenv('MESSAGE_RETENTION_PERIOD', '1209600'))  # 14 days in seconds
    VISIBILITY_TIMEOUT = int(os.getenv('VISIBILITY_TIMEOUT', '300'))  # 5 minutes
    MAX_MESSAGE_SIZE = int(os.getenv('MAX_MESSAGE_SIZE', '262144'))  # 256KB
    
    # Retry Configuration
    MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
    RETRY_DELAY_BASE = float(os.getenv('RETRY_DELAY_BASE', '2.0'))  # Base delay in seconds
    RETRY_DELAY_MAX = float(os.getenv('RETRY_DELAY_MAX', '60.0'))  # Max delay in seconds
    
    # Producer Settings
    BATCH_SIZE = int(os.getenv('SQS_BATCH_SIZE', '10'))  # Max 10 for SQS SendMessageBatch
    PRODUCER_INTERVAL = int(os.getenv('PRODUCER_INTERVAL', '60'))  # seconds between data fetches
    
    # Data Fetching Settings
    MARKET_DATA_SYMBOLS = os.getenv('MARKET_DATA_SYMBOLS', 'AAPL,GOOGL,MSFT,AMZN,TSLA,NVDA,META,NFLX,AMD,INTC').split(',')
    YFINANCE_PERIOD = os.getenv('YFINANCE_PERIOD', '1d')  # Data period for fetching
    YFINANCE_INTERVAL = os.getenv('YFINANCE_INTERVAL', '1m')  # Data interval
    
    # Service Configuration
    SERVICE_NAME = os.getenv('SERVICE_NAME', 'market-surveillance-producer')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Market Hours (for production optimization)
    MARKET_OPEN_HOUR = int(os.getenv('MARKET_OPEN_HOUR', '9'))  # 9 AM EST
    MARKET_CLOSE_HOUR = int(os.getenv('MARKET_CLOSE_HOUR', '16'))  # 4 PM EST
    WEEKEND_OPERATION = os.getenv('WEEKEND_OPERATION', 'false').lower() == 'true'
    
    # Monitoring
    ENABLE_CLOUDWATCH_METRICS = os.getenv('ENABLE_CLOUDWATCH_METRICS', 'false').lower() == 'true'
    METRICS_NAMESPACE = os.getenv('METRICS_NAMESPACE', 'MarketSurveillance/Producer')
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate required configuration"""
        required_configs = []
        
        if not cls.AWS_REGION:
            required_configs.append('AWS_REGION')
            
        if not cls.SQS_QUEUE_NAME:
            required_configs.append('SQS_QUEUE_NAME')
            
        if not cls.MARKET_DATA_SYMBOLS:
            required_configs.append('MARKET_DATA_SYMBOLS')
        
        if required_configs:
            raise ValueError(f"Missing required configuration: {', '.join(required_configs)}")
        
        return True
    
    @classmethod
    def get_aws_credentials(cls) -> dict:
        """Get AWS credentials configuration"""
        config = {
            'region_name': cls.AWS_REGION
        }
        
        # Only add credentials if they're provided (prefer IAM roles)
        if cls.AWS_ACCESS_KEY_ID and cls.AWS_SECRET_ACCESS_KEY:
            config.update({
                'aws_access_key_id': cls.AWS_ACCESS_KEY_ID,
                'aws_secret_access_key': cls.AWS_SECRET_ACCESS_KEY
            })
            
        return config