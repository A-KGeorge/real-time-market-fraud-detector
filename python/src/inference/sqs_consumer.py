"""
SQS Consumer for Market Surveillance Inference Service
Consumes market data messages from SQS FIFO queues
"""

import json
import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import pandas as pd
import numpy as np
from .config import Config

logger = logging.getLogger(__name__)

class SQSConsumer:
    def __init__(self):
        self.config = Config
        self.sqs_client = None
        self.queue_url = None
        self.message_cache = {}
        self.last_poll_time = None
        self._initialize_client()
        
    def _initialize_client(self):
        """Initialize SQS client"""
        try:
            # Create SQS client
            self.sqs_client = boto3.client('sqs', region_name=self.config.AWS_REGION)
            
            # Get queue URL
            if self.config.SQS_QUEUE_URL:
                self.queue_url = self.config.SQS_QUEUE_URL
            else:
                # Try to discover queue URL by name
                response = self.sqs_client.get_queue_url(QueueName=self.config.SQS_QUEUE_NAME)
                self.queue_url = response['QueueUrl']
            
            logger.info(f"SQS Consumer initialized for queue: {self.queue_url}")
            
        except Exception as e:
            logger.error(f"Failed to initialize SQS consumer: {str(e)}")
            if not self.config.FALLBACK_TO_YFINANCE:
                raise
            logger.warning("Will use fallback data source")
    
    def poll_messages(self) -> List[Dict[str, Any]]:
        """
        Poll SQS queue for new messages
        
        Returns:
            List of parsed message dictionaries
        """
        try:
            if not self.sqs_client or not self.queue_url:
                logger.warning("SQS client not initialized")
                return []
            
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.config.SQS_MAX_MESSAGES,
                WaitTimeSeconds=self.config.SQS_WAIT_TIME,
                MessageAttributeNames=['All'],
                AttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            self.last_poll_time = datetime.now()
            
            if not messages:
                logger.debug("No messages received from SQS")
                return []
            
            logger.info(f"Received {len(messages)} messages from SQS")
            
            # Parse and validate messages
            parsed_messages = []
            for message in messages:
                try:
                    parsed_msg = self._parse_message(message)
                    if parsed_msg:
                        parsed_messages.append(parsed_msg)
                        # Delete message after successful parsing
                        self._delete_message(message['ReceiptHandle'])
                except Exception as e:
                    logger.error(f"Failed to parse message: {str(e)}")
                    # Don't delete unparseable messages - they'll be retried
            
            return parsed_messages
            
        except Exception as e:
            logger.error(f"Failed to poll SQS messages: {str(e)}")
            return []
    
    def get_latest_data_for_symbol(self, symbol: str) -> Optional[pd.Series]:
        """
        Get the latest market data for a specific symbol from cache or SQS
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Series with latest market data or None
        """
        try:
            # Check cache first
            cache_key = symbol.upper()
            if cache_key in self.message_cache:
                cached_data = self.message_cache[cache_key]
                # Check if cache is still fresh (within last 5 minutes)
                cache_age = (datetime.now() - cached_data['timestamp']).total_seconds()
                if cache_age < 300:  # 5 minutes
                    logger.debug(f"Using cached data for {symbol}")
                    return cached_data['data']
            
            # Poll for new messages
            messages = self.poll_messages()
            
            # Look for the specific symbol in new messages
            for message in messages:
                if message.get('symbol', '').upper() == symbol.upper():
                    market_data = self._convert_message_to_series(message)
                    if market_data is not None:
                        # Cache the data
                        self.message_cache[cache_key] = {
                            'data': market_data,
                            'timestamp': datetime.now()
                        }
                        return market_data
            
            # If no specific symbol found, return None
            logger.debug(f"No recent data found for {symbol} in SQS")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get latest data for {symbol}: {str(e)}")
            return None
    
    def get_batch_data(self, symbols: List[str]) -> Dict[str, Optional[pd.Series]]:
        """
        Get latest data for multiple symbols
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dictionary mapping symbols to their latest data
        """
        try:
            # Poll for new messages
            messages = self.poll_messages()
            
            results = {}
            
            # First, update cache with new messages
            for message in messages:
                symbol = message.get('symbol', '').upper()
                if symbol:
                    market_data = self._convert_message_to_series(message)
                    if market_data is not None:
                        self.message_cache[symbol] = {
                            'data': market_data,
                            'timestamp': datetime.now()
                        }
            
            # Now get data for requested symbols
            for symbol in symbols:
                cache_key = symbol.upper()
                if cache_key in self.message_cache:
                    cached_data = self.message_cache[cache_key]
                    # Check freshness (within last 5 minutes)
                    cache_age = (datetime.now() - cached_data['timestamp']).total_seconds()
                    if cache_age < 300:
                        results[symbol] = cached_data['data']
                    else:
                        results[symbol] = None
                else:
                    results[symbol] = None
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get batch data: {str(e)}")
            return {symbol: None for symbol in symbols}
    
    def _parse_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse SQS message and validate format"""
        try:
            # Parse message body
            body = json.loads(message['Body'])
            
            # Validate message structure
            if not self._validate_message_format(body):
                logger.warning("Invalid message format received")
                return None
            
            # Add SQS metadata
            body['_sqs_metadata'] = {
                'receipt_handle': message['ReceiptHandle'],
                'message_id': message['MessageId'],
                'received_time': datetime.now().isoformat()
            }
            
            return body
            
        except Exception as e:
            logger.error(f"Failed to parse message: {str(e)}")
            return None
    
    def _validate_message_format(self, message: Dict[str, Any]) -> bool:
        """Validate that message has required fields"""
        required_fields = ['messageType', 'symbol', 'data', 'timestamp']
        
        for field in required_fields:
            if field not in message:
                logger.warning(f"Message missing required field: {field}")
                return False
        
        # Validate data structure
        data = message.get('data', {})
        required_data_fields = ['open', 'high', 'low', 'close', 'volume']
        
        for field in required_data_fields:
            if field not in data:
                logger.warning(f"Message data missing required field: {field}")
                return False
        
        return True
    
    def _convert_message_to_series(self, message: Dict[str, Any]) -> Optional[pd.Series]:
        """Convert SQS message to pandas Series"""
        try:
            data = message.get('data', {})
            symbol = message.get('symbol', '')
            timestamp = message.get('timestamp', '')
            
            # Create Series with market data
            series_data = {
                'Open': float(data['open']),
                'High': float(data['high']),
                'Low': float(data['low']),
                'Close': float(data['close']),
                'Volume': int(data['volume']),
                'Symbol': symbol,
                'Date': pd.to_datetime(timestamp)
            }
            
            # Add additional fields if present
            if 'adjClose' in data:
                series_data['Adj Close'] = float(data['adjClose'])
            
            return pd.Series(series_data)
            
        except Exception as e:
            logger.error(f"Failed to convert message to Series: {str(e)}")
            return None
    
    def _delete_message(self, receipt_handle: str):
        """Delete message from SQS queue"""
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.debug("Message deleted from queue")
        except Exception as e:
            logger.error(f"Failed to delete message: {str(e)}")
    
    def get_consumer_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        return {
            'cache_size': len(self.message_cache),
            'last_poll_time': self.last_poll_time.isoformat() if self.last_poll_time else None,
            'queue_url': self.queue_url,
            'is_connected': self.sqs_client is not None
        }
    
    def clear_cache(self):
        """Clear message cache"""
        self.message_cache.clear()
        logger.info("Message cache cleared")