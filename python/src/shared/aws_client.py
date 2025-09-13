"""
AWS SQS Client wrapper for Market Surveillance Producer
"""

import json
import time
import hashlib
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from .config import Config

logger = logging.getLogger(__name__)

class SQSClient:
    def __init__(self):
        self.config = Config
        self.sqs_client = None
        self.queue_url = None
        self.queue_attributes = {}
        self._initialize_client()
        
    def _initialize_client(self):
        """Initialize SQS client and get queue URL"""
        try:
            # Validate configuration
            self.config.validate_config()
            
            # Create SQS client
            aws_config = self.config.get_aws_credentials()
            self.sqs_client = boto3.client('sqs', **aws_config)
            
            # Get or create queue URL
            self.queue_url = self._get_queue_url()
            
            # Get queue attributes for validation
            self._get_queue_attributes()
            
            logger.info(f"SQS client initialized successfully for queue: {self.config.SQS_QUEUE_NAME}")
            
        except Exception as e:
            logger.error(f"Failed to initialize SQS client: {str(e)}")
            raise
    
    def _get_queue_url(self) -> str:
        """Get SQS queue URL"""
        try:
            # Use provided URL if available
            if self.config.SQS_QUEUE_URL:
                logger.info(f"Using provided queue URL: {self.config.SQS_QUEUE_URL}")
                return self.config.SQS_QUEUE_URL
            
            # Get queue URL by name
            response = self.sqs_client.get_queue_url(
                QueueName=self.config.SQS_QUEUE_NAME
            )
            queue_url = response['QueueUrl']
            logger.info(f"Retrieved queue URL: {queue_url}")
            return queue_url
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
                logger.error(f"Queue {self.config.SQS_QUEUE_NAME} does not exist")
                raise ValueError(f"SQS queue {self.config.SQS_QUEUE_NAME} not found")
            else:
                logger.error(f"Error getting queue URL: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error getting queue URL: {str(e)}")
            raise
    
    def _get_queue_attributes(self):
        """Get queue attributes for validation"""
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['All']
            )
            self.queue_attributes = response['Attributes']
            
            # Validate it's a FIFO queue
            fifo_queue = self.queue_attributes.get('FifoQueue', 'false')
            if fifo_queue.lower() != 'true':
                logger.warning("Queue is not a FIFO queue - message ordering not guaranteed")
            
            logger.debug(f"Queue attributes retrieved: {len(self.queue_attributes)} attributes")
            
        except Exception as e:
            logger.warning(f"Could not retrieve queue attributes: {str(e)}")
    
    def send_message(self, message_body: str, message_group_id: str, 
                    message_attributes: Optional[Dict] = None) -> bool:
        """
        Send a single message to SQS FIFO queue
        
        Args:
            message_body: JSON string message body
            message_group_id: Message group ID for FIFO ordering
            message_attributes: Optional message attributes
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate deduplication ID if needed
            deduplication_id = self._generate_deduplication_id(message_body, message_group_id)
            
            # Prepare message parameters
            message_params = {
                'QueueUrl': self.queue_url,
                'MessageBody': message_body,
                'MessageGroupId': message_group_id,
                'MessageDeduplicationId': deduplication_id
            }
            
            # Add message attributes if provided
            if message_attributes:
                message_params['MessageAttributes'] = self._format_message_attributes(message_attributes)
            
            # Send message with retry logic
            return self._send_with_retry(message_params)
            
        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            return False
    
    def send_message_batch(self, messages: List[Dict]) -> Tuple[int, int, List[str]]:
        """
        Send multiple messages in a batch to SQS FIFO queue
        
        Args:
            messages: List of message dictionaries with keys:
                     - body: message body (string)
                     - group_id: message group ID
                     - attributes: optional message attributes dict
                     
        Returns:
            Tuple of (successful_count, failed_count, error_messages)
        """
        if not messages:
            return 0, 0, []
        
        # SQS batch limit is 10 messages
        batch_size = min(len(messages), self.config.BATCH_SIZE)
        successful_count = 0
        failed_count = 0
        all_errors = []
        
        # Process in batches
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            success, fail, errors = self._send_batch(batch)
            successful_count += success
            failed_count += fail
            all_errors.extend(errors)
        
        logger.info(f"Batch send completed: {successful_count} successful, {failed_count} failed")
        return successful_count, failed_count, all_errors
    
    def _send_batch(self, batch: List[Dict]) -> Tuple[int, int, List[str]]:
        """Send a single batch of messages"""
        try:
            # Prepare batch entries
            entries = []
            for idx, msg in enumerate(batch):
                message_id = f"msg_{int(time.time())}_{idx}"
                deduplication_id = self._generate_deduplication_id(msg['body'], msg['group_id'])
                
                entry = {
                    'Id': message_id,
                    'MessageBody': msg['body'],
                    'MessageGroupId': msg['group_id'],
                    'MessageDeduplicationId': deduplication_id
                }
                
                # Add attributes if present
                if msg.get('attributes'):
                    entry['MessageAttributes'] = self._format_message_attributes(msg['attributes'])
                
                entries.append(entry)
            
            # Send batch with retry
            response = self._send_batch_with_retry(entries)
            
            if response:
                successful = len(response.get('Successful', []))
                failed_entries = response.get('Failed', [])
                failed = len(failed_entries)
                
                errors = [f"Message {entry['Id']}: {entry.get('Message', 'Unknown error')}" 
                         for entry in failed_entries]
                
                return successful, failed, errors
            else:
                return 0, len(batch), [f"Batch send failed after retries"]
                
        except Exception as e:
            logger.error(f"Batch send error: {str(e)}")
            return 0, len(batch), [f"Batch processing error: {str(e)}"]
    
    def _send_with_retry(self, message_params: Dict) -> bool:
        """Send single message with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.MAX_RETRY_ATTEMPTS + 1):
            try:
                response = self.sqs_client.send_message(**message_params)
                
                if response.get('MessageId'):
                    if attempt > 0:
                        logger.info(f"Message sent successfully on attempt {attempt + 1}")
                    return True
                else:
                    logger.warning("Message sent but no MessageId in response")
                    return False
                    
            except ClientError as e:
                last_exception = e
                error_code = e.response['Error']['Code']
                
                # Don't retry certain errors
                if error_code in ['InvalidParameterValue', 'MissingParameter']:
                    logger.error(f"Non-retryable error: {str(e)}")
                    return False
                
                if attempt < self.config.MAX_RETRY_ATTEMPTS:
                    delay = min(
                        self.config.RETRY_DELAY_BASE * (2 ** attempt),
                        self.config.RETRY_DELAY_MAX
                    )
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay}s: {str(e)}")
                    time.sleep(delay)
                else:
                    logger.error(f"All retry attempts failed: {str(e)}")
                    
            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                if attempt >= self.config.MAX_RETRY_ATTEMPTS:
                    break
                time.sleep(1)  # Short delay for unexpected errors
        
        return False
    
    def _send_batch_with_retry(self, entries: List[Dict]) -> Optional[Dict]:
        """Send message batch with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.MAX_RETRY_ATTEMPTS + 1):
            try:
                response = self.sqs_client.send_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )
                
                if attempt > 0:
                    logger.info(f"Batch sent successfully on attempt {attempt + 1}")
                
                return response
                
            except ClientError as e:
                last_exception = e
                error_code = e.response['Error']['Code']
                
                # Don't retry certain errors
                if error_code in ['InvalidParameterValue', 'MissingParameter']:
                    logger.error(f"Non-retryable batch error: {str(e)}")
                    return None
                
                if attempt < self.config.MAX_RETRY_ATTEMPTS:
                    delay = min(
                        self.config.RETRY_DELAY_BASE * (2 ** attempt),
                        self.config.RETRY_DELAY_MAX
                    )
                    logger.warning(f"Batch attempt {attempt + 1} failed, retrying in {delay}s: {str(e)}")
                    time.sleep(delay)
                else:
                    logger.error(f"All batch retry attempts failed: {str(e)}")
                    
            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected batch error on attempt {attempt + 1}: {str(e)}")
                if attempt >= self.config.MAX_RETRY_ATTEMPTS:
                    break
                time.sleep(1)
        
        return None
    
    def _generate_deduplication_id(self, message_body: str, message_group_id: str) -> str:
        """Generate deduplication ID based on message content and timestamp"""
        # Use message content + group ID + minute timestamp for deduplication
        # This allows same message to be sent if it's in different minute
        timestamp_minute = int(time.time() // 60) * 60
        content = f"{message_body}_{message_group_id}_{timestamp_minute}"
        
        # Generate SHA-256 hash and take first 128 chars (SQS limit)
        hash_obj = hashlib.sha256(content.encode('utf-8'))
        return hash_obj.hexdigest()[:128]
    
    def _format_message_attributes(self, attributes: Dict) -> Dict:
        """Format message attributes for SQS"""
        formatted = {}
        
        for key, value in attributes.items():
            if isinstance(value, str):
                formatted[key] = {
                    'StringValue': value,
                    'DataType': 'String'
                }
            elif isinstance(value, (int, float)):
                formatted[key] = {
                    'StringValue': str(value),
                    'DataType': 'Number'
                }
            elif isinstance(value, bool):
                formatted[key] = {
                    'StringValue': str(value).lower(),
                    'DataType': 'String'
                }
            else:
                # Convert complex types to JSON string
                formatted[key] = {
                    'StringValue': json.dumps(value),
                    'DataType': 'String'
                }
        
        return formatted
    
    def get_queue_info(self) -> Dict:
        """Get queue information and statistics"""
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=[
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible',
                    'ApproximateNumberOfMessagesDelayed',
                    'CreatedTimestamp',
                    'LastModifiedTimestamp'
                ]
            )
            
            attributes = response['Attributes']
            
            return {
                'queue_name': self.config.SQS_QUEUE_NAME,
                'queue_url': self.queue_url,
                'messages_available': int(attributes.get('ApproximateNumberOfMessages', 0)),
                'messages_in_flight': int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0)),
                'messages_delayed': int(attributes.get('ApproximateNumberOfMessagesDelayed', 0)),
                'created_timestamp': attributes.get('CreatedTimestamp'),
                'last_modified': attributes.get('LastModifiedTimestamp')
            }
            
        except Exception as e:
            logger.error(f"Failed to get queue info: {str(e)}")
            return {
                'error': str(e),
                'queue_name': self.config.SQS_QUEUE_NAME,
                'queue_url': self.queue_url
            }
    
    def health_check(self) -> bool:
        """Check if SQS client is healthy"""
        try:
            # Try to get queue attributes as a simple health check
            self.sqs_client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            return True
        except Exception as e:
            logger.error(f"SQS health check failed: {str(e)}")
            return False