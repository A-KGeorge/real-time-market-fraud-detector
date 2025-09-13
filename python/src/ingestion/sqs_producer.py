"""
SQS Producer for Market Surveillance Data Ingestion
Fetches market data and sends to AWS SQS FIFO queues
"""

import logging
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from ..inference.data_fetcher import DataFetcher
from ..shared.aws_client import SQSClient
from ..shared.config import Config
from .message_formatter import MessageFormatter

logger = logging.getLogger(__name__)

class MarketDataProducer:
    def __init__(self):
        self.config = Config
        self.data_fetcher = DataFetcher()
        self.sqs_client = SQSClient()
        self.message_formatter = MessageFormatter()
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'batches_processed': 0,
            'last_run_time': None,
            'last_successful_run': None,
            'errors': []
        }
        
        # Thread safety
        self.stats_lock = threading.Lock()
        self.is_running = False
        
        logger.info("Market Data Producer initialized")
    
    def produce_single_symbol(self, symbol: str) -> bool:
        """
        Fetch and send market data for a single symbol
        
        Args:
            symbol: Stock symbol to process
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.debug(f"Producing data for symbol: {symbol}")
            
            # Fetch market data
            market_data = self.data_fetcher.fetch_stock_data(
                symbol, 
                period=self.config.YFINANCE_PERIOD,
                interval=self.config.YFINANCE_INTERVAL
            )
            
            if market_data is None or market_data.empty:
                logger.warning(f"No market data available for {symbol}")
                self._record_error(f"No data for {symbol}")
                return False
            
            # Format message
            message = self.message_formatter.format_market_data_message(symbol, market_data)
            message_body = self.message_formatter.to_json_string(message)
            
            # Create message group ID for FIFO ordering
            message_group_id = f"{self.config.MESSAGE_GROUP_ID_PREFIX}-{symbol.upper()}"
            
            # Add message attributes
            message_attributes = {
                'Symbol': symbol.upper(),
                'MessageType': 'MARKET_DATA',
                'Source': 'yahoo_finance',
                'Timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            # Send to SQS
            success = self.sqs_client.send_message(
                message_body=message_body,
                message_group_id=message_group_id,
                message_attributes=message_attributes
            )
            
            if success:
                self._update_stats(messages_sent=1)
                logger.debug(f"Successfully sent message for {symbol}")
                return True
            else:
                self._update_stats(messages_failed=1)
                self._record_error(f"Failed to send message for {symbol}")
                return False
                
        except Exception as e:
            error_msg = f"Error producing data for {symbol}: {str(e)}"
            logger.error(error_msg)
            self._record_error(error_msg)
            self._update_stats(messages_failed=1)
            return False
    
    def produce_batch(self, symbols: Optional[List[str]] = None) -> Dict[str, any]:
        """
        Fetch and send market data for multiple symbols in batch
        
        Args:
            symbols: List of symbols to process (uses default if None)
            
        Returns:
            Dictionary with batch processing results
        """
        try:
            if symbols is None:
                symbols = self.config.MARKET_DATA_SYMBOLS
            
            logger.info(f"Starting batch production for {len(symbols)} symbols")
            start_time = time.time()
            
            # Fetch batch data
            batch_data = self.data_fetcher.fetch_batch_data(symbols)
            
            # Prepare messages for batch send
            messages = []
            successful_symbols = []
            failed_symbols = []
            
            for symbol, data in batch_data.items():
                try:
                    if data is not None and not data.empty:
                        # Convert Series to DataFrame for message formatting
                        df = data.to_frame().T
                        df['Date'] = data.get('Date', datetime.now())
                        
                        # Format message
                        message = self.message_formatter.format_market_data_message(symbol, df)
                        message_body = self.message_formatter.to_json_string(message)
                        
                        # Create message for batch
                        batch_message = {
                            'body': message_body,
                            'group_id': f"{self.config.MESSAGE_GROUP_ID_PREFIX}-{symbol.upper()}",
                            'attributes': {
                                'Symbol': symbol.upper(),
                                'MessageType': 'MARKET_DATA',
                                'Source': 'yahoo_finance',
                                'BatchId': f"batch_{int(time.time())}",
                                'Timestamp': datetime.now(timezone.utc).isoformat()
                            }
                        }
                        
                        messages.append(batch_message)
                        successful_symbols.append(symbol)
                    else:
                        failed_symbols.append(symbol)
                        logger.warning(f"No data available for {symbol}")
                        
                except Exception as e:
                    failed_symbols.append(symbol)
                    error_msg = f"Failed to prepare message for {symbol}: {str(e)}"
                    logger.error(error_msg)
                    self._record_error(error_msg)
            
            # Send batch to SQS
            if messages:
                sent_count, failed_count, errors = self.sqs_client.send_message_batch(messages)
                
                # Update statistics
                self._update_stats(
                    messages_sent=sent_count,
                    messages_failed=failed_count,
                    batches_processed=1
                )
                
                # Record errors
                for error in errors:
                    self._record_error(f"Batch send error: {error}")
                    
            else:
                sent_count = failed_count = 0
                errors = ["No valid messages to send"]
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Create result summary
            result = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'processing_time_seconds': processing_time,
                'total_symbols_requested': len(symbols),
                'successful_data_fetch': len(successful_symbols),
                'failed_data_fetch': len(failed_symbols),
                'messages_sent': sent_count,
                'messages_failed': failed_count,
                'successful_symbols': successful_symbols,
                'failed_symbols': failed_symbols,
                'errors': errors
            }
            
            logger.info(f"Batch completed: {sent_count} sent, {failed_count} failed, {processing_time:.2f}s")
            self._update_stats(last_run_time=datetime.now(timezone.utc))
            
            if sent_count > 0:
                self._update_stats(last_successful_run=datetime.now(timezone.utc))
            
            return result
            
        except Exception as e:
            error_msg = f"Batch production failed: {str(e)}"
            logger.error(error_msg)
            self._record_error(error_msg)
            
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': error_msg,
                'total_symbols_requested': len(symbols) if symbols else 0,
                'messages_sent': 0,
                'messages_failed': len(symbols) if symbols else 0
            }
    
    def produce_parallel_batch(self, symbols: Optional[List[str]] = None, 
                             max_workers: int = 5) -> Dict[str, any]:
        """
        Produce market data for symbols in parallel for better performance
        
        Args:
            symbols: List of symbols to process
            max_workers: Maximum number of parallel workers
            
        Returns:
            Dictionary with batch processing results
        """
        try:
            if symbols is None:
                symbols = self.config.MARKET_DATA_SYMBOLS
            
            logger.info(f"Starting parallel batch production for {len(symbols)} symbols with {max_workers} workers")
            start_time = time.time()
            
            successful_symbols = []
            failed_symbols = []
            
            # Process symbols in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_symbol = {
                    executor.submit(self.produce_single_symbol, symbol): symbol 
                    for symbol in symbols
                }
                
                # Collect results
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        success = future.result(timeout=30)  # 30 second timeout per symbol
                        if success:
                            successful_symbols.append(symbol)
                        else:
                            failed_symbols.append(symbol)
                    except Exception as e:
                        failed_symbols.append(symbol)
                        error_msg = f"Parallel processing failed for {symbol}: {str(e)}"
                        logger.error(error_msg)
                        self._record_error(error_msg)
            
            # Calculate results
            processing_time = time.time() - start_time
            sent_count = len(successful_symbols)
            failed_count = len(failed_symbols)
            
            result = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'processing_time_seconds': processing_time,
                'parallel_workers': max_workers,
                'total_symbols_requested': len(symbols),
                'messages_sent': sent_count,
                'messages_failed': failed_count,
                'successful_symbols': successful_symbols,
                'failed_symbols': failed_symbols
            }
            
            self._update_stats(
                batches_processed=1,
                last_run_time=datetime.now(timezone.utc)
            )
            
            if sent_count > 0:
                self._update_stats(last_successful_run=datetime.now(timezone.utc))
            
            logger.info(f"Parallel batch completed: {sent_count} sent, {failed_count} failed, {processing_time:.2f}s")
            return result
            
        except Exception as e:
            error_msg = f"Parallel batch production failed: {str(e)}"
            logger.error(error_msg)
            self._record_error(error_msg)
            
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': error_msg,
                'total_symbols_requested': len(symbols) if symbols else 0,
                'messages_sent': 0,
                'messages_failed': len(symbols) if symbols else 0
            }
    
    def start_continuous_production(self, symbols: Optional[List[str]] = None,
                                  interval_seconds: Optional[int] = None) -> None:
        """
        Start continuous market data production
        
        Args:
            symbols: List of symbols to process
            interval_seconds: Interval between batches (uses config default if None)
        """
        if self.is_running:
            logger.warning("Producer is already running")
            return
        
        if symbols is None:
            symbols = self.config.MARKET_DATA_SYMBOLS
        
        if interval_seconds is None:
            interval_seconds = self.config.PRODUCER_INTERVAL
        
        logger.info(f"Starting continuous production for {len(symbols)} symbols every {interval_seconds}s")
        self.is_running = True
        
        try:
            while self.is_running:
                logger.info("Starting scheduled batch production...")
                
                # Run batch production
                result = self.produce_batch(symbols)
                
                # Log results
                if 'error' in result:
                    logger.error(f"Batch production failed: {result['error']}")
                else:
                    logger.info(f"Batch completed: {result['messages_sent']} sent, {result['messages_failed']} failed")
                
                # Wait for next interval
                if self.is_running:
                    logger.debug(f"Waiting {interval_seconds} seconds until next batch...")
                    time.sleep(interval_seconds)
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping producer...")
        except Exception as e:
            logger.error(f"Continuous production failed: {str(e)}")
            self._record_error(f"Continuous production error: {str(e)}")
        finally:
            self.is_running = False
            logger.info("Continuous production stopped")
    
    def stop_continuous_production(self) -> None:
        """Stop continuous production"""
        if self.is_running:
            logger.info("Stopping continuous production...")
            self.is_running = False
        else:
            logger.warning("Producer is not running")
    
    def get_statistics(self) -> Dict[str, any]:
        """Get producer statistics"""
        with self.stats_lock:
            stats_copy = self.stats.copy()
            
        # Add current status
        stats_copy.update({
            'is_running': self.is_running,
            'queue_info': self.sqs_client.get_queue_info(),
            'data_fetcher_cache_info': self.data_fetcher.get_cache_info(),
            'current_timestamp': datetime.now(timezone.utc).isoformat()
        })
        
        return stats_copy
    
    def health_check(self) -> Dict[str, any]:
        """Perform health check"""
        health = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'status': 'HEALTHY',
            'components': {}
        }
        
        try:
            # Check SQS client
            health['components']['sqs_client'] = {
                'status': 'HEALTHY' if self.sqs_client.health_check() else 'UNHEALTHY'
            }
            
            # Check data fetcher (simple test)
            try:
                test_data = self.data_fetcher.fetch_stock_data('AAPL', period='1d', interval='1h')
                health['components']['data_fetcher'] = {
                    'status': 'HEALTHY' if test_data is not None else 'UNHEALTHY',
                    'last_test_result': 'Success' if test_data is not None else 'No data'
                }
            except Exception as e:
                health['components']['data_fetcher'] = {
                    'status': 'UNHEALTHY',
                    'error': str(e)
                }
            
            # Check message formatter
            try:
                test_message = self.message_formatter.format_market_data_message('TEST', test_data if test_data is not None else None)
                health['components']['message_formatter'] = {
                    'status': 'HEALTHY' if test_message else 'UNHEALTHY'
                }
            except Exception as e:
                health['components']['message_formatter'] = {
                    'status': 'UNHEALTHY',
                    'error': str(e)
                }
            
            # Overall status
            component_statuses = [comp['status'] for comp in health['components'].values()]
            if all(status == 'HEALTHY' for status in component_statuses):
                health['status'] = 'HEALTHY'
            elif any(status == 'HEALTHY' for status in component_statuses):
                health['status'] = 'DEGRADED'
            else:
                health['status'] = 'UNHEALTHY'
                
        except Exception as e:
            health['status'] = 'UNHEALTHY'
            health['error'] = str(e)
        
        return health
    
    def _update_stats(self, **kwargs):
        """Thread-safe statistics update"""
        with self.stats_lock:
            for key, value in kwargs.items():
                if key in ['messages_sent', 'messages_failed', 'batches_processed']:
                    self.stats[key] += value
                else:
                    self.stats[key] = value
    
    def _record_error(self, error_msg: str):
        """Record error in statistics"""
        with self.stats_lock:
            self.stats['errors'].append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'message': error_msg
            })
            
            # Keep only last 100 errors
            if len(self.stats['errors']) > 100:
                self.stats['errors'] = self.stats['errors'][-100:]