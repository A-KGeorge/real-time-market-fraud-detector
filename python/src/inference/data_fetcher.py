"""
Data fetcher for real-time market data using SQS consumer with yfinance fallback
"""

import logging
import pandas as pd
import yfinance as yf
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import numpy as np
from .config import Config
from .sqs_consumer import SQSConsumer

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self):
        self.cache: Dict[str, pd.DataFrame] = {}
        self.cache_expiry: Dict[str, datetime] = {}
        self.cache_duration = timedelta(minutes=5)  # Cache for 5 minutes
        
        # Initialize SQS consumer if enabled
        self.use_sqs = Config.USE_SQS_DATA_SOURCE
        self.sqs_consumer = None
        self.fallback_enabled = Config.FALLBACK_TO_YFINANCE
        
        if self.use_sqs:
            try:
                self.sqs_consumer = SQSConsumer()
                logger.info("SQS consumer initialized for data fetching")
            except Exception as e:
                logger.error(f"Failed to initialize SQS consumer: {str(e)}")
                if not self.fallback_enabled:
                    raise
                logger.warning("SQS initialization failed, will use yfinance fallback")
        
    def fetch_stock_data(self, symbol: str, period: str = None, interval: str = None) -> Optional[pd.DataFrame]:
        """
        Fetch stock data for a given symbol using SQS consumer (primary) or yfinance (fallback)
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period: Data period (used for yfinance fallback)
            interval: Data interval (used for yfinance fallback)
            
        Returns:
            DataFrame with OHLCV data or None if failed
        """
        try:
            # Try SQS first if enabled
            if self.use_sqs and self.sqs_consumer:
                sqs_data = self._fetch_from_sqs(symbol)
                if sqs_data is not None:
                    return sqs_data
                
                # If SQS fails and no fallback, return None
                if not self.fallback_enabled:
                    logger.warning(f"No SQS data for {symbol} and fallback disabled")
                    return None
                
                logger.debug(f"No SQS data for {symbol}, falling back to yfinance")
            
            # Fallback to yfinance
            return self._fetch_from_yfinance(symbol, period, interval)
            
        except Exception as e:
            logger.error(f"Failed to fetch data for {symbol}: {str(e)}")
            return None
    
    def _fetch_from_sqs(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Fetch data from SQS consumer
        
        Args:
            symbol: Stock symbol
            
        Returns:
            DataFrame with latest data or None
        """
        try:
            logger.debug(f"Fetching data for {symbol} from SQS")
            
            # Get latest data from SQS
            latest_series = self.sqs_consumer.get_latest_data_for_symbol(symbol)
            
            if latest_series is None:
                logger.debug(f"No SQS data available for {symbol}")
                return None
            
            # Convert Series to DataFrame format expected by downstream
            df = latest_series.to_frame().T
            df = df.reset_index(drop=True)
            
            # Ensure consistent column naming
            df = self._standardize_columns(df)
            
            logger.info(f"Successfully fetched SQS data for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch SQS data for {symbol}: {str(e)}")
            return None
    
    def _fetch_from_yfinance(self, symbol: str, period: str = None, interval: str = None) -> Optional[pd.DataFrame]:
        """
        Fetch stock data for a given symbol using yfinance
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period: Data period (default from config)
            interval: Data interval (default from config)
            
        Returns:
            DataFrame with OHLCV data or None if failed
        """
        try:
            # Use defaults from config if not provided
            if period is None:
                period = Config.YFINANCE_PERIOD
            if interval is None:
                interval = Config.YFINANCE_INTERVAL
                
            cache_key = f"{symbol}_{period}_{interval}"
            
            # Check cache first
            if self._is_cache_valid(cache_key):
                logger.debug(f"Using cached data for {symbol}")
                return self.cache[cache_key].copy()
            
            logger.info(f"Fetching data for {symbol} (period={period}, interval={interval})")
            
            # Fetch data from yfinance
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval=interval)
            
            if data.empty:
                logger.warning(f"No data received for {symbol}")
                return None
                
            # Clean and prepare data
            data = self._clean_data(data, symbol)
            
            # Update cache
            self.cache[cache_key] = data.copy()
            self.cache_expiry[cache_key] = datetime.now() + self.cache_duration
            
            logger.info(f"Successfully fetched {len(data)} records for {symbol}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch yfinance data for {symbol}: {str(e)}")
            return None
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names and format for consistency
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            Standardized DataFrame
        """
        try:
            # Create a copy to avoid modifying original
            df = df.copy()
            
            # Ensure we have required columns with proper case
            column_mapping = {
                'open': 'Open',
                'high': 'High', 
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume',
                'adj close': 'Adj Close',
                'adjclose': 'Adj Close'
            }
            
            # Apply column mapping (case insensitive)
            for old_col in df.columns:
                lower_col = old_col.lower()
                if lower_col in column_mapping:
                    new_col = column_mapping[lower_col]
                    if old_col != new_col:
                        df[new_col] = df[old_col]
                        if old_col != new_col:  # Only drop if names are different
                            df = df.drop(columns=[old_col])
            
            # Ensure Volume is integer if present
            if 'Volume' in df.columns:
                df['Volume'] = df['Volume'].astype(int)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to standardize columns: {str(e)}")
            return df
    
    def fetch_latest_data(self, symbol: str) -> Optional[pd.Series]:
        """
        Fetch the latest available data point for a symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Series with latest data or None if failed
        """
        try:
            # Try SQS first if enabled
            if self.use_sqs and self.sqs_consumer:
                sqs_data = self.sqs_consumer.get_latest_data_for_symbol(symbol)
                if sqs_data is not None:
                    return sqs_data
                
                # If SQS fails and no fallback, return None
                if not self.fallback_enabled:
                    logger.warning(f"No SQS data for {symbol} and fallback disabled")
                    return None
                
                logger.debug(f"No SQS data for {symbol}, falling back to yfinance")
            
            # Fallback to yfinance
            data = self._fetch_from_yfinance(symbol, period='1d', interval='1m')
            if data is None or data.empty:
                return None
                
            # Return the most recent row
            latest = data.iloc[-1].copy()
            latest['Symbol'] = symbol
            latest['Date'] = data.index[-1] if hasattr(data.index[-1], 'to_pydatetime') else data.index[-1]
            
            return latest
            
        except Exception as e:
            logger.error(f"Failed to fetch latest data for {symbol}: {str(e)}")
            return None
    
    def fetch_batch_data(self, symbols: List[str]) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch latest data for multiple symbols
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dictionary mapping symbols to their latest data
        """
        try:
            # Try SQS first if enabled
            if self.use_sqs and self.sqs_consumer:
                sqs_results = self.sqs_consumer.get_batch_data(symbols)
                
                # Check if we got data for all symbols from SQS
                all_symbols_found = all(sqs_results.get(symbol) is not None for symbol in symbols)
                
                if all_symbols_found or not self.fallback_enabled:
                    return sqs_results
                
                # If some symbols missing and fallback enabled, fill missing with yfinance
                if self.fallback_enabled:
                    logger.debug("Some symbols missing from SQS, using yfinance fallback for missing ones")
                    for symbol in symbols:
                        if sqs_results.get(symbol) is None:
                            try:
                                fallback_data = self.fetch_latest_data(symbol)
                                sqs_results[symbol] = fallback_data
                            except Exception as e:
                                logger.error(f"Fallback failed for {symbol}: {str(e)}")
                                sqs_results[symbol] = None
                    return sqs_results
            
            # Full fallback to yfinance
            results = {}
            for symbol in symbols:
                try:
                    results[symbol] = self.fetch_latest_data(symbol)
                except Exception as e:
                    logger.error(f"Failed to fetch data for {symbol}: {str(e)}")
                    results[symbol] = None
                    
            return results
            
        except Exception as e:
            logger.error(f"Failed to fetch batch data: {str(e)}")
            return {symbol: None for symbol in symbols}
    
    def _clean_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Clean and standardize the data"""
        # Reset index to make Date a column
        data = data.reset_index()
        
        # Add symbol column
        data['Symbol'] = symbol
        
        # Ensure we have the required columns
        required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        for col in required_columns:
            if col not in data.columns:
                logger.error(f"Missing required column: {col}")
                return pd.DataFrame()  # Return empty DataFrame
        
        # Remove any rows with NaN values in critical columns
        data = data.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'])
        
        # Ensure Volume is integer
        data['Volume'] = data['Volume'].astype(int)
        
        # Sort by date
        data = data.sort_values('Date').reset_index(drop=True)
        
        return data
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid"""
        if cache_key not in self.cache:
            return False
            
        if cache_key not in self.cache_expiry:
            return False
            
        return datetime.now() < self.cache_expiry[cache_key]
    
    def clear_cache(self):
        """Clear all cached data"""
        self.cache.clear()
        self.cache_expiry.clear()
        logger.info("Data cache cleared")
    
    def get_cache_info(self) -> Dict:
        """Get information about cached data and SQS consumer"""
        cache_info = {
            'cached_symbols': list(self.cache.keys()),
            'cache_count': len(self.cache),
            'cache_expiry_times': {k: v.isoformat() for k, v in self.cache_expiry.items()},
            'use_sqs': self.use_sqs,
            'fallback_enabled': self.fallback_enabled
        }
        
        # Add SQS consumer stats if available
        if self.sqs_consumer:
            try:
                sqs_stats = self.sqs_consumer.get_consumer_stats()
                cache_info['sqs_stats'] = sqs_stats
            except Exception as e:
                cache_info['sqs_stats'] = {'error': str(e)}
        
        return cache_info
    
    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate if a symbol exists and has data
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if symbol is valid and has data
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Check if we got valid info
            if not info or 'symbol' not in info:
                return False
                
            # Try to fetch a small amount of data
            test_data = ticker.history(period='1d', interval='1m')
            return not test_data.empty
            
        except Exception as e:
            logger.debug(f"Symbol validation failed for {symbol}: {str(e)}")
            return False
    
    def get_market_status(self) -> Dict:
        """
        Get current market status (this is a simple implementation)
        
        Returns:
            Dictionary with market status information
        """
        try:
            # Use SPY as a proxy for market status
            spy = yf.Ticker("SPY")
            data = spy.history(period='1d', interval='1m')
            
            if data.empty:
                return {'status': 'UNKNOWN', 'last_update': None}
            
            last_update = data.index[-1]
            current_time = datetime.now()
            
            # Simple heuristic: if last data is more than 30 minutes old, market might be closed
            time_diff = current_time - last_update.to_pydatetime().replace(tzinfo=None)
            
            if time_diff > timedelta(minutes=30):
                status = 'CLOSED'
            else:
                status = 'OPEN'
                
            return {
                'status': status,
                'last_update': last_update.isoformat(),
                'time_diff_minutes': time_diff.total_seconds() / 60
            }
            
        except Exception as e:
            logger.error(f"Failed to get market status: {str(e)}")
            return {'status': 'ERROR', 'error': str(e)}