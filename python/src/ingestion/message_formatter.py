"""
Message formatter for market surveillance SQS messages
Converts pandas DataFrames to structured JSON for downstream consumption
"""

import json
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from ..shared.config import Config

logger = logging.getLogger(__name__)

class MessageFormatter:
    def __init__(self):
        self.config = Config
        
    def format_market_data_message(self, symbol: str, data: pd.DataFrame, 
                                 message_type: str = "MARKET_DATA") -> Dict[str, Any]:
        """
        Format market data DataFrame into structured SQS message
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            data: DataFrame with OHLCV data
            message_type: Type of message (default: "MARKET_DATA")
            
        Returns:
            Dictionary representing the structured message
        """
        try:
            if data.empty:
                logger.warning(f"Empty data received for symbol {symbol}")
                return self._create_error_message(symbol, "No market data available")
            
            # Get the latest data point
            latest_data = data.iloc[-1].copy()
            
            # Create base message structure
            message = {
                "messageType": message_type,
                "version": "1.0",
                "timestamp": self._get_timestamp(),
                "source": "yahoo_finance",
                "symbol": symbol.upper(),
                "data": self._format_ohlcv_data(latest_data),
                "metadata": self._create_metadata(symbol, data),
                "schema": self._get_message_schema()
            }
            
            # Add historical context if available
            if len(data) > 1:
                message["historicalContext"] = self._create_historical_context(data)
            
            return message
            
        except Exception as e:
            logger.error(f"Failed to format message for {symbol}: {str(e)}")
            return self._create_error_message(symbol, f"Formatting error: {str(e)}")
    
    def format_batch_message(self, batch_data: Dict[str, Optional[pd.Series]], 
                           message_type: str = "BATCH_MARKET_DATA") -> Dict[str, Any]:
        """
        Format batch market data into structured SQS message
        
        Args:
            batch_data: Dictionary mapping symbols to their latest data
            message_type: Type of message (default: "BATCH_MARKET_DATA")
            
        Returns:
            Dictionary representing the batch message
        """
        try:
            successful_symbols = []
            failed_symbols = []
            market_data = {}
            
            # Process each symbol
            for symbol, series_data in batch_data.items():
                if series_data is not None and not series_data.empty:
                    try:
                        market_data[symbol] = self._format_series_data(series_data)
                        successful_symbols.append(symbol)
                    except Exception as e:
                        logger.error(f"Failed to format data for {symbol}: {str(e)}")
                        failed_symbols.append(symbol)
                else:
                    failed_symbols.append(symbol)
            
            # Create batch message
            message = {
                "messageType": message_type,
                "version": "1.0", 
                "timestamp": self._get_timestamp(),
                "source": "yahoo_finance",
                "batchInfo": {
                    "totalSymbols": len(batch_data),
                    "successfulSymbols": len(successful_symbols),
                    "failedSymbols": len(failed_symbols),
                    "successfulSymbolsList": successful_symbols,
                    "failedSymbolsList": failed_symbols
                },
                "data": market_data,
                "schema": self._get_batch_message_schema()
            }
            
            return message
            
        except Exception as e:
            logger.error(f"Failed to format batch message: {str(e)}")
            return {
                "messageType": "ERROR",
                "timestamp": self._get_timestamp(),
                "error": f"Batch formatting error: {str(e)}"
            }
    
    def _format_ohlcv_data(self, data: pd.Series) -> Dict[str, Any]:
        """Format OHLCV data from pandas Series"""
        try:
            # Extract OHLCV values
            ohlcv = {
                "open": self._safe_float(data.get('Open')),
                "high": self._safe_float(data.get('High')),
                "low": self._safe_float(data.get('Low')),
                "close": self._safe_float(data.get('Close')),
                "volume": self._safe_int(data.get('Volume')),
                "date": self._format_date(data.get('Date')),
                "adjustedClose": self._safe_float(data.get('Adj Close')),  # If available
            }
            
            # Calculate derived metrics
            derived_metrics = self._calculate_basic_metrics(ohlcv)
            ohlcv.update(derived_metrics)
            
            return ohlcv
            
        except Exception as e:
            logger.error(f"Error formatting OHLCV data: {str(e)}")
            return {
                "error": f"OHLCV formatting error: {str(e)}",
                "raw_data": str(data.to_dict()) if hasattr(data, 'to_dict') else str(data)
            }
    
    def _format_series_data(self, series_data: pd.Series) -> Dict[str, Any]:
        """Format pandas Series data for batch processing"""
        try:
            formatted = {}
            
            # Convert all series data to appropriate types
            for key, value in series_data.items():
                if key in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
                    formatted[key.lower().replace(' ', '_')] = self._safe_float(value)
                elif key == 'Volume':
                    formatted['volume'] = self._safe_int(value)
                elif key == 'Date':
                    formatted['date'] = self._format_date(value)
                elif key == 'Symbol':
                    formatted['symbol'] = str(value).upper()
                else:
                    # Handle any additional fields
                    formatted[key.lower()] = self._safe_convert(value)
            
            # Calculate basic derived metrics
            if all(k in formatted for k in ['open', 'high', 'low', 'close', 'volume']):
                derived = self._calculate_basic_metrics(formatted)
                formatted.update(derived)
            
            return formatted
            
        except Exception as e:
            logger.error(f"Error formatting series data: {str(e)}")
            return {
                "error": f"Series formatting error: {str(e)}",
                "raw_data": str(series_data.to_dict()) if hasattr(series_data, 'to_dict') else str(series_data)
            }
    
    def _calculate_basic_metrics(self, ohlcv: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate basic trading metrics"""
        try:
            metrics = {}
            
            open_price = ohlcv.get('open', 0)
            high_price = ohlcv.get('high', 0)
            low_price = ohlcv.get('low', 0)
            close_price = ohlcv.get('close', 0)
            volume = ohlcv.get('volume', 0)
            
            if close_price and open_price:
                # Price change metrics
                metrics['priceChange'] = close_price - open_price
                metrics['priceChangePercent'] = ((close_price - open_price) / open_price) * 100
                
                # Range metrics
                if high_price and low_price:
                    metrics['dayRange'] = high_price - low_price
                    metrics['dayRangePercent'] = ((high_price - low_price) / close_price) * 100
                
                # Volume metrics
                if volume:
                    metrics['volumePriceRatio'] = volume / close_price
                    metrics['avgPrice'] = (high_price + low_price + close_price) / 3
                    metrics['typicalPrice'] = (high_price + low_price + close_price) / 3
                    metrics['weightedPrice'] = ((high_price + low_price + 2 * close_price) / 4)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating basic metrics: {str(e)}")
            return {}
    
    def _create_metadata(self, symbol: str, data: pd.DataFrame) -> Dict[str, Any]:
        """Create metadata for the message"""
        return {
            "symbol": symbol.upper(),
            "recordCount": len(data),
            "dataSource": "yahoo_finance",
            "fetchTimestamp": self._get_timestamp(),
            "processingTimestamp": self._get_timestamp(),
            "dataQuality": self._assess_data_quality(data),
            "timeRange": {
                "start": self._format_date(data.index.min()) if not data.empty else None,
                "end": self._format_date(data.index.max()) if not data.empty else None
            }
        }
    
    def _create_historical_context(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Create historical context from DataFrame"""
        try:
            if len(data) < 2:
                return {}
            
            # Get previous periods for comparison
            latest = data.iloc[-1]
            previous = data.iloc[-2] if len(data) >= 2 else latest
            
            context = {
                "periodsAvailable": len(data),
                "previousClose": self._safe_float(previous.get('Close')),
                "changeFromPrevious": self._safe_float(latest.get('Close', 0) - previous.get('Close', 0)),
                "changeFromPreviousPercent": 0
            }
            
            # Calculate percentage change
            if previous.get('Close', 0) != 0:
                context["changeFromPreviousPercent"] = (
                    (latest.get('Close', 0) - previous.get('Close', 0)) / previous.get('Close', 0) * 100
                )
            
            # Add volume comparison if available
            if 'Volume' in latest and 'Volume' in previous:
                context["volumeChangePercent"] = (
                    (latest.get('Volume', 0) - previous.get('Volume', 0)) / max(previous.get('Volume', 1), 1) * 100
                )
            
            return context
            
        except Exception as e:
            logger.error(f"Error creating historical context: {str(e)}")
            return {}
    
    def _assess_data_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Assess data quality metrics"""
        try:
            if data.empty:
                return {"status": "NO_DATA", "score": 0}
            
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing_columns = [col for col in required_columns if col not in data.columns]
            
            # Check for null values
            null_counts = data[required_columns].isnull().sum() if not missing_columns else {}
            
            # Calculate quality score (0-100)
            score = 100
            if missing_columns:
                score -= len(missing_columns) * 20
            
            for col, null_count in null_counts.items():
                if null_count > 0:
                    score -= (null_count / len(data)) * 20
            
            return {
                "status": "GOOD" if score >= 80 else "FAIR" if score >= 60 else "POOR",
                "score": max(0, score),
                "missingColumns": missing_columns,
                "nullCounts": dict(null_counts) if null_counts else {},
                "recordCount": len(data)
            }
            
        except Exception as e:
            logger.error(f"Error assessing data quality: {str(e)}")
            return {"status": "ERROR", "error": str(e)}
    
    def _create_error_message(self, symbol: str, error: str) -> Dict[str, Any]:
        """Create error message structure"""
        return {
            "messageType": "ERROR",
            "version": "1.0",
            "timestamp": self._get_timestamp(),
            "symbol": symbol.upper() if symbol else "UNKNOWN",
            "error": error,
            "source": "market_surveillance_producer"
        }
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        return datetime.now(timezone.utc).isoformat()
    
    def _format_date(self, date_value: Any) -> Optional[str]:
        """Format date value to ISO string"""
        try:
            if pd.isna(date_value):
                return None
            
            if isinstance(date_value, pd.Timestamp):
                return date_value.isoformat()
            elif isinstance(date_value, datetime):
                return date_value.isoformat()
            elif isinstance(date_value, str):
                return pd.to_datetime(date_value).isoformat()
            else:
                return str(date_value)
        except Exception:
            return None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        try:
            if pd.isna(value) or value is None:
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        try:
            if pd.isna(value) or value is None:
                return None
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_convert(self, value: Any) -> Any:
        """Safely convert value to JSON-serializable type"""
        try:
            if pd.isna(value) or value is None:
                return None
            elif isinstance(value, (np.integer, np.int64)):
                return int(value)
            elif isinstance(value, (np.floating, np.float64)):
                return float(value)
            elif isinstance(value, np.bool_):
                return bool(value)
            elif isinstance(value, pd.Timestamp):
                return value.isoformat()
            elif isinstance(value, datetime):
                return value.isoformat()
            else:
                return str(value)
        except Exception:
            return str(value)
    
    def _get_message_schema(self) -> Dict[str, str]:
        """Get message schema definition"""
        return {
            "messageType": "string",
            "version": "string",
            "timestamp": "ISO8601",
            "source": "string",
            "symbol": "string",
            "data": {
                "open": "number",
                "high": "number", 
                "low": "number",
                "close": "number",
                "volume": "integer",
                "date": "ISO8601",
                "priceChange": "number",
                "priceChangePercent": "number"
            },
            "metadata": "object",
            "historicalContext": "object"
        }
    
    def _get_batch_message_schema(self) -> Dict[str, str]:
        """Get batch message schema definition"""
        return {
            "messageType": "string",
            "version": "string",
            "timestamp": "ISO8601",
            "source": "string",
            "batchInfo": "object",
            "data": "object"
        }
    
    def to_json_string(self, message: Dict[str, Any]) -> str:
        """Convert message dictionary to JSON string"""
        try:
            return json.dumps(message, ensure_ascii=False, separators=(',', ':'))
        except Exception as e:
            logger.error(f"Failed to serialize message to JSON: {str(e)}")
            # Return error message as JSON
            error_msg = {
                "messageType": "SERIALIZATION_ERROR",
                "timestamp": self._get_timestamp(),
                "error": str(e)
            }
            return json.dumps(error_msg)