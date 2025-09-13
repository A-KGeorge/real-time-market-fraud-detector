"""
Feature engineering for market surveillance inference
Mirrors the feature engineering from training
"""

import logging
import pandas as pd
import numpy as np
import ta  # Technical Analysis library
from typing import Dict, List, Optional
from datetime import datetime
from .config import Config

logger = logging.getLogger(__name__)

class FeatureEngineer:
    def __init__(self, feature_columns: List[str]):
        self.feature_columns = feature_columns
        
    def engineer_features_for_inference(self, data: pd.DataFrame, symbol: str) -> Optional[Dict]:
        """
        Engineer features for a single symbol's data for real-time inference
        
        Args:
            data: DataFrame with OHLCV data
            symbol: Stock symbol
            
        Returns:
            Dictionary with engineered features or None if failed
        """
        try:
            if data.empty or len(data) < 2:
                logger.warning(f"Insufficient data for feature engineering: {len(data)} rows")
                return None
            
            # Make a copy and sort by date
            df = data.copy().sort_values('Date').reset_index(drop=True)
            
            # Add symbol if not present
            if 'Symbol' not in df.columns:
                df['Symbol'] = symbol
            
            # Engineer all features
            df = self._engineer_all_features(df)
            
            # Get the latest row with features
            latest_row = df.iloc[-1]
            
            # Extract feature values in the correct order
            features = {}
            for col in self.feature_columns:
                if col in latest_row:
                    value = latest_row[col]
                    # Handle NaN values
                    if pd.isna(value) or np.isinf(value):
                        features[col] = 0.0
                    else:
                        features[col] = float(value)
                else:
                    logger.warning(f"Missing feature column: {col}")
                    features[col] = 0.0
            
            # Add market data for response
            features['_market_data'] = {
                'open': float(latest_row.get('Open', 0)),
                'high': float(latest_row.get('High', 0)),
                'low': float(latest_row.get('Low', 0)),
                'close': float(latest_row.get('Close', 0)),
                'volume': int(latest_row.get('Volume', 0)),
                'price_change': float(features.get('Price_Change', 0)),
                'volume_ratio': float(features.get('Volume_Ratio', 1)),
                'rsi': float(features.get('RSI', 50)),
                'macd': float(features.get('MACD', 0)),
                'bb_position': float(features.get('BB_Position', 0.5))
            }
            
            return features
            
        except Exception as e:
            logger.error(f"Feature engineering failed for {symbol}: {str(e)}")
            return None
    
    def _engineer_all_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer all features (mirrors training feature engineering)"""
        
        # Basic price features
        df['Price_Change'] = df['Close'].pct_change()
        df['Price_Range'] = (df['High'] - df['Low']) / df['Close']
        df['Gap'] = (df['Open'] - df['Close'].shift(1)) / df['Close'].shift(1)
        
        # Volume features
        df['Volume_Change'] = df['Volume'].pct_change()
        df['Volume_Price_Ratio'] = df['Volume'] / df['Close']
        df['Volume_MA'] = df['Volume'].rolling(window=min(20, len(df)), min_periods=1).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_MA']
        
        # Technical indicators using ta library
        try:
            # RSI (Relative Strength Index)
            df['RSI'] = ta.momentum.RSIIndicator(df['Close']).rsi()
            
            # MACD
            macd_indicator = ta.trend.MACD(df['Close'])
            df['MACD'] = macd_indicator.macd()
            df['MACD_Signal'] = macd_indicator.macd_signal()
            df['MACD_Histogram'] = macd_indicator.macd_diff()
            
            # Bollinger Bands
            bb_indicator = ta.volatility.BollingerBands(df['Close'])
            df['BB_Upper'] = bb_indicator.bollinger_hband()
            df['BB_Lower'] = bb_indicator.bollinger_lband()
            df['BB_Middle'] = bb_indicator.bollinger_mavg()
            df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Middle']
            df['BB_Position'] = ((df['Close'] - df['BB_Lower']) / 
                               (df['BB_Upper'] - df['BB_Lower'])).fillna(0.5)
            
            # Moving averages
            df['SMA_10'] = df['Close'].rolling(window=min(10, len(df)), min_periods=1).mean()
            df['SMA_20'] = df['Close'].rolling(window=min(20, len(df)), min_periods=1).mean()
            df['EMA_12'] = df['Close'].ewm(span=min(12, len(df))).mean()
            df['EMA_26'] = df['Close'].ewm(span=min(26, len(df))).mean()
            
        except Exception as e:
            logger.warning(f"Technical indicator calculation failed: {str(e)}")
            # Fill with default values if technical indicators fail
            for col in ['RSI', 'MACD', 'MACD_Signal', 'MACD_Histogram', 
                       'BB_Upper', 'BB_Lower', 'BB_Middle', 'BB_Width', 'BB_Position',
                       'SMA_10', 'SMA_20', 'EMA_12', 'EMA_26']:
                if col not in df.columns:
                    if col == 'RSI':
                        df[col] = 50.0  # Neutral RSI
                    elif col == 'BB_Position':
                        df[col] = 0.5   # Middle of BB
                    else:
                        df[col] = 0.0
        
        # Price position relative to moving averages
        df['Price_vs_SMA10'] = df['Close'] / df['SMA_10'] - 1
        df['Price_vs_SMA20'] = df['Close'] / df['SMA_20'] - 1
        
        # Volatility measures
        df['Volatility_10d'] = df['Price_Change'].rolling(window=min(10, len(df)), min_periods=1).std()
        df['Volatility_20d'] = df['Price_Change'].rolling(window=min(20, len(df)), min_periods=1).std()
        
        # Time-based features
        df['Date'] = pd.to_datetime(df['Date'])
        df['Hour'] = df['Date'].dt.hour
        df['DayOfWeek'] = df['Date'].dt.dayofweek
        df['IsMonday'] = (df['DayOfWeek'] == 0).astype(int)
        df['IsFriday'] = (df['DayOfWeek'] == 4).astype(int)
        
        # Replace inf/-inf with NaN, then fill NaN with appropriate values
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # Fill NaN values with reasonable defaults
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if col in ['RSI']:
                df[col] = df[col].fillna(50.0)  # Neutral RSI
            elif col in ['BB_Position']:
                df[col] = df[col].fillna(0.5)   # Middle of Bollinger Bands
            elif col in ['Volume_Ratio']:
                df[col] = df[col].fillna(1.0)   # Normal volume
            elif 'Change' in col or 'Gap' in col:
                df[col] = df[col].fillna(0.0)   # No change
            else:
                df[col] = df[col].fillna(df[col].mean() if not df[col].isna().all() else 0.0)
        
        return df
    
    def get_feature_vector(self, features_dict: Dict) -> np.ndarray:
        """
        Convert features dictionary to numpy array in correct order
        
        Args:
            features_dict: Dictionary with feature values
            
        Returns:
            Numpy array with features in correct order
        """
        feature_vector = []
        
        for col in self.feature_columns:
            value = features_dict.get(col, 0.0)
            
            # Handle any remaining NaN or inf values
            if pd.isna(value) or np.isinf(value):
                value = 0.0
                
            feature_vector.append(float(value))
        
        return np.array([feature_vector])  # Shape: (1, n_features)
    
    def generate_alerts(self, features_dict: Dict, ensemble_score: float, 
                       classifier_prob: float, anomaly_score: int) -> List[str]:
        """
        Generate specific alerts based on feature values and predictions
        
        Args:
            features_dict: Engineered features
            ensemble_score: Ensemble prediction score
            classifier_prob: Classifier probability
            anomaly_score: Anomaly detection score
            
        Returns:
            List of alert messages
        """
        alerts = []
        
        try:
            # Volume-based alerts
            volume_ratio = features_dict.get('Volume_Ratio', 1.0)
            if volume_ratio > 5.0:
                alerts.append(f"Extremely high volume: {volume_ratio:.2f}x normal")
            elif volume_ratio > 3.0:
                alerts.append(f"High volume detected: {volume_ratio:.2f}x normal")
            elif volume_ratio < 0.1:
                alerts.append(f"Unusually low volume: {volume_ratio:.2f}x normal")
            
            # Price movement alerts
            price_change = features_dict.get('Price_Change', 0.0)
            if abs(price_change) > 0.05:
                direction = "increase" if price_change > 0 else "decrease"
                alerts.append(f"Large price {direction}: {abs(price_change)*100:.2f}%")
            
            # Technical indicator alerts
            rsi = features_dict.get('RSI', 50.0)
            if rsi > 80:
                alerts.append(f"Overbought condition (RSI: {rsi:.1f})")
            elif rsi < 20:
                alerts.append(f"Oversold condition (RSI: {rsi:.1f})")
            
            # Bollinger Bands alerts
            bb_position = features_dict.get('BB_Position', 0.5)
            if bb_position > 0.95:
                alerts.append("Price near upper Bollinger Band (potential reversal)")
            elif bb_position < 0.05:
                alerts.append("Price near lower Bollinger Band (potential reversal)")
            
            # Volatility alerts
            volatility_10d = features_dict.get('Volatility_10d', 0.0)
            if volatility_10d > 0.05:
                alerts.append(f"High volatility detected: {volatility_10d*100:.2f}%")
            
            # Gap trading alerts
            gap = features_dict.get('Gap', 0.0)
            if abs(gap) > 0.02:
                direction = "up" if gap > 0 else "down"
                alerts.append(f"Gap {direction}: {abs(gap)*100:.2f}%")
            
            # Model-specific alerts
            if ensemble_score > 0.8:
                alerts.append("HIGH RISK: Multiple fraud indicators detected")
            elif ensemble_score > 0.6:
                alerts.append("MEDIUM RISK: Suspicious trading pattern")
            
            if anomaly_score == 1:
                alerts.append("Anomaly detected: Trading pattern deviates from normal")
            
            if classifier_prob > 0.9:
                alerts.append("Classifier: Very high probability of suspicious activity")
                
        except Exception as e:
            logger.error(f"Alert generation failed: {str(e)}")
            alerts.append("Alert generation error - manual review recommended")
        
        return alerts