#!/usr/bin/env python3
"""
Lambda-triggered inference entry point

This script is used when the Python service is triggered by Lambda
via ECS runTask. It processes a single market data record passed
via environment variables and then exits.
"""

import sys
import os
import json
import logging
from typing import Dict, Any

# Add the src directory to the path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from inference.model_loader import ModelLoader
from inference.feature_engineer import FeatureEngineer
from shared.config import Config

def setup_logging():
    """Configure logging for the service"""
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )

def process_lambda_triggered_data() -> bool:
    """
    Process market data passed from Lambda via environment variables
    Returns True if processing was successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    # Check if we're in Lambda-triggered mode
    processing_mode = os.getenv('PROCESSING_MODE')
    if processing_mode != 'LAMBDA_TRIGGERED':
        logger.error("This script should only be run in LAMBDA_TRIGGERED mode")
        return False
    
    # Check if we have batch data or single data
    batch_data_str = os.getenv('BATCH_MARKET_DATA')
    single_data_str = os.getenv('MARKET_DATA')
    
    if batch_data_str:
        logger.info("Processing batch market data")
        return process_batch_market_data(batch_data_str)
    elif single_data_str:
        logger.info("Processing single market data point")
        return process_single_market_data(single_data_str)
    else:
        logger.error("Neither BATCH_MARKET_DATA nor MARKET_DATA environment variable found")
        return False

def process_batch_market_data(batch_data_str: str) -> bool:
    """
    Process multiple market data points for richer feature engineering
    """
    logger = logging.getLogger(__name__)
    
    correlation_id = os.getenv('CORRELATION_ID', 'batch-unknown')
    target_symbol = os.getenv('TARGET_SYMBOL')
    
    logger.info(f"Processing batch Lambda-triggered inference for correlation_id: {correlation_id}")
    if target_symbol:
        logger.info(f"Target symbol: {target_symbol}")
    
    try:
        # Parse batch market data JSON (array of market data records)
        batch_data = json.loads(batch_data_str)
        logger.info(f"Successfully parsed batch data with {len(batch_data)} records")
        
        if not batch_data:
            logger.error("Empty batch data")
            return False
        
        # Group data by symbol
        symbol_data = {}
        for record in batch_data:
            data_dict = record.get('data', record)
            symbol = data_dict.get('symbol', 'UNKNOWN')
            
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(data_dict)
        
        logger.info(f"Grouped data into {len(symbol_data)} symbols: {list(symbol_data.keys())}")
        
        # Log detailed data point information
        total_data_points = sum(len(data_points) for data_points in symbol_data.values())
        logger.info(f"=== BATCH DATA POINTS SUMMARY ===")
        logger.info(f"Total data points across all symbols: {total_data_points}")
        for symbol, data_points in symbol_data.items():
            logger.info(f"  {symbol}: {len(data_points)} data points")
        logger.info("==================================")
        
        # Initialize ML components once
        logger.info("Initializing Market Surveillance Service...")
        model_loader = ModelLoader()
        
        if not model_loader.load_models():
            logger.error("Failed to load models")
            return False
        
        feature_engineer = FeatureEngineer(model_loader.feature_columns)
        logger.info("Market Surveillance Service initialized successfully")
        
        # Process each symbol with its accumulated data
        results = {}
        for symbol, data_points in symbol_data.items():
            logger.info(f"=== PROCESSING SYMBOL: {symbol} ===")
            logger.info(f"Processing {len(data_points)} data points for symbol: {symbol}")
            
            # Log details of each data point
            for i, dp in enumerate(data_points):
                timestamp = dp.get('timestamp', 'unknown')
                close_price = dp.get('close', 0)
                volume = dp.get('volume', 0)
                logger.info(f"  Data point {i+1}: timestamp={timestamp}, close={close_price}, volume={volume}")
            
            # Convert to DataFrame with proper column names and sorting
            import pandas as pd
            market_df = pd.DataFrame([{
                'Symbol': dp.get('symbol', symbol),
                'Close': dp.get('close', 0),
                'High': dp.get('high', 0),
                'Low': dp.get('low', 0),
                'Open': dp.get('open', 0),
                'Volume': dp.get('volume', 0),
                'Date': pd.to_datetime(dp.get('timestamp', pd.Timestamp.now())),
                'previous_close': dp.get('previous_close', 0),
            } for dp in data_points])
            
            # Sort by date to ensure proper time series order
            market_df = market_df.sort_values('Date').reset_index(drop=True)
            
            logger.info(f"Created DataFrame with {len(market_df)} rows for {symbol}")
            logger.info(f"Date range: {market_df['Date'].min()} to {market_df['Date'].max()}")
            logger.info(f"Price range: ${market_df['Close'].min():.2f} to ${market_df['Close'].max():.2f}")
            
            # Process fraud detection with multiple data points
            result = process_symbol_fraud_detection(
                symbol, market_df, model_loader, feature_engineer, is_batch_mode=True
            )
            
            results[symbol] = result
        
        # Log overall results
        logger.info("=== BATCH PROCESSING RESULTS ===")
        processed_symbols = 0
        total_processed_points = 0
        for symbol, result in results.items():
            data_points_count = len(symbol_data[symbol])
            total_processed_points += data_points_count
            processed_symbols += 1
            if result:
                logger.info(f"{symbol} ({data_points_count} points): Risk={result['risk_level']}, Score={result['ensemble_score']:.6f}")
            else:
                logger.error(f"{symbol} ({data_points_count} points): Processing failed")
        
        logger.info(f"SUMMARY: Processed {processed_symbols} symbols with {total_processed_points} total data points")
        logger.info(f"Successfully processed batch correlation_id: {correlation_id}")
        return True
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse batch market data JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"Error during batch processing: {e}")
        return False

def process_single_market_data(market_data_str: str) -> bool:
    """
    Process single market data point (legacy mode)
    """
    logger = logging.getLogger(__name__)
    
    correlation_id = os.getenv('CORRELATION_ID')
    target_symbol = os.getenv('TARGET_SYMBOL')
    
    if not correlation_id:
        logger.error("CORRELATION_ID environment variable is required")
        return False
    
    logger.info(f"Processing Lambda-triggered inference for correlation_id: {correlation_id}")
    if target_symbol:
        logger.info(f"Target symbol: {target_symbol}")
    
    try:
        # Parse market data JSON
        market_data = json.loads(market_data_str)
        logger.info(f"Successfully parsed market data: {market_data}")
        
        # Initialize components for Lambda mode (no SQS/external connections)
        logger.info("Initializing Market Surveillance Service...")
        model_loader = ModelLoader()
        
        # Load models first
        if not model_loader.load_models():
            logger.error("Failed to load models")
            return False
        
        # Initialize feature engineer with loaded feature columns
        feature_engineer = FeatureEngineer(model_loader.feature_columns)
        
        logger.info("Market Surveillance Service initialized successfully")
        
        # Extract symbol from market data
        symbol = market_data.get('data', {}).get('symbol') or market_data.get('symbol', 'UNKNOWN')
        
        logger.info(f"Processing fraud detection for symbol: {symbol}")
        
        # Convert market data to DataFrame for feature engineering
        # Assuming market_data has structure: {"data": {"symbol": "AAPL", "close": 245.5, ...}}
        data_dict = market_data.get('data', market_data)
        
        # Create a simple DataFrame with the market data point
        import pandas as pd
        market_df = pd.DataFrame([{
            'Symbol': data_dict.get('symbol', symbol),
            'Close': data_dict.get('close', 0),
            'High': data_dict.get('high', 0),
            'Low': data_dict.get('low', 0),
            'Open': data_dict.get('open', 0),
            'Volume': data_dict.get('volume', 0),
            'Date': pd.to_datetime(data_dict.get('timestamp', pd.Timestamp.now())),
            'previous_close': data_dict.get('previous_close', 0),
        }])
        
        logger.info(f"Created DataFrame with {len(market_df)} row(s)")
        
        result = process_symbol_fraud_detection(symbol, market_df, model_loader, feature_engineer, is_batch_mode=False)
        
        if result:
            logger.info(f"Successfully processed correlation_id: {correlation_id}")
            return True
        else:
            logger.error("Fraud detection processing failed")
            return False
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse market data JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        return False

def process_symbol_fraud_detection(symbol: str, market_df, model_loader, feature_engineer, is_batch_mode: bool = False) -> dict:
    """
    Common fraud detection logic for both single and batch processing
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Engineer features for inference
        features = feature_engineer.engineer_features_for_inference(market_df, symbol, is_batch_mode=is_batch_mode)
        if features is None:
            logger.error(f"Feature engineering failed for {symbol}")
            return None
            
        # Extract market data for response (if it exists)
        market_data_proto = features.pop('_market_data', {})
        
        # Convert features to numpy array and scale
        feature_vector = feature_engineer.get_feature_vector(features)
        scaled_features = model_loader.scale_features(feature_vector)
        
        # Run inference
        ensemble_scores, anomaly_scores, classifier_probs = model_loader.ensemble_predict(scaled_features)
        
        ensemble_score = float(ensemble_scores[0])
        anomaly_score = int(anomaly_scores[0])
        classifier_prob = float(classifier_probs[0])
        
        # Determine risk level
        if ensemble_score > 0.7:
            risk_level = "HIGH"
        elif ensemble_score > 0.3:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        # Generate alerts
        alerts = feature_engineer.generate_alerts(
            features, ensemble_score, classifier_prob, anomaly_score
        )
        
        # Log results
        logger.info(f"Fraud detection completed for {symbol}")
        logger.info(f"Ensemble Score: {ensemble_score}")
        logger.info(f"Classifier Probability: {classifier_prob}")
        logger.info(f"Anomaly Score: {anomaly_score}")
        logger.info(f"Is Suspicious: {ensemble_score > 0.5}")
        logger.info(f"Risk Level: {risk_level}")
        
        if alerts:
            logger.warning(f"Alerts generated: {list(alerts)}")
        
        return {
            'symbol': symbol,
            'ensemble_score': ensemble_score,
            'classifier_probability': classifier_prob,
            'anomaly_score': anomaly_score,
            'is_suspicious': ensemble_score > 0.5,
            'risk_level': risk_level,
            'alerts': alerts,
            'market_data': market_data_proto
        }
            
    except Exception as feature_error:
        logger.error(f"Error during feature engineering/inference for {symbol}: {feature_error}")
        # Create basic error response
        return {
            'symbol': symbol,
            'ensemble_score': 0.0,
            'classifier_probability': 0.0,
            'anomaly_score': 0,
            'is_suspicious': False,
            'risk_level': "ERROR",
            'alerts': [f"Error: {str(feature_error)}"],
            'market_data': {}
        }

if __name__ == "__main__":
    # Setup logging first
    setup_logging()
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting Lambda-triggered inference service")
        
        # Process the Lambda-triggered data
        success = process_lambda_triggered_data()
        
        if not success:
            logger.error("Processing failed")
            sys.exit(1)
        
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)