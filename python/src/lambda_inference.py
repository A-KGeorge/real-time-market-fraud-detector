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
from inference.grpc_server import MarketSurveillanceServicer
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
    
    # Get required environment variables
    correlation_id = os.getenv('CORRELATION_ID')
    market_data_str = os.getenv('MARKET_DATA')
    target_symbol = os.getenv('TARGET_SYMBOL')
    
    if not correlation_id:
        logger.error("CORRELATION_ID environment variable is required")
        return False
    
    if not market_data_str:
        logger.error("MARKET_DATA environment variable is required")
        return False
    
    logger.info(f"Processing Lambda-triggered inference for correlation_id: {correlation_id}")
    if target_symbol:
        logger.info(f"Target symbol: {target_symbol}")
    
    try:
        # Parse market data JSON
        market_data = json.loads(market_data_str)
        logger.info(f"Parsed market data: {market_data}")
        
        # Initialize components
        logger.info("Loading ML models...")
        model_loader = ModelLoader()
        feature_engineer = FeatureEngineer()
        
        # Create a temporary servicer instance for processing
        servicer = MarketSurveillanceServicer(
            model_loader=model_loader,
            feature_engineer=feature_engineer
        )
        
        # Extract symbol from market data
        symbol = market_data.get('data', {}).get('symbol') or market_data.get('symbol', 'UNKNOWN')
        
        logger.info(f"Processing fraud detection for symbol: {symbol}")
        
        # Perform fraud detection
        result = servicer.perform_fraud_detection(symbol, market_data)
        
        # Log results
        logger.info(f"Fraud detection completed for {symbol}")
        logger.info(f"Ensemble Score: {result.ensemble_score}")
        logger.info(f"Classifier Probability: {result.classifier_probability}")
        logger.info(f"Anomaly Score: {result.anomaly_score}")
        logger.info(f"Is Suspicious: {result.is_suspicious}")
        logger.info(f"Risk Level: {result.risk_level}")
        
        if result.alerts:
            logger.warning(f"Alerts generated: {list(result.alerts)}")
        
        # TODO: Here you could send results to:
        # 1. DynamoDB for storage
        # 2. SNS for notifications
        # 3. CloudWatch metrics
        # 4. Another SQS queue for downstream processing
        
        # For now, just log the successful completion
        logger.info(f"Successfully processed correlation_id: {correlation_id}")
        
        return True
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse market data JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        logger.exception("Full traceback:")
        return False

def main():
    """Main entry point for Lambda-triggered processing"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Lambda-triggered inference service")
    
    try:
        success = process_lambda_triggered_data()
        if success:
            logger.info("Processing completed successfully")
            sys.exit(0)
        else:
            logger.error("Processing failed")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

if __name__ == "__main__":
    main()