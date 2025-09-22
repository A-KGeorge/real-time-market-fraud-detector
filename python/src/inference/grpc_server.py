"""
gRPC server for market surveillance fraud detection
"""

import asyncio
import logging
import time
import signal
import sys
from concurrent import futures
from typing import List, Dict, Optional
import grpc
import numpy as np
from datetime import datetime
import sys
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

# Add the src directory to the path so we can import generated modules
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__))))

# Generated proto imports (these will be generated from the .proto file)
from generated import market_surveillance_pb2 as pb2
from generated import market_surveillance_pb2_grpc as pb2_grpc

# Use absolute imports when running as script
try:
    from .config import Config
    from .model_loader import ModelLoader
    from .data_fetcher import DataFetcher
    from .feature_engineer import FeatureEngineer
except ImportError:
    # Fallback for when running as script
    from inference.config import Config
    from inference.model_loader import ModelLoader
    from inference.data_fetcher import DataFetcher
    from inference.feature_engineer import FeatureEngineer

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MarketSurveillanceService(pb2_grpc.MarketSurveillanceServiceServicer):
    def __init__(self):
        self.model_loader = ModelLoader()
        self.data_fetcher = DataFetcher()
        self.feature_engineer: Optional[FeatureEngineer] = None
        self.service_start_time = datetime.now()
        self.request_count = 0
        self.is_healthy = False
        
        # Initialize the service
        self._initialize_service()
    
    def _initialize_service(self):
        """Initialize the service by loading models and setting up components"""
        try:
            logger.info("Initializing Market Surveillance Service...")
            
            # Load models
            if not self.model_loader.load_models():
                logger.error("Failed to load models")
                return
            
            # Initialize feature engineer with loaded feature columns
            self.feature_engineer = FeatureEngineer(
                self.model_loader.feature_columns
            )
            
            self.is_healthy = True
            logger.info("Market Surveillance Service initialized successfully")
            
        except Exception as e:
            logger.error(f"Service initialization failed: {str(e)}")
            self.is_healthy = False
    
    def DetectFraud(self, request, context):
        """Single fraud detection RPC"""
        try:
            self.request_count += 1
            logger.info(f"Processing fraud detection for symbol: {request.symbol}")
            
            if not self.is_healthy:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("Service is not healthy")
                return pb2.FraudDetectionResult()
            
            # Use provided timestamp or current time
            timestamp = request.timestamp if request.timestamp > 0 else int(time.time())
            
            # Fetch market data
            market_data = self.data_fetcher.fetch_stock_data(request.symbol)
            if market_data is None or market_data.empty:
                logger.warning(f"No data available for {request.symbol}")
                return self._create_error_result(request.symbol, timestamp, "No market data available")
            
            # Engineer features
            features = self.feature_engineer.engineer_features_for_inference(
                market_data, request.symbol
            )
            if features is None:
                logger.warning(f"Feature engineering failed for {request.symbol}")
                return self._create_error_result(request.symbol, timestamp, "Feature engineering failed")
            
            # Extract market data for response
            market_data_proto = features.pop('_market_data')
            
            # Convert features to numpy array and scale
            feature_vector = self.feature_engineer.get_feature_vector(features)
            scaled_features = self.model_loader.scale_features(feature_vector)
            
            # Run inference
            ensemble_scores, anomaly_scores, classifier_probs = self.model_loader.ensemble_predict(scaled_features)
            
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
            alerts = self.feature_engineer.generate_alerts(
                features, ensemble_score, classifier_prob, anomaly_score
            )
            
            # Create response
            result = pb2.FraudDetectionResult(
                symbol=request.symbol,
                timestamp=timestamp,
                ensemble_score=ensemble_score,
                classifier_probability=classifier_prob,
                anomaly_score=anomaly_score,
                is_suspicious=bool(ensemble_score > 0.5),
                risk_level=risk_level,
                market_data=pb2.MarketData(**market_data_proto),
                alerts=alerts
            )
            
            logger.info(f"Fraud detection completed for {request.symbol}: risk={risk_level}, score={ensemble_score:.3f}")
            return result
            
        except Exception as e:
            logger.error(f"Fraud detection failed for {request.symbol}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return pb2.FraudDetectionResult()
    
    def DetectFraudBatch(self, request, context):
        """Batch fraud detection RPC"""
        try:
            logger.info(f"Processing batch fraud detection for {len(request.symbols)} symbols")
            
            if not self.is_healthy:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("Service is not healthy")
                return pb2.BatchFraudDetectionResponse()
            
            # Use provided timestamp or current time
            timestamp = request.timestamp if request.timestamp > 0 else int(time.time())
            
            results = []
            errors = []
            processed_count = 0
            failed_count = 0
            
            # Fetch data for all symbols
            batch_data = self.data_fetcher.fetch_batch_data(list(request.symbols))
            
            for symbol in request.symbols:
                try:
                    latest_data = batch_data.get(symbol)
                    if latest_data is None:
                        errors.append(f"No data available for {symbol}")
                        failed_count += 1
                        continue
                    
                    # Convert Series to DataFrame for feature engineering
                    df = latest_data.to_frame().T
                    df['Date'] = latest_data.get('Date', datetime.now())
                    
                    # Engineer features
                    features = self.feature_engineer.engineer_features_for_inference(df, symbol)
                    if features is None:
                        errors.append(f"Feature engineering failed for {symbol}")
                        failed_count += 1
                        continue
                    
                    # Extract market data
                    market_data_proto = features.pop('_market_data')
                    
                    # Run inference
                    feature_vector = self.feature_engineer.get_feature_vector(features)
                    scaled_features = self.model_loader.scale_features(feature_vector)
                    ensemble_scores, anomaly_scores, classifier_probs = self.model_loader.ensemble_predict(scaled_features)
                    
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
                    alerts = self.feature_engineer.generate_alerts(
                        features, ensemble_score, classifier_prob, anomaly_score
                    )
                    
                    # Create result
                    result = pb2.FraudDetectionResult(
                        symbol=symbol,
                        timestamp=timestamp,
                        ensemble_score=ensemble_score,
                        classifier_probability=classifier_prob,
                        anomaly_score=anomaly_score,
                        is_suspicious=bool(ensemble_score > 0.5),
                        risk_level=risk_level,
                        market_data=pb2.MarketData(**market_data_proto),
                        alerts=alerts
                    )
                    
                    results.append(result)
                    processed_count += 1
                    
                except Exception as e:
                    error_msg = f"Processing failed for {symbol}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                    failed_count += 1
            
            response = pb2.BatchFraudDetectionResponse(
                results=results,
                processed_count=processed_count,
                failed_count=failed_count,
                errors=errors
            )
            
            logger.info(f"Batch processing completed: {processed_count} successful, {failed_count} failed")
            return response
            
        except Exception as e:
            logger.error(f"Batch fraud detection failed: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return pb2.BatchFraudDetectionResponse()
    
    def StreamFraudDetection(self, request, context):
        """Stream fraud detection RPC (server streaming)"""
        try:
            logger.info(f"Starting fraud detection stream for {len(request.symbols)} symbols")
            
            if not self.is_healthy:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("Service is not healthy")
                return
            
            # Validate interval
            interval = max(Config.MIN_STREAM_INTERVAL, 
                         min(request.interval_seconds, Config.MAX_STREAM_INTERVAL))
            if interval != request.interval_seconds:
                logger.info(f"Adjusted streaming interval from {request.interval_seconds}s to {interval}s")
            
            symbols = list(request.symbols) if request.symbols else Config.DEFAULT_SYMBOLS
            
            while context.is_active():
                timestamp = int(time.time())
                
                # Fetch data for all symbols
                batch_data = self.data_fetcher.fetch_batch_data(symbols)
                
                for symbol in symbols:
                    try:
                        if not context.is_active():
                            break
                        
                        latest_data = batch_data.get(symbol)
                        if latest_data is None:
                            continue
                        
                        # Convert to DataFrame
                        df = latest_data.to_frame().T
                        df['Date'] = latest_data.get('Date', datetime.now())
                        
                        # Engineer features
                        features = self.feature_engineer.engineer_features_for_inference(df, symbol)
                        if features is None:
                            continue
                        
                        # Extract market data
                        market_data_proto = features.pop('_market_data')
                        
                        # Run inference
                        feature_vector = self.feature_engineer.get_feature_vector(features)
                        scaled_features = self.model_loader.scale_features(feature_vector)
                        ensemble_scores, anomaly_scores, classifier_probs = self.model_loader.ensemble_predict(scaled_features)
                        
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
                        alerts = self.feature_engineer.generate_alerts(
                            features, ensemble_score, classifier_prob, anomaly_score
                        )
                        
                        # Create and yield result
                        result = pb2.FraudDetectionResult(
                            symbol=symbol,
                            timestamp=timestamp,
                            ensemble_score=ensemble_score,
                            classifier_probability=classifier_prob,
                            anomaly_score=anomaly_score,
                            is_suspicious=bool(ensemble_score > 0.5),
                            risk_level=risk_level,
                            market_data=pb2.MarketData(**market_data_proto),
                            alerts=alerts
                        )
                        
                        yield result
                        
                    except Exception as e:
                        logger.error(f"Stream processing failed for {symbol}: {str(e)}")
                        continue
                
                # Wait for next interval
                time.sleep(interval)
                
        except Exception as e:
            logger.error(f"Stream fraud detection failed: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
    
    def HealthCheck(self, request, context):
        """Health check RPC"""
        try:
            model_status_dict = self.model_loader.get_model_status()
            model_status = pb2.ModelStatus(**model_status_dict)
            
            status = "SERVING" if self.is_healthy else "NOT_SERVING"
            
            response = pb2.HealthCheckResponse(
                status=status,
                timestamp=int(time.time()),
                version=Config.SERVICE_VERSION,
                model_status=model_status
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Health check error: {str(e)}")
            return pb2.HealthCheckResponse()
    
    def GetModelMetrics(self, request, context):
        """Get model metrics RPC"""
        try:
            model_status_dict = self.model_loader.get_model_status()
            model_performance_dict = self.model_loader.get_model_performance()
            
            model_status = pb2.ModelStatus(**model_status_dict)
            model_performance = pb2.ModelPerformance(**model_performance_dict)
            
            response = pb2.ModelMetricsResponse(
                status=model_status,
                performance=model_performance,
                last_updated=int(self.service_start_time.timestamp())
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Get model metrics failed: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Model metrics error: {str(e)}")
            return pb2.ModelMetricsResponse()
    
    def _create_error_result(self, symbol: str, timestamp: int, error_msg: str):
        """Create an error result for failed predictions"""
        return pb2.FraudDetectionResult(
            symbol=symbol,
            timestamp=timestamp,
            ensemble_score=0.0,
            classifier_probability=0.0,
            anomaly_score=0,
            is_suspicious=False,
            risk_level="ERROR",
            market_data=pb2.MarketData(),
            alerts=[f"Error: {error_msg}"]
        )

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP health check handler"""
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            health_status = {
                "status": "healthy",
                "service": "market-surveillance",
                "timestamp": datetime.now().isoformat(),
                "uptime_seconds": int((datetime.now() - service_start_time).total_seconds())
            }
            
            self.wfile.write(json.dumps(health_status).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress default logging to avoid cluttering logs
        pass

# Global variable to track service start time
service_start_time = datetime.now()

def start_health_check_server():
    """Start HTTP health check server in a separate thread"""
    try:
        health_server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
        logger.info("Health check server listening on port 8080")
        health_server.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start health check server: {e}")

def start_sqs_polling(surveillance_service):
    """Start adaptive SQS polling in background thread"""
    logger.info("Starting background SQS polling thread...")
    
    while True:
        try:
            if surveillance_service.data_fetcher.use_sqs and surveillance_service.data_fetcher.sqs_consumer:
                # Poll for messages
                messages = surveillance_service.data_fetcher.sqs_consumer.poll_messages()
                if messages:
                    logger.info(f"Background poll: Received {len(messages)} SQS messages")
                    for message in messages:
                        symbol = message.get('symbol', 'UNKNOWN')
                        price = message.get('price', 'N/A')
                        logger.info(f"  - {symbol}: ${price}")
                else:
                    logger.debug("Background poll: No new SQS messages")
                
                # Use adaptive polling interval for cost optimization
                interval = surveillance_service.data_fetcher.sqs_consumer.get_adaptive_interval()
                time.sleep(interval)
            else:
                # Fallback to default interval
                time.sleep(Config.SQS_POLLING_INTERVAL)
            
        except Exception as e:
            logger.error(f"SQS polling error: {e}")
            time.sleep(Config.SQS_POLLING_INTERVAL)

def serve():
    """Start the gRPC server and health check server"""
    global service_start_time
    service_start_time = datetime.now()
    
    logger.info("Starting Market Surveillance gRPC Server...")
    
    # Start health check server in background thread
    health_thread = threading.Thread(target=start_health_check_server, daemon=True)
    health_thread.start()
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add service
    surveillance_service = MarketSurveillanceService()
    pb2_grpc.add_MarketSurveillanceServiceServicer_to_server(surveillance_service, server)
    
    # Choose data source: SQS polling only
    if Config.USE_SQS_DATA_SOURCE:
        # Use SQS polling for data ingestion
        sqs_thread = threading.Thread(target=start_sqs_polling, args=(surveillance_service,), daemon=True)
        sqs_thread.start()
        logger.info("Background SQS polling thread started")
        logger.info("Using SQS polling for data ingestion")
    else:
        logger.info("No data source enabled, using yfinance fallback only")
    
    # Configure server
    listen_addr = f"{Config.GRPC_HOST}:{Config.GRPC_PORT}"
    server.add_insecure_port(listen_addr)
    
    # Start server
    server.start()
    logger.info(f"Market Surveillance Service listening on {listen_addr}")
    logger.info("Health check endpoint available at http://0.0.0.0:8080/health")
    
    # Handle shutdown gracefully
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal")
        server.stop(grace=30)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
        server.stop(grace=30)

if __name__ == '__main__':
    serve()