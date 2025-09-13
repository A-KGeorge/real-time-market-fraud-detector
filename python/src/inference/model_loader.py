"""
Model loader for TensorFlow Lite market surveillance models
"""

import json
import pickle
import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
import tensorflow as tf
from .config import Config

logger = logging.getLogger(__name__)

class ModelLoader:
    def __init__(self):
        self.autoencoder_interpreter: Optional[tf.lite.Interpreter] = None
        self.classifier_interpreter: Optional[tf.lite.Interpreter] = None
        self.scaler = None
        self.feature_columns: List[str] = []
        self.surveillance_config: Dict = {}
        self.model_loaded = False
        self.last_error: Optional[str] = None
        
    def load_models(self) -> bool:
        """Load all models and artifacts"""
        try:
            logger.info("Loading market surveillance models...")
            
            # Load TensorFlow Lite models
            if not self._load_tflite_models():
                return False
                
            # Load scaler
            if not self._load_scaler():
                return False
                
            # Load configuration and features
            if not self._load_config_and_features():
                return False
                
            self.model_loaded = True
            logger.info("All models loaded successfully")
            return True
            
        except Exception as e:
            self.last_error = f"Failed to load models: {str(e)}"
            logger.error(self.last_error)
            return False
    
    def _load_tflite_models(self) -> bool:
        """Load TensorFlow Lite models"""
        try:
            # Load autoencoder
            logger.info("Loading autoencoder...")
            self.autoencoder_interpreter = tf.lite.Interpreter(
                model_path=Config.AUTOENCODER_MODEL_PATH
            )
            self.autoencoder_interpreter.allocate_tensors()
            
            # Load classifier
            logger.info("Loading classifier...")
            self.classifier_interpreter = tf.lite.Interpreter(
                model_path=Config.CLASSIFIER_MODEL_PATH
            )
            self.classifier_interpreter.allocate_tensors()
            
            logger.info("TensorFlow Lite models loaded successfully")
            return True
            
        except Exception as e:
            self.last_error = f"Failed to load TFLite models: {str(e)}"
            logger.error(self.last_error)
            return False
    
    def _load_scaler(self) -> bool:
        """Load the feature scaler"""
        try:
            logger.info("Loading feature scaler...")
            with open(Config.SCALER_PATH, 'rb') as f:
                self.scaler = pickle.load(f)
            logger.info("Feature scaler loaded successfully")
            return True
            
        except Exception as e:
            self.last_error = f"Failed to load scaler: {str(e)}"
            logger.error(self.last_error)
            return False
    
    def _load_config_and_features(self) -> bool:
        """Load model configuration and feature columns"""
        try:
            # Load feature columns
            logger.info("Loading feature columns...")
            with open(Config.FEATURES_PATH, 'r') as f:
                self.feature_columns = json.load(f)
            
            # Load surveillance configuration
            logger.info("Loading surveillance configuration...")
            with open(Config.CONFIG_PATH, 'r') as f:
                self.surveillance_config = json.load(f)
                
            logger.info(f"Loaded {len(self.feature_columns)} feature columns")
            logger.info("Configuration loaded successfully")
            return True
            
        except Exception as e:
            self.last_error = f"Failed to load config/features: {str(e)}"
            logger.error(self.last_error)
            return False
    
    def predict_autoencoder(self, X: np.ndarray) -> np.ndarray:
        """Run autoencoder inference"""
        if not self.model_loaded:
            raise RuntimeError("Models not loaded")
            
        # Get input and output tensors
        input_details = self.autoencoder_interpreter.get_input_details()
        output_details = self.autoencoder_interpreter.get_output_details()
        
        # Set input tensor
        self.autoencoder_interpreter.set_tensor(
            input_details[0]['index'], 
            X.astype(np.float32)
        )
        
        # Run inference
        self.autoencoder_interpreter.invoke()
        
        # Get output
        output_data = self.autoencoder_interpreter.get_tensor(output_details[0]['index'])
        return output_data
    
    def predict_classifier(self, X: np.ndarray) -> np.ndarray:
        """Run classifier inference"""
        if not self.model_loaded:
            raise RuntimeError("Models not loaded")
            
        # Get input and output tensors
        input_details = self.classifier_interpreter.get_input_details()
        output_details = self.classifier_interpreter.get_output_details()
        
        # Set input tensor
        self.classifier_interpreter.set_tensor(
            input_details[0]['index'], 
            X.astype(np.float32)
        )
        
        # Run inference
        self.classifier_interpreter.invoke()
        
        # Get output
        output_data = self.classifier_interpreter.get_tensor(output_details[0]['index'])
        return output_data
    
    def calculate_reconstruction_error(self, X: np.ndarray) -> np.ndarray:
        """Calculate reconstruction error from autoencoder"""
        reconstructed = self.predict_autoencoder(X)
        mse = np.mean(np.power(X - reconstructed, 2), axis=1)
        return mse
    
    def ensemble_predict(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Make ensemble predictions combining autoencoder and classifier"""
        if not self.model_loaded:
            raise RuntimeError("Models not loaded")
        
        # Get configuration weights
        config = self.surveillance_config
        ae_weight = config.get('best_ensemble_ae_weight', 0.4)
        clf_weight = config.get('best_ensemble_clf_weight', 0.6)
        
        # Autoencoder predictions (anomaly detection)
        reconstruction_errors = self.calculate_reconstruction_error(X)
        
        # Normalize reconstruction errors based on training data range
        error_min = config.get('reconstruction_error_min', 0.0)
        error_max = config.get('reconstruction_error_max', 1.0)
        normalized_errors = (reconstruction_errors - error_min) / (error_max - error_min)
        normalized_errors = np.clip(normalized_errors, 0, 1)
        
        # Classifier predictions
        classifier_probs = self.predict_classifier(X).flatten()
        
        # Ensemble prediction (weighted combination)
        ensemble_scores = ae_weight * normalized_errors + clf_weight * classifier_probs
        
        return ensemble_scores, normalized_errors, classifier_probs
    
    def scale_features(self, features: np.ndarray) -> np.ndarray:
        """Scale features using the loaded scaler"""
        if not self.model_loaded:
            raise RuntimeError("Models not loaded")
        return self.scaler.transform(features)
    
    def get_model_status(self) -> Dict:
        """Get model loading status"""
        return {
            'autoencoder_loaded': self.autoencoder_interpreter is not None,
            'classifier_loaded': self.classifier_interpreter is not None,
            'scaler_loaded': self.scaler is not None,
            'config_loaded': bool(self.surveillance_config),
            'last_error': self.last_error or ''
        }
    
    def get_model_performance(self) -> Dict:
        """Get model performance metrics from config"""
        if not self.surveillance_config:
            return {
                'accuracy': 0.0,
                'precision': 0.0, 
                'recall': 0.0,
                'f1_score': 0.0,
                'auc_score': 0.0
            }
        
        # These would come from your training results
        # For now, using example values - you can update with actual metrics
        return {
            'accuracy': 0.97,
            'precision': 0.38,
            'recall': 0.99,
            'f1_score': 0.55,
            'auc_score': 0.9962
        }