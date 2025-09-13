"""
Market Surveillance Model Training Script

This script trains a market surveillance model that combines:
1. Autoencoder for anomaly detection (learns normal trading patterns)
2. Neural network classifier for suspicious activity detection

The model is designed to detect suspicious trading patterns and potential market manipulation.
"""

import tensorflow as tf
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, roc_auc_score
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve, precision_score, recall_score
import json
import pickle
import os
import ta  # Technical Analysis library
import warnings
import glob

warnings.filterwarnings('ignore')

# Set random seeds for reproducibility
tf.random.set_seed(42)
np.random.seed(42)

print("All libraries imported successfully!")
print(f"TensorFlow version: {tf.__version__}")
print(f"GPU Available: {tf.config.list_physical_devices('GPU')}")

# Configuration and Path Setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data')
MODELS_DIR = os.path.join(BASE_DIR, 'models')

# Create directories if they don't exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

print(f"Data directory: {DATA_DIR}")
print(f"Models directory: {MODELS_DIR}")


class MarketSurveillanceModel:
    def __init__(self):
        self.scaler = StandardScaler()
        self.autoencoder = None
        self.classifier = None
        self.feature_columns = None
        
    def engineer_features(self, df):
        """Engineer features for market surveillance"""
        print("Engineering market surveillance features...")
        
        # Sort by symbol and date for proper time series calculations
        df = df.sort_values(['Symbol', 'Date']).copy()
        
        # Basic price features
        df['Price_Change'] = df.groupby('Symbol')['Close'].pct_change()
        df['Price_Range'] = (df['High'] - df['Low']) / df['Close']
        df['Gap'] = (df['Open'] - df.groupby('Symbol')['Close'].shift(1)) / df.groupby('Symbol')['Close'].shift(1)
        
        # Volume features
        df['Volume_Change'] = df.groupby('Symbol')['Volume'].pct_change()
        df['Volume_Price_Ratio'] = df['Volume'] / df['Close']
        df['Volume_MA'] = df.groupby('Symbol')['Volume'].rolling(window=20, min_periods=1).mean().reset_index(0, drop=True)
        df['Volume_Ratio'] = df['Volume'] / df['Volume_MA']
        
        # Technical indicators using ta library
        for symbol in df['Symbol'].unique():
            mask = df['Symbol'] == symbol
            symbol_data = df[mask].copy()
            
            # RSI (Relative Strength Index)
            df.loc[mask, 'RSI'] = ta.momentum.RSIIndicator(symbol_data['Close']).rsi()
            
            # MACD
            macd_indicator = ta.trend.MACD(symbol_data['Close'])
            df.loc[mask, 'MACD'] = macd_indicator.macd()
            df.loc[mask, 'MACD_Signal'] = macd_indicator.macd_signal()
            df.loc[mask, 'MACD_Histogram'] = macd_indicator.macd_diff()
            
            # Bollinger Bands
            bb_indicator = ta.volatility.BollingerBands(symbol_data['Close'])
            df.loc[mask, 'BB_Upper'] = bb_indicator.bollinger_hband()
            df.loc[mask, 'BB_Lower'] = bb_indicator.bollinger_lband()
            df.loc[mask, 'BB_Middle'] = bb_indicator.bollinger_mavg()
            df.loc[mask, 'BB_Width'] = (df.loc[mask, 'BB_Upper'] - df.loc[mask, 'BB_Lower']) / df.loc[mask, 'BB_Middle']
            df.loc[mask, 'BB_Position'] = (df.loc[mask, 'Close'] - df.loc[mask, 'BB_Lower']) / (df.loc[mask, 'BB_Upper'] - df.loc[mask, 'BB_Lower'])
            
            # Moving averages
            df.loc[mask, 'SMA_10'] = symbol_data['Close'].rolling(window=10, min_periods=1).mean()
            df.loc[mask, 'SMA_20'] = symbol_data['Close'].rolling(window=20, min_periods=1).mean()
            df.loc[mask, 'EMA_12'] = symbol_data['Close'].ewm(span=12).mean()
            df.loc[mask, 'EMA_26'] = symbol_data['Close'].ewm(span=26).mean()
        
        # Price position relative to moving averages
        df['Price_vs_SMA10'] = df['Close'] / df['SMA_10'] - 1
        df['Price_vs_SMA20'] = df['Close'] / df['SMA_20'] - 1
        
        # Volatility measures
        df['Volatility_10d'] = df.groupby('Symbol')['Price_Change'].rolling(window=10, min_periods=1).std().reset_index(0, drop=True)
        df['Volatility_20d'] = df.groupby('Symbol')['Price_Change'].rolling(window=20, min_periods=1).std().reset_index(0, drop=True)
        
        # Time-based features
        df['Date'] = pd.to_datetime(df['Date'])
        df['Hour'] = df['Date'].dt.hour
        df['DayOfWeek'] = df['Date'].dt.dayofweek
        df['IsMonday'] = (df['DayOfWeek'] == 0).astype(int)
        df['IsFriday'] = (df['DayOfWeek'] == 4).astype(int)
        
        # Suspicious pattern indicators (these create synthetic labels for training)
        df['Unusual_Volume'] = ((df['Volume_Ratio'] > 3) | (df['Volume_Ratio'] < 0.1)).astype(int)
        df['Extreme_Price_Move'] = (abs(df['Price_Change']) > 0.05).astype(int)  # >5% move
        df['Gap_Trading'] = (abs(df['Gap']) > 0.02).astype(int)  # >2% gap
        df['After_Hours_Activity'] = ((df['Hour'] < 9) | (df['Hour'] > 16)).astype(int)
        
        # Create composite suspicious activity label
        df['Suspicious_Activity'] = (
            (df['Unusual_Volume'] == 1) & 
            (df['Extreme_Price_Move'] == 1)
        ).astype(int)
        
        # Additional suspicious patterns
        df['Potential_Manipulation'] = (
            (df['Volume_Ratio'] > 5) & 
            (abs(df['Price_Change']) > 0.03) & 
            (df['BB_Position'] > 0.95)  # Price near upper Bollinger Band
        ).astype(int)
        
        # Final suspicious label (combine multiple indicators)
        df['Target'] = ((df['Suspicious_Activity'] == 1) | (df['Potential_Manipulation'] == 1)).astype(int)

        # Replace inf/-inf with NaN
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Drop rows where key features are NaN
        df.dropna(subset=[
            'Price_Change', 'Volume_Ratio', 'RSI', 'MACD', 'BB_Position',
            'SMA_10', 'SMA_20'
        ], inplace=True)

        # Final reset of index after cleaning
        df.reset_index(drop=True, inplace=True)

        print(f"Final shape after cleaning: {df.shape}")
        
        return df

    def load_and_preprocess_data(self, file_path):
        """Load and preprocess market surveillance dataset"""
        print("Loading and preprocessing market data...")
        
        # Load data
        df = pd.read_csv(file_path)
        print(f"Dataset shape: {df.shape}")
        
        # Engineer features
        df = self.engineer_features(df)
        
        # Select features for training (exclude original OHLCV, dates, symbols, and intermediate calculations)
        feature_columns = [
            'Price_Change', 'Price_Range', 'Gap', 'Volume_Change', 'Volume_Price_Ratio',
            'Volume_Ratio', 'RSI', 'MACD', 'MACD_Signal', 'MACD_Histogram',
            'BB_Width', 'BB_Position', 'Price_vs_SMA10', 'Price_vs_SMA20',
            'Volatility_10d', 'Volatility_20d', 'Hour', 'DayOfWeek',
            'IsMonday', 'IsFriday'
        ]
        
        # Remove rows with NaN values
        df_clean = df[feature_columns + ['Target']].dropna()
        
        X = df_clean[feature_columns]
        y = df_clean['Target']
        
        print(f"Clean dataset shape: {X.shape}")
        print(f"Suspicious activities: {y.sum()} ({y.mean()*100:.2f}%)")
        
        self.feature_columns = feature_columns
        
        # Split the data (70% train, 15% validation, 15% test)
        X_temp, X_test, y_temp, y_test = train_test_split(
            X, y, test_size=0.15, random_state=42, stratify=y
        )
        
        X_train, X_val, y_train, y_val = train_test_split(
            X_temp, y_temp, test_size=0.176, random_state=42, stratify=y_temp
        )
        
        print(f"Train set: {X_train.shape[0]} samples")
        print(f"Validation set: {X_val.shape[0]} samples") 
        print(f"Test set: {X_test.shape[0]} samples")
        
        # Scale the features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_val_scaled = self.scaler.transform(X_val)
        X_test_scaled = self.scaler.transform(X_test)
        
        return (X_train_scaled, X_val_scaled, X_test_scaled, 
                y_train, y_val, y_test)

    def build_autoencoder(self, input_dim, encoding_dim=10):
        """Build autoencoder for anomaly detection"""
        print("Building autoencoder for market anomaly detection...")
        
        # Encoder
        input_layer = tf.keras.Input(shape=(input_dim,))
        encoded = tf.keras.layers.Dense(encoding_dim * 2, activation='relu')(input_layer)
        encoded = tf.keras.layers.Dropout(0.2)(encoded)
        encoded = tf.keras.layers.Dense(encoding_dim, activation='relu')(encoded)
        
        # Decoder
        decoded = tf.keras.layers.Dense(encoding_dim * 2, activation='relu')(encoded)
        decoded = tf.keras.layers.Dropout(0.2)(decoded)
        decoded = tf.keras.layers.Dense(input_dim, activation='linear')(decoded)
        
        autoencoder = tf.keras.Model(input_layer, decoded)
        autoencoder.compile(optimizer='adam', loss='mse', metrics=['mae'])
        
        return autoencoder

    def build_classifier(self, input_dim):
        """Build neural network classifier for suspicious activity detection"""
        print("Building classifier for suspicious activity detection...")
        
        model = tf.keras.Sequential([
            tf.keras.layers.Input(shape=(input_dim,)),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.3),
            tf.keras.layers.Dense(64, activation='relu'),
            tf.keras.layers.Dropout(0.3),
            tf.keras.layers.Dense(32, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(16, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(1, activation='sigmoid')
        ])
        
        model.compile(
            optimizer='adam',
            loss='binary_crossentropy',
            metrics=['accuracy', 'precision', 'recall']
        )
        
        return model

    def train_autoencoder(self, X_train, X_val, epochs=100):
        """Train autoencoder on normal trading patterns only"""
        print("Training autoencoder on normal trading patterns...")
        
        # Use only normal trading patterns for training autoencoder
        if hasattr(self, 'y_train'):
            normal_indices = np.where(self.y_train == 0)[0]
            normal_data = X_train[normal_indices]
            print(f"Training autoencoder on {len(normal_data)} normal patterns")
        else:
            normal_data = X_train
        
        self.autoencoder = self.build_autoencoder(X_train.shape[1])
        
        early_stopping = tf.keras.callbacks.EarlyStopping(
            monitor='val_loss', patience=15, restore_best_weights=True
        )
        
        reduce_lr = tf.keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss', factor=0.2, patience=7, min_lr=0.0001
        )
        
        history_ae = self.autoencoder.fit(
            normal_data, normal_data,
            epochs=epochs,
            batch_size=64,
            validation_data=(X_val, X_val),
            callbacks=[early_stopping, reduce_lr],
            verbose=1
        )
        
        return history_ae

    def train_classifier(self, X_train, y_train, X_val, y_val, epochs=100):
        """Train neural network classifier for suspicious activity detection"""
        print("Training classifier for suspicious activity detection...")
        
        self.classifier = self.build_classifier(X_train.shape[1])
        
        # Handle class imbalance with class weights
        class_weight = {
            0: 1.0,
            1: len(y_train[y_train == 0]) / len(y_train[y_train == 1])
        }
        
        print(f"Class weights: Normal={class_weight[0]}, Suspicious={class_weight[1]:.2f}")
        
        early_stopping = tf.keras.callbacks.EarlyStopping(
            monitor='val_loss', patience=20, restore_best_weights=True
        )
        
        reduce_lr = tf.keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss', factor=0.2, patience=10, min_lr=0.00001
        )
        
        history_clf = self.classifier.fit(
            X_train, y_train,
            epochs=epochs,
            batch_size=64,
            validation_data=(X_val, y_val),
            class_weight=class_weight,
            callbacks=[early_stopping, reduce_lr],
            verbose=1
        )
        
        return history_clf

    def calculate_reconstruction_error(self, X):
        """Calculate reconstruction error for autoencoder"""
        reconstructed = self.autoencoder.predict(X, verbose=0)
        mse = np.mean(np.power(X - reconstructed, 2), axis=1)
        return mse

    def set_anomaly_threshold(self, X_val, y_val, percentile=90):
        """Set threshold for anomaly detection based on validation set"""
        reconstruction_errors = self.calculate_reconstruction_error(X_val)
        normal_errors = reconstruction_errors[y_val == 0]
        threshold = np.percentile(normal_errors, percentile)
        self.anomaly_threshold = threshold
        print(f"Anomaly threshold set to: {threshold:.6f} (based on {percentile}th percentile)")
        return threshold

    def ensemble_predict(self, X, autoencoder_weight=0.4, classifier_weight=0.6):
        """Make ensemble predictions combining autoencoder and classifier"""
        # Autoencoder predictions (anomaly detection)
        reconstruction_errors = self.calculate_reconstruction_error(X)
        anomaly_scores = (reconstruction_errors > self.anomaly_threshold).astype(int)
        
        # Classifier predictions
        classifier_probs = self.classifier.predict(X, verbose=0).flatten()
        
        # Ensemble prediction (weighted combination)
        ensemble_scores = (autoencoder_weight * anomaly_scores + 
                            classifier_weight * classifier_probs)
        
        return ensemble_scores, anomaly_scores, classifier_probs

    def evaluate_model(self, X_test, y_test):
        """Evaluate the ensemble model"""
        print("\n" + "="*50)
        print("EVALUATING MARKET SURVEILLANCE MODEL")
        print("="*50)
        
        ensemble_scores, anomaly_scores, classifier_probs = self.ensemble_predict(X_test)
        
        # Convert to binary predictions
        ensemble_preds = (ensemble_scores > 0.5).astype(int)
        classifier_preds = (classifier_probs > 0.5).astype(int)
        
        # Print evaluation metrics
        print("\n=== ENSEMBLE MODEL RESULTS ===")
        print("Detects: Suspicious trading patterns and market manipulation")
        print(classification_report(y_test, ensemble_preds))
        print(f"AUC Score: {roc_auc_score(y_test, ensemble_scores):.4f}")
        
        print("\n=== CLASSIFIER ONLY RESULTS ===")
        print(classification_report(y_test, classifier_preds))
        print(f"AUC Score: {roc_auc_score(y_test, classifier_probs):.4f}")
        
        print("\n=== AUTOENCODER ONLY RESULTS ===")
        print("(Pure anomaly detection based on reconstruction error)")
        print(classification_report(y_test, anomaly_scores))
        
        return ensemble_scores, classifier_probs, anomaly_scores

    def find_optimal_threshold(self, y_true, y_prob, metric='f1'):
        """Find optimal threshold based on specified metric"""
        precision, recall, thresholds = precision_recall_curve(y_true, y_prob)
        
        if metric == 'f1':
            scores = 2 * (precision * recall) / (precision + recall + 1e-10)
        elif metric == 'precision':
            scores = precision
        elif metric == 'recall':
            scores = recall
        else:
            raise ValueError("Metric must be 'f1', 'precision', or 'recall'")
        
        best_index = np.argmax(scores)
        best_threshold = thresholds[best_index] if best_index < len(thresholds) else thresholds[-1]
        
        return best_threshold, precision[best_index], recall[best_index], scores[best_index]

    def optimize_thresholds(self, X_val, y_val, X_test):
        """Optimize thresholds for better precision-recall balance"""
        print("\nOptimizing thresholds for market surveillance...")
        
        # Get predictions
        val_reconstruction_errors = self.calculate_reconstruction_error(X_val)
        val_classifier_probs = self.classifier.predict(X_val, verbose=0).flatten()
        
        test_reconstruction_errors = self.calculate_reconstruction_error(X_test)

        clf_threshold, _, _, clf_f1 = self.find_optimal_threshold(
            y_val, val_classifier_probs, 'f1'
        )
        
        ae_threshold, _, _, ae_f1 = self.find_optimal_threshold(
            y_val, val_reconstruction_errors, 'f1'
        )
        
        print(f"Optimized Classifier - Threshold: {clf_threshold:.4f}, F1: {clf_f1:.4f}")
        print(f"Optimized Autoencoder - Threshold: {ae_threshold:.4f}, F1: {ae_f1:.4f}")
        
        # Test ensemble combinations
        val_errors_norm = (val_reconstruction_errors - val_reconstruction_errors.min()) / \
                        (val_reconstruction_errors.max() - val_reconstruction_errors.min())
        
        best_config = None
        best_f1 = 0
        
        # Test different ensemble weights
        for ae_weight, clf_weight in [(0.3, 0.7), (0.4, 0.6), (0.5, 0.5), (0.6, 0.4)]:
            val_hybrid_scores = ae_weight * val_errors_norm + clf_weight * val_classifier_probs
            hybrid_threshold, _, _, hybrid_f1 = self.find_optimal_threshold(y_val, val_hybrid_scores, 'f1')
            
            if hybrid_f1 > best_f1:
                best_f1 = hybrid_f1
                best_config = {
                    'ae_weight': ae_weight,
                    'clf_weight': clf_weight, 
                    'threshold': hybrid_threshold,
                    'f1': hybrid_f1
                }
        
        # Store results
        self.optimal_config = {
            'classifier_f1_threshold': clf_threshold,
            'autoencoder_f1_threshold': ae_threshold,
            'best_ensemble_ae_weight': best_config['ae_weight'],
            'best_ensemble_clf_weight': best_config['clf_weight'],
            'best_ensemble_threshold': best_config['threshold'],
            'reconstruction_error_min': float(test_reconstruction_errors.min()),
            'reconstruction_error_max': float(test_reconstruction_errors.max()),
            'feature_columns': self.feature_columns
        }
        
        print(f"Best ensemble: AE={best_config['ae_weight']}, CLF={best_config['clf_weight']}, F1={best_f1:.4f}")
        
        return self.optimal_config

    def predict_real_time(self, features_dict):
        """Make real-time predictions for market surveillance (single sample)"""
        # Convert dict to DataFrame with correct feature order
        df_input = pd.DataFrame([features_dict])
        
        # Ensure all required features are present
        missing_features = set(self.feature_columns) - set(df_input.columns)
        if missing_features:
            raise ValueError(f"Missing features: {missing_features}")
        
        # Scale features
        X_scaled = self.scaler.transform(df_input[self.feature_columns])
        
        # Get predictions
        ensemble_score, anomaly_score, classifier_prob = self.ensemble_predict(X_scaled)
        
        return {
            'ensemble_score': float(ensemble_score[0]),
            'anomaly_score': int(anomaly_score[0]),
            'classifier_probability': float(classifier_prob[0]),
            'is_suspicious': bool(ensemble_score[0] > 0.5),
            'risk_level': 'HIGH' if ensemble_score[0] > 0.7 else 'MEDIUM' if ensemble_score[0] > 0.3 else 'LOW'
        }

    def save_models(self, autoencoder_path=None, classifier_path=None):
        """Save trained models, config, and scaler"""
        if autoencoder_path is None:
            autoencoder_path = os.path.join(MODELS_DIR, 'market_autoencoder.h5')
        if classifier_path is None:
            classifier_path = os.path.join(MODELS_DIR, 'market_classifier.h5')
        
        # Create models directory if it doesn't exist
        os.makedirs(MODELS_DIR, exist_ok=True)
        
        # Save models
        self.autoencoder.save(autoencoder_path)
        self.classifier.save(classifier_path)
        print(f"Models saved: {autoencoder_path}, {classifier_path}")
        
        # Save scaler
        scaler_path = os.path.join(MODELS_DIR, 'market_scaler.pkl')
        with open(scaler_path, 'wb') as f:
            pickle.dump(self.scaler, f)
        print(f"Scaler saved: {scaler_path}")

        # Save feature columns
        feature_path = os.path.join(MODELS_DIR, 'market_features.json')
        with open(feature_path, 'w') as f:
            json.dump(self.feature_columns, f, indent=2)
        print(f"Feature order saved: {feature_path}")
        
        # Save config if optimization was run
        if hasattr(self, 'optimal_config'):
            config_path = os.path.join(MODELS_DIR, 'market_surveillance_config.json')
            
            # Convert NumPy types to Python native types
            serializable_config = {}
            for key, value in self.optimal_config.items():
                if isinstance(value, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, 
                                    np.int64, np.uint8, np.uint16, np.uint32, np.uint64)):
                    serializable_config[key] = int(value)
                elif isinstance(value, (np.float64, np.float16, np.float32)):
                    serializable_config[key] = float(value)
                else:
                    serializable_config[key] = value
            
            with open(config_path, 'w') as f:
                json.dump(serializable_config, f, indent=2)
            print(f"Configuration saved: {config_path}")

    def convert_to_tflite(self, model, model_name):
        """Convert model to TensorFlow Lite for real-time deployment"""
        converter = tf.lite.TFLiteConverter.from_keras_model(model)
        converter.optimizations = [tf.lite.Optimize.DEFAULT]
        tflite_model = converter.convert()
        
        tflite_path = os.path.join(MODELS_DIR, f"market_{model_name}.tflite")
        with open(tflite_path, 'wb') as f:
            f.write(tflite_model)
        
        print(f"TensorFlow Lite model saved: {tflite_path}")
        return tflite_path

    def plot_training_history(self, history_ae, history_clf):
        """Plot training history"""
        _, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # Autoencoder loss
        axes[0, 0].plot(history_ae.history['loss'], label='Training Loss', color='blue')
        axes[0, 0].plot(history_ae.history['val_loss'], label='Validation Loss', color='red')
        axes[0, 0].set_title('Autoencoder Loss (Anomaly Detection)')
        axes[0, 0].set_xlabel('Epoch')
        axes[0, 0].set_ylabel('MSE Loss')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        # Classifier loss
        axes[0, 1].plot(history_clf.history['loss'], label='Training Loss', color='blue')
        axes[0, 1].plot(history_clf.history['val_loss'], label='Validation Loss', color='red')
        axes[0, 1].set_title('Classifier Loss (Suspicious Activity Detection)')
        axes[0, 1].set_xlabel('Epoch')
        axes[0, 1].set_ylabel('Binary Crossentropy')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
        
        # Classifier accuracy
        axes[1, 0].plot(history_clf.history['accuracy'], label='Training Accuracy', color='blue')
        axes[1, 0].plot(history_clf.history['val_accuracy'], label='Validation Accuracy', color='red')
        axes[1, 0].set_title('Classifier Accuracy')
        axes[1, 0].set_xlabel('Epoch')
        axes[1, 0].set_ylabel('Accuracy')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)
        
        # Classifier precision & recall
        axes[1, 1].plot(history_clf.history['precision'], label='Training Precision', color='green')
        axes[1, 1].plot(history_clf.history['val_precision'], label='Validation Precision', color='orange')
        axes[1, 1].plot(history_clf.history['recall'], label='Training Recall', color='purple')
        axes[1, 1].plot(history_clf.history['val_recall'], label='Validation Recall', color='brown')
        axes[1, 1].set_title('Classifier Precision & Recall')
        axes[1, 1].set_xlabel('Epoch')
        axes[1, 1].set_ylabel('Score')
        axes[1, 1].legend()
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(MODELS_DIR, 'training_history.png'), dpi=300, bbox_inches='tight')
        plt.show()


def load_stock_data(stocks_folder, limit=None):
    """
    Load multiple stock CSV files into a single dataframe.
    
    Parameters:
        stocks_folder (str): Path to the folder containing stock CSV files.
        limit (int, optional): Number of files to load (for testing).
        
    Returns:
        pd.DataFrame: Combined dataframe of all stocks.
    """
    print(f"Loading stock data from: {stocks_folder}")
    all_files = glob.glob(os.path.join(stocks_folder, "*.csv"))
    
    if not all_files:
        raise FileNotFoundError(f"No CSV files found in {stocks_folder}")
    
    if limit:
        all_files = all_files[:limit]
        print(f"Limiting to first {limit} files for testing...")

    dataframes = []

    for file in all_files:
        symbol = os.path.splitext(os.path.basename(file))[0]  # e.g., AAPL.csv -> AAPL
        df = pd.read_csv(file)
        
        # Add symbol column
        df["Symbol"] = symbol
        
        # Ensure Date is datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Drop rows where date failed to parse
        df = df.dropna(subset=['Date'])

        # Drop 'Adj Close' since it's not needed
        if 'Adj Close' in df.columns:
            df = df.drop(columns=['Adj Close'])
        
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    
    print(f"Combined dataframe shape: {combined_df.shape}")
    print(f"Symbols loaded: {combined_df['Symbol'].nunique()}")
    return combined_df


def tune_classifier_threshold(model, X_val, y_val, target_precision=0.95):
    """Tune classifier threshold for higher precision"""
    print("\n" + "="*40)
    print("THRESHOLD TUNING FOR HIGHER PRECISION")
    print("="*40)

    # Get classifier prediction probabilities on validation data
    val_probs = model.classifier.predict(X_val, verbose=0).flatten()

    # Compute precision-recall curve
    precision, recall, thresholds = precision_recall_curve(y_val, val_probs)

    # Find the threshold where precision crosses a target value
    best_threshold = None

    for p, r, t in zip(precision, recall, thresholds):
        if p >= target_precision:
            best_threshold = t
            break

    # Fallback if no threshold meets target precision
    if best_threshold is None:
        best_threshold = 0.5  # default

    print(f"Selected threshold for precision ≥ {target_precision}: {best_threshold:.4f}")

    # Store threshold inside the model for later use
    model.optimal_config = getattr(model, 'optimal_config', {})
    model.optimal_config['classifier_precision_threshold'] = float(best_threshold)

    # Evaluate precision and recall at this threshold
    val_preds_tuned = (val_probs > best_threshold).astype(int)
    val_precision = precision_score(y_val, val_preds_tuned)
    val_recall = recall_score(y_val, val_preds_tuned)

    print(f"Validation Precision at tuned threshold: {val_precision:.4f}")
    print(f"Validation Recall at tuned threshold: {val_recall:.4f}")
    print("="*40)

    return best_threshold


def plot_precision_recall_curve(y_val, val_probs, best_threshold):
    """Plot precision-recall curve with threshold"""
    precision, recall, thresholds = precision_recall_curve(y_val, val_probs)

    plt.figure(figsize=(8, 6))
    plt.plot(thresholds, precision[:-1], label='Precision', color='blue')
    plt.plot(thresholds, recall[:-1], label='Recall', color='green')
    plt.axvline(x=best_threshold, color='red', linestyle='--', label=f'Tuned Threshold ({best_threshold:.2f})')
    plt.xlabel('Threshold')
    plt.ylabel('Score')
    plt.title('Precision & Recall vs Threshold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()


def main():
    """Main training pipeline"""
    print("="*60)
    print("MARKET SURVEILLANCE MODEL TRAINING")
    print("="*60)
    print("Purpose: Detect suspicious trading patterns and market manipulation")
    print("Features: Technical indicators, volume patterns, price anomalies")
    print("="*60)

    # Initialize model
    surveillance_model = MarketSurveillanceModel()

    # Path to the Kaggle stock folder
    STOCKS_FOLDER = os.path.join(DATA_DIR, 'stocks')

    # Load data
    # Start with a small limit for testing performance, e.g., 50 files
    df = load_stock_data(STOCKS_FOLDER, limit=50)

    # Save combined dataframe as CSV (optional for caching)
    combined_file = os.path.join(DATA_DIR, 'stock_market_data.csv')
    df.to_csv(combined_file, index=False)
    print(f"Combined dataset saved to: {combined_file}")

    # Preprocess for model
    X_train, X_val, X_test, y_train, y_val, y_test = surveillance_model.load_and_preprocess_data(combined_file)

    # Store y_train for autoencoder training
    surveillance_model.y_train = y_train

    print("Data loaded and preprocessed successfully!")

    # Train autoencoder
    print("\n" + "="*40)
    print("TRAINING AUTOENCODER (Anomaly Detection)")
    print("="*40)

    history_ae = surveillance_model.train_autoencoder(X_train, X_val, epochs=50)
    surveillance_model.set_anomaly_threshold(X_val, y_val, percentile=85)

    print("Autoencoder training completed!")

    # Train classifier
    print("\n" + "="*40)
    print("TRAINING CLASSIFIER (Suspicious Activity Detection)")
    print("="*40)

    history_clf = surveillance_model.train_classifier(X_train, y_train, X_val, y_val, epochs=100)

    print("Classifier training completed!")

    # Tune threshold for higher precision
    best_threshold = tune_classifier_threshold(surveillance_model, X_val, y_val, target_precision=0.95)

    # Evaluate models
    print("\n" + "="*40)
    print("MODEL EVALUATION")
    print("="*40)

    surveillance_model.evaluate_model(X_test, y_test)
    surveillance_model.optimize_thresholds(X_val, y_val, X_test)

    print("Model evaluation completed!")

    # Plot training history
    print("\n" + "="*40)
    print("TRAINING HISTORY VISUALIZATION")
    print("="*40)

    surveillance_model.plot_training_history(history_ae, history_clf)

    # Plot precision-recall curve
    val_probs = surveillance_model.classifier.predict(X_val, verbose=0).flatten()
    plot_precision_recall_curve(y_val, val_probs, best_threshold)

    # Save models and export
    print("\n" + "="*40)
    print("SAVING MODELS AND EXPORTING")
    print("="*40)

    surveillance_model.save_models()
    surveillance_model.convert_to_tflite(surveillance_model.autoencoder, 'autoencoder')
    surveillance_model.convert_to_tflite(surveillance_model.classifier, 'classifier')

    print("Models saved and exported successfully!")
    print("Training pipeline completed!")


if __name__ == "__main__":
    main()