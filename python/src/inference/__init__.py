"""
Market Surveillance Inference Package
"""

# Only import config by default to avoid heavy dependencies
from .config import Config

# Other imports available on demand:
# from .model_loader import ModelLoader
# from .data_fetcher import DataFetcher 
# from .feature_engineer import FeatureEngineer
# from .grpc_server import MarketSurveillanceService

__all__ = [
    'Config',
    'ModelLoader', 
    'DataFetcher',
    'FeatureEngineer',
    'MarketSurveillanceService'
]
__version__ = '1.0.0'