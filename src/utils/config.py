# src/utils/config.py
import yaml
import logging
from typing import Dict, Any
from pathlib import Path

class ConfigManager:
    def __init__(self, config_path: str = "config/app_config.yaml"):
        self.logger = logging.getLogger(__name__)
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Failed to load config: {str(e)}")
            raise

    def get_firebase_config(self) -> Dict[str, str]:
        """Get Firebase configuration"""
        return {
            'credentials_path': self.config['firebase']['credentials_path']
        }

    def get_collector_config(self) -> Dict[str, Any]:
        """Get data collector configuration"""
        return self.config['collector']

    def get_processing_config(self) -> Dict[str, Any]:
        """Get data processing configuration"""
        return self.config['processing']