# src/collectors/base_collector.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class BeachData:
    id: str
    name: str
    latitude: float
    longitude: float
    rating: float | None
    description: str | None
    country: str | None
    region: str | None
    amenities: List[str]
    last_updated: datetime
    data_source: str

class BaseCollector(ABC):
    """Abstract base class for all data collectors"""
    
    @abstractmethod
    def collect(self, region: Dict[str, float]) -> List[BeachData]:
        """Collect beach data for a given region"""
        pass

    @abstractmethod
    def validate_data(self, data: BeachData) -> bool:
        """Validate collected beach data"""
        pass

    @abstractmethod
    def process_data(self, raw_data: Any) -> BeachData:
        """Process raw data into BeachData format"""
        pass