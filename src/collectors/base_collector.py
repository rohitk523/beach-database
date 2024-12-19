from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union, Generator, Iterator
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
    def collect(self, region: Dict[str, float]) -> Union[List[BeachData], Generator[List[BeachData], None, None]]:
        """
        Collect beach data for a given region
        
        Returns either:
        - A list of BeachData for single region collection
        - A generator yielding lists of BeachData for split region collection
        """
        pass

    @abstractmethod
    def validate_data(self, data: BeachData) -> bool:
        """Validate collected beach data"""
        pass

    @abstractmethod
    def process_data(self, raw_data: Any) -> BeachData:
        """Process raw data into BeachData format"""
        pass