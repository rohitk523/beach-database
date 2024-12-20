# src/collectors/base_collector.py
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
    image_url: str = ""  # Added with default empty string

class BaseCollector(ABC):
    """Abstract base class for all data collectors"""
    
    @abstractmethod
    def collect(self, region: Dict[str, float]) -> Union[List[BeachData], Generator[List[BeachData], None, None]]:
        """
        Collect and enrich beach data for a given region.
        Handles large regions by splitting them automatically.
        """
        try:
            area = self._calculate_area(region)
            region_name = region.get('name', 'unnamed')
            self.logger.info(f"Starting collection for region: {region_name}")
            
            if area > self.max_area and area > self.min_area:
                self.logger.info(f"Region {region_name} too large ({area:.2f} sq deg), splitting...")
                return self._collect_split_region(region)  # Returns generator
            
            beaches = self._collect_with_retry(region)
            
            if not beaches and area > self.min_area:
                self.logger.info(f"No results for {region_name}, trying split collection...")
                return self._collect_split_region(region)  # Returns generator
            
            return beaches
            
        except Exception as e:
            self.logger.error(f"Error collecting data: {str(e)}")
            if "timeout" in str(e).lower():
                return self._handle_timeout(region)
            raise

    @abstractmethod
    def validate_data(self, data: BeachData) -> bool:
        """Validate collected beach data"""
        pass

    @abstractmethod
    def process_data(self, raw_data: Any) -> BeachData:
        """Process raw data into BeachData format"""
        pass