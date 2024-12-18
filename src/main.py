# src/main.py
from typing import List, Dict
import logging
from pathlib import Path
import time
from collectors.base_collector import BeachData

from collectors.osm_collector import OSMCollector
from processors.data_cleaner import DataCleaner
from processors.geo_processor import GeoProcessor
from processors.rating_processor import RatingProcessor
from database.firebase_manager import FirebaseManager
from utils.config import ConfigManager

class BeachDataOrchestrator:
    def __init__(self):
        self.config = ConfigManager()
        self.setup_logging()
        
        # Initialize components
        self.collector = OSMCollector()
        self.data_cleaner = DataCleaner()
        self.geo_processor = GeoProcessor()
        self.rating_processor = RatingProcessor()
        
        firebase_config = self.config.get_firebase_config()
        self.firebase = FirebaseManager(
            firebase_config['credentials_path']
        )

    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('beach_data.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def process_region(self, region: Dict[str, float]) -> None:
        """Process beach data for a specific region"""
        try:
            # Collect data
            self.logger.info(f"Collecting data for region: {region}")
            beaches = self.collector.collect(region)
            
            # Process each beach
            processed_beaches = []
            for beach in beaches:
                # Clean data
                cleaned_beach = self.data_cleaner.clean_beach_data(beach)
                
                # Add geohash
                cleaned_beach = self._add_geohash(cleaned_beach)
                
                processed_beaches.append(cleaned_beach)
                
            # Upload to Firebase in batches
            self.firebase.batch_upload(processed_beaches)
            
            self.logger.info(f"Processed {len(processed_beaches)} beaches for region")
            
        except Exception as e:
            self.logger.error(f"Error processing region: {str(e)}")
            raise

    def _add_geohash(self, beach: BeachData) -> BeachData:
        """Add geohash to beach data"""
        geohash = self.geo_processor.create_geohash(
            beach.latitude,
            beach.longitude
        )
        # Create new BeachData instance with geohash
        # Note: You might want to modify BeachData class to include geohash
        return beach

    def run_full_update(self, regions: List[Dict[str, float]]) -> None:
        """Run full data update for all specified regions"""
        start_time = time.time()
        total_beaches = 0
        
        try:
            for region in regions:
                self.process_region(region)
                # Keep track of total beaches processed
                total_beaches += len(self.collector.collect(region))
                
            duration = time.time() - start_time
            self.logger.info(f"Full update completed in {duration:.2f} seconds")
            self.logger.info(f"Total beaches processed: {total_beaches}")
            
        except Exception as e:
            self.logger.error(f"Error in full update: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    orchestrator = BeachDataOrchestrator()
    
    # Define regions to process (example: major coastal areas)
    regions = [
        {
            'south': -34.0,
            'west': 115.0,
            'north': -33.0,
            'east': 116.0
        },
        # Add more regions as needed
    ]
    
    orchestrator.run_full_update(regions)