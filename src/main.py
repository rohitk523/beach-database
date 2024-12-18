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
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / 'beach_data.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def process_region(self, region: Dict[str, float]) -> int:
        """
        Process beach data for a specific region
        
        Returns:
            int: Number of beaches processed
        """
        try:
            region_name = region.get('name', f"Region {region['south']},{region['west']}")
            self.logger.info(f"Starting processing for region: {region_name}")
            
            # Collect data
            start_time = time.time()
            beaches = self.collector.collect(region)
            collection_time = time.time() - start_time
            self.logger.info(f"Collected {len(beaches)} beaches in {collection_time:.2f} seconds")
            
            if not beaches:
                self.logger.warning(f"No beaches found in region {region_name}")
                return 0
            
            # Process each beach
            processed_beaches = []
            processed_count = 0
            
            for beach in beaches:
                try:
                    # Clean data
                    cleaned_beach = self.data_cleaner.clean_beach_data(beach)
                    
                    # Add geohash for spatial queries
                    cleaned_beach = self._add_geohash(cleaned_beach)
                    
                    # Add to batch
                    processed_beaches.append(cleaned_beach)
                    processed_count += 1
                    
                    # Upload in batches of 500
                    if len(processed_beaches) >= 500:
                        self.firebase.batch_upload(processed_beaches)
                        self.logger.info(f"Uploaded batch of {len(processed_beaches)} beaches")
                        processed_beaches = []
                        
                except Exception as e:
                    self.logger.error(f"Error processing beach {beach.id}: {str(e)}")
                    continue
            
            # Upload any remaining beaches
            if processed_beaches:
                self.firebase.batch_upload(processed_beaches)
                self.logger.info(f"Uploaded final batch of {len(processed_beaches)} beaches")
            
            total_time = time.time() - start_time
            self.logger.info(
                f"Region {region_name} complete: "
                f"Processed {processed_count} beaches in {total_time:.2f} seconds"
            )
            
            return processed_count
            
        except Exception as e:
            self.logger.error(f"Error processing region {region.get('name', 'unnamed')}: {str(e)}")
            raise

    def _add_geohash(self, beach: BeachData) -> BeachData:
        """Add geohash to beach data"""
        try:
            geohash = self.geo_processor.create_geohash(
                beach.latitude,
                beach.longitude
            )
            # Assuming BeachData has a geohash field or can accept additional attributes
            setattr(beach, 'geohash', geohash)
            return beach
        except Exception as e:
            self.logger.warning(f"Error adding geohash for beach {beach.id}: {str(e)}")
            return beach

    def run_full_update(self, regions: List[Dict[str, float]]) -> None:
        """Run full data update for all specified regions"""
        start_time = time.time()
        total_beaches = 0
        failed_regions = []
        
        self.logger.info(f"Starting full update for {len(regions)} regions")
        
        for i, region in enumerate(regions, 1):
            try:
                self.logger.info(f"Processing region {i}/{len(regions)}")
                beaches_processed = self.process_region(region)
                total_beaches += beaches_processed
                
                # Add delay between regions to avoid rate limiting
                if i < len(regions):
                    time.sleep(2)
                    
            except Exception as e:
                failed_regions.append(region.get('name', f"Region {i}"))
                self.logger.error(f"Failed to process region {region.get('name', f'Region {i}')}: {str(e)}")
                continue
        
        duration = time.time() - start_time
        
        # Final summary
        self.logger.info("=== Update Summary ===")
        self.logger.info(f"Total time: {duration:.2f} seconds")
        self.logger.info(f"Total beaches processed: {total_beaches}")
        self.logger.info(f"Regions processed: {len(regions) - len(failed_regions)}/{len(regions)}")
        
        if failed_regions:
            self.logger.warning("Failed regions:")
            for region in failed_regions:
                self.logger.warning(f"- {region}")

if __name__ == "__main__":
    orchestrator = BeachDataOrchestrator()
    
    # Define regions to process
    regions = [
        {
            'name': "Western Australia",
            'south': -35.0,
            'north': -13.0,
            'west': 112.0,
            'east': 129.0
        },
        {
            'name': "Eastern Australia",
            'south': -39.0,
            'north': -10.0,
            'west': 140.0,
            'east': 154.0
        },
        {
            'name': "Mediterranean Coast",
            'south': 30.0,
            'north': 45.0,
            'west': -6.0,
            'east': 36.0
        }
        # Add more regions as needed
    ]
    
    orchestrator.run_full_update(regions)