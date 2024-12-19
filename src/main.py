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
            total_processed = 0
            
            # Process data in chunks from collector
            beaches_iterator = self.collector.collect(region)
            
            # If it's a regular list (non-split region), wrap it in a list
            if isinstance(beaches_iterator, list):
                beaches_iterator = [beaches_iterator]
                
            for beaches_chunk in beaches_iterator:
                try:
                    if not beaches_chunk:
                        continue
                    
                    # Process each beach in the chunk
                    processed_beaches = []
                    
                    for beach in beaches_chunk:
                        try:
                            # Clean data
                            cleaned_beach = self.data_cleaner.clean_beach_data(beach)
                            
                            # Add geohash for spatial queries
                            cleaned_beach = self._add_geohash(cleaned_beach)
                            
                            # Add to current chunk's batch
                            processed_beaches.append(cleaned_beach)
                            total_processed += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error processing beach {beach.id}: {str(e)}")
                            continue
                    
                    # Upload this chunk's beaches immediately
                    if processed_beaches:
                        try:
                            self.firebase.batch_upload(processed_beaches)
                            self.logger.info(f"Uploaded batch of {len(processed_beaches)} beaches from sub-region")
                        except Exception as e:
                            self.logger.error(f"Failed to upload batch from sub-region: {str(e)}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing chunk: {str(e)}")
                    continue
            
            total_time = time.time() - start_time
            self.logger.info(
                f"Region {region_name} complete: "
                f"Processed {total_processed} beaches in {total_time:.2f} seconds"
            )
            
            return total_processed
                
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
        'name': "Mediterranean Coast",
        'south': 30.0,
        'north': 45.0,
        'west': -6.0,
        'east': 36.0
    },
    {
        'name': "California Coast",
        'south': 32.5,
        'north': 42.0,
        'west': -125.0,
        'east': -114.0
    },
    {
        'name': "Caribbean Sea",
        'south': 10.0,
        'north': 20.0,
        'west': -85.0,
        'east': -60.0
    },
    {
        'name': "Florida Coast",
        'south': 24.0,
        'north': 31.0,
        'west': -87.0,
        'east': -80.0
    },
    {
        'name': "Hawaii",
        'south': 18.5,
        'north': 20.5,
        'west': -156.5,
        'east': -154.5
    },
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
        'name': "South Africa",
        'south': -34.0,
        'north': -22.0,
        'west': 16.5,
        'east': 34.5
    },
    {
        'name': "Mediterranean Coast",
        'south': 30.0,
        'north': 45.0,
        'west': -6.0,
        'east': 36.0
    },
    {
        'name': "Gulf of Thailand",
        'south': 6.0,
        'north': 13.0,
        'west': 99.0,
        'east': 106.0
    },
    {
        'name': "Indochina Peninsula",
        'south': 6.0,
        'north': 23.0,
        'west': 97.0,
        'east': 110.0
    },
    {
        'name': "Bali",
        'south': -9.5,
        'north': -8.0,
        'west': 114.0,
        'east': 116.5
    },
    {
        'name': "South East Asia",
        'south': -10.0,
        'north': 25.0,
        'west': 95.0,
        'east': 140.0
    },
    {
        'name': "East Coast of India",
        'south': 8.0,
        'north': 22.0,
        'west': 80.0,
        'east': 92.0
    },
    {
        'name': "East Coast of Africa",
        'south': -35.0,
        'north': 5.0,
        'west': 30.0,
        'east': 60.0
    },
    {
        'name': "Brazilian Coast",
        'south': -33.0,
        'north': 5.0,
        'west': -75.0,
        'east': -34.0
    },
    {
        'name': "Pacific Northwest Coast (USA/Canada)",
        'south': 42.0,
        'north': 60.0,
        'west': -125.0,
        'east': -114.0
    },
    {
        'name': "Indian Ocean Islands",
        'south': -26.0,
        'north': -2.0,
        'west': 40.0,
        'east': 100.0
    },
    {
        'name': "Arabian Gulf Coast",
        'south': 24.0,
        'north': 30.0,
        'west': 48.0,
        'east': 56.0
    },
    {
        'name': "New Zealand",
        'south': -47.0,
        'north': -34.0,
        'west': 166.0,
        'east': 179.0
    },
    {
        'name': "French Polynesia",
        'south': -23.0,
        'north': -15.0,
        'west': -150.0,
        'east': -130.0
    }
]

    
    orchestrator.run_full_update(regions)