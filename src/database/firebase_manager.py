# src/database/firebase_manager.py
from typing import List, Dict, Any
import firebase_admin
from firebase_admin import credentials, db
from datetime import datetime
import logging
from ..collectors.base_collector import BeachData

class FirebaseManager:
    def __init__(self, cred_path: str, database_url: str):
        """
        Initialize Firebase connection
        
        Args:
            cred_path: Path to Firebase credentials JSON
            database_url: Firebase database URL
        """
        self.logger = logging.getLogger(__name__)
        self._initialize_firebase(cred_path, database_url)
        self.batch_size = 500  # Optimize batch size for Firebase limits

    def _initialize_firebase(self, cred_path: str, database_url: str) -> None:
        """Initialize Firebase connection with credentials"""
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred, {
                    'databaseURL': database_url
                })
            self.db = db.reference('beaches')
            self.logger.info("Firebase connection initialized successfully")
        except Exception as e:
            self.logger.error(f"Firebase initialization failed: {str(e)}")
            raise

    def format_beach_data(self, beach: BeachData) -> Dict[str, Any]:
        """Format beach data for Firebase storage"""
        return {
            'basic_info': {
                'name': beach.name,
                'country': beach.country,
                'region': beach.region,
                'rating': beach.rating
            },
            'location': {
                'latitude': beach.latitude,
                'longitude': beach.longitude,
                'geohash': self._calculate_geohash(beach.latitude, beach.longitude)
            },
            'details': {
                'description': beach.description,
                'amenities': beach.amenities
            },
            'metadata': {
                'last_updated': beach.last_updated.isoformat(),
                'data_source': beach.data_source
            }
        }

    def batch_upload(self, beaches: List[BeachData]) -> None:
        """
        Upload beach data in batches to minimize Firebase operations
        
        Args:
            beaches: List of BeachData objects to upload
        """
        try:
            for i in range(0, len(beaches), self.batch_size):
                batch = beaches[i:i + self.batch_size]
                updates = {}
                
                for beach in batch:
                    beach_data = self.format_beach_data(beach)
                    updates[f"data/{beach.id}"] = beach_data
                    
                    # Update indices for efficient querying
                    if beach.country:
                        updates[f"indices/by_country/{beach.country}/{beach.id}"] = True
                    if beach.rating:
                        updates[f"indices/by_rating/{int(beach.rating)}/{beach.id}"] = True
                
                self.db.update(updates)
                self.logger.info(f"Uploaded batch of {len(batch)} beaches")
                
            # Update metadata
            self._update_metadata(len(beaches))
            
        except Exception as e:
            self.logger.error(f"Batch upload failed: {str(e)}")
            raise

    def _update_metadata(self, count: int) -> None:
        """Update database metadata"""
        metadata_ref = db.reference('beaches/metadata')
        metadata_ref.update({
            'last_updated': datetime.now().isoformat(),
            'total_count': count
        })

    def query_beaches_by_location(self, lat: float, lon: float, radius_km: float) -> List[Dict]:
        """
        Query beaches within radius of coordinates
        
        Args:
            lat: Latitude of center point
            lon: Longitude of center point
            radius_km: Search radius in kilometers
        """
        try:
            # Calculate bounding box for initial filtering
            bbox = self._calculate_bounding_box(lat, lon, radius_km)
            
            # Query using geohash prefix
            geohash_prefix = self._calculate_geohash(lat, lon)[:5]
            
            beaches = []
            beaches_ref = self.db.child('data')
            results = beaches_ref.order_by_child('location/geohash')\
                               .start_at(geohash_prefix)\
                               .end_at(geohash_prefix + '\uf8ff')\
                               .get()
            
            if results:
                for beach_id, beach_data in results.items():
                    if self._is_within_radius(
                        lat, lon,
                        beach_data['location']['latitude'],
                        beach_data['location']['longitude'],
                        radius_km
                    ):
                        beaches.append({
                            'id': beach_id,
                            **beach_data
                        })
            
            return beaches
            
        except Exception as e:
            self.logger.error(f"Location query failed: {str(e)}")
            raise

    def update_beach(self, beach_id: str, updates: Dict[str, Any]) -> None:
        """Update specific beach data"""
        try:
            beach_ref = self.db.child(f'data/{beach_id}')
            beach_ref.update(updates)
            self.logger.info(f"Updated beach {beach_id}")
        except Exception as e:
            self.logger.error(f"Beach update failed: {str(e)}")
            raise

    def _calculate_geohash(self, lat: float, lon: float) -> str:
        """Calculate geohash for location indexing"""
        # Implementation of geohash calculation
        pass

    def _calculate_bounding_box(self, lat: float, lon: float, radius_km: float) -> Dict[str, float]:
        """Calculate bounding box for radius search"""
        # Implementation of bounding box calculation
        pass

    def _is_within_radius(self, lat1: float, lon1: float, lat2: float, lon2: float, radius_km: float) -> bool:
        """Check if point is within radius"""
        # Implementation of distance calculation
        pass