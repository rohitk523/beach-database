# src/database/firebase_manager.py
from typing import List, Dict, Any
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import logging
from collectors.base_collector import BeachData

class FirebaseManager:
    def __init__(self, cred_path: str):
        """
        Initialize Firestore connection
        
        Args:
            cred_path: Path to Firebase credentials JSON
        """
        self.logger = logging.getLogger(__name__)
        self._initialize_firebase(cred_path)
        self.batch_size = 500  # Optimize batch size for Firestore limits

    def _initialize_firebase(self, cred_path: str) -> None:
        """Initialize Firestore connection with credentials"""
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            self.beaches_ref = self.db.collection('beachesdata')  # Changed collection name
            self.logger.info("Firestore connection initialized successfully")
        except Exception as e:
            self.logger.error(f"Firestore initialization failed: {str(e)}")
            raise

    def format_beach_data(self, beach: BeachData) -> Dict[str, Any]:
        """Format beach data for Firestore storage"""
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
                'geopoint': firestore.GeoPoint(beach.latitude, beach.longitude)
            },
            'details': {
                'description': beach.description,
                'amenities': beach.amenities
            },
            'metadata': {
                'last_updated': beach.last_updated,
                'data_source': beach.data_source
            }
        }

    def batch_upload(self, beaches: List[BeachData]) -> None:
        """
        Upload beach data in batches to minimize Firestore operations
        
        Args:
            beaches: List of BeachData objects to upload
        """
        try:
            batch = self.db.batch()
            count = 0
            
            for beach in beaches:
                beach_data = self.format_beach_data(beach)
                beach_ref = self.beaches_ref.document(beach.id)
                batch.set(beach_ref, beach_data, merge=True)
                count += 1
                
                # Commit when batch size is reached
                if count % self.batch_size == 0:
                    batch.commit()
                    self.logger.info(f"Uploaded batch of {self.batch_size} beaches to beachesdata")
                    batch = self.db.batch()
            
            # Commit any remaining documents
            if count % self.batch_size != 0:
                batch.commit()
                self.logger.info(f"Uploaded final batch of {count % self.batch_size} beaches to beachesdata")
            
            # Update metadata
            self._update_metadata(len(beaches))
            
        except Exception as e:
            self.logger.error(f"Batch upload failed: {str(e)}")
            raise

    def _update_metadata(self, count: int) -> None:
        """Update database metadata"""
        try:
            metadata_ref = self.db.collection('metadata').document('beachesdata')  # Changed metadata document name
            metadata_ref.set({
                'last_updated': firestore.SERVER_TIMESTAMP,
                'total_count': count
            }, merge=True)
        except Exception as e:
            self.logger.error(f"Failed to update metadata: {str(e)}")

    def query_beaches_by_location(self, lat: float, lon: float, radius_km: float) -> List[Dict]:
        """
        Query beaches within radius of coordinates using Firestore
        
        Args:
            lat: Latitude of center point
            lon: Longitude of center point
            radius_km: Search radius in kilometers
        """
        try:
            # Convert radius to lat/lon bounds (approximate)
            # 1 degree of latitude = 111.32 km
            # 1 degree of longitude = 111.32 * cos(latitude) km
            lat_radius = radius_km / 111.32
            lon_radius = radius_km / (111.32 * cos(abs(lat) * 3.14159 / 180))
            
            # Query within bounding box
            results = (self.beaches_ref
                .where('location.latitude', '>=', lat - lat_radius)
                .where('location.latitude', '<=', lat + lat_radius)
                .get())
            
            beaches = []
            for doc in results:
                beach_data = doc.to_dict()
                beach_lat = beach_data['location']['latitude']
                beach_lon = beach_data['location']['longitude']
                
                # Calculate actual distance
                distance = self._calculate_distance(lat, lon, beach_lat, beach_lon)
                if distance <= radius_km:
                    beach_data['distance'] = round(distance, 2)
                    beach_data['id'] = doc.id
                    beaches.append(beach_data)
            
            return sorted(beaches, key=lambda x: x['distance'])
            
        except Exception as e:
            self.logger.error(f"Location query failed: {str(e)}")
            raise

    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two points using Haversine formula
        """
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371  # Earth's radius in kilometers
        
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c