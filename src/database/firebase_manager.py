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
        Initialize Firebase connection
        
        Args:
            cred_path: Path to Firebase credentials JSON
        """
        self.logger = logging.getLogger(__name__)
        self._initialize_firebase(cred_path)
        self.batch_size = 500  # Optimize batch size for Firestore limits

    def _initialize_firebase(self, cred_path: str) -> None:
        """Initialize Firebase connection with credentials"""
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            self.beaches_collection = self.db.collection('beaches')
            self.logger.info("Firebase connection initialized successfully")
        except Exception as e:
            self.logger.error(f"Firebase initialization failed: {str(e)}")
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
                'geohash': self._calculate_geohash(beach.latitude, beach.longitude)
            },
            'details': {
                'description': beach.description,
                'amenities': beach.amenities
            },
            'metadata': {
                'last_updated': beach.last_updated,  # Firestore handles datetime objects directly
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
            for i in range(0, len(beaches), self.batch_size):
                batch = self.db.batch()
                current_beaches = beaches[i:i + self.batch_size]
                
                for beach in current_beaches:
                    beach_data = self.format_beach_data(beach)
                    beach_ref = self.beaches_collection.document(beach.id)
                    batch.set(beach_ref, beach_data, merge=True)
                    
                    # Update indices using subcollections
                    if beach.country:
                        country_ref = self.db.collection('indices').document('by_country')
                        batch.set(country_ref, {
                            beach.country: firestore.ArrayUnion([beach.id])
                        }, merge=True)
                    
                    if beach.rating:
                        rating_ref = self.db.collection('indices').document('by_rating')
                        batch.set(rating_ref, {
                            str(int(beach.rating)): firestore.ArrayUnion([beach.id])
                        }, merge=True)
                
                batch.commit()
                self.logger.info(f"Uploaded batch of {len(current_beaches)} beaches")
                
            # Update metadata
            self._update_metadata(len(beaches))
            
        except Exception as e:
            self.logger.error(f"Batch upload failed: {str(e)}")
            raise

    def _update_metadata(self, count: int) -> None:
        """Update database metadata"""
        try:
            metadata_ref = self.db.collection('metadata').document('beaches')
            metadata_ref.set({
                'last_updated': datetime.now(),
                'total_count': firestore.Increment(count)
            }, merge=True)
        except Exception as e:
            self.logger.error(f"Metadata update failed: {str(e)}")
            raise

    def query_beaches_by_location(self, lat: float, lon: float, radius_km: float) -> List[Dict]:
        """
        Query beaches within radius of coordinates using Firestore geoqueries
        
        Args:
            lat: Latitude of center point
            lon: Longitude of center point
            radius_km: Search radius in kilometers
        """
        try:
            # Calculate geohash range for the area
            geohash_prefix = self._calculate_geohash(lat, lon)[:5]
            
            # Query using compound queries
            beaches = []
            query = (self.beaches_collection
                    .where('location.geohash', '>=', geohash_prefix)
                    .where('location.geohash', '<=', geohash_prefix + '\uf8ff')
                    .get())
            
            for doc in query:
                beach_data = doc.to_dict()
                if self._is_within_radius(
                    lat, lon,
                    beach_data['location']['latitude'],
                    beach_data['location']['longitude'],
                    radius_km
                ):
                    beaches.append({
                        'id': doc.id,
                        **beach_data
                    })
            
            return beaches
            
        except Exception as e:
            self.logger.error(f"Location query failed: {str(e)}")
            raise

    def update_beach(self, beach_id: str, updates: Dict[str, Any]) -> None:
        """Update specific beach data"""
        try:
            beach_ref = self.beaches_collection.document(beach_id)
            beach_ref.update(updates)
            self.logger.info(f"Updated beach {beach_id}")
        except Exception as e:
            self.logger.error(f"Beach update failed: {str(e)}")
            raise