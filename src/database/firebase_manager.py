# src/database/firebase_manager.py
from typing import List, Dict, Any
import firebase_admin
from firebase_admin import credentials, firestore
import math
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
        self.batch_size = 500

    def _initialize_firebase(self, cred_path: str) -> None:
        """Initialize Firestore connection"""
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            self.beaches_ref = self.db.collection('coastal_locations')
            self.logger.info("Firestore connection initialized successfully")
        except Exception as e:
            self.logger.error(f"Firestore initialization failed: {str(e)}")
            raise

    def format_beach_data(self, beach: BeachData) -> Dict[str, Any]:
        """Format beach data with flat structure"""
        return {
            'id': beach.id,
            'name': beach.name,
            'country': beach.country,
            'region': beach.region,
            'rating': beach.rating,
            'latitude': beach.latitude,
            'longitude': beach.longitude,
            'geopoint': firestore.GeoPoint(beach.latitude, beach.longitude),
            'description': beach.description,
            'amenities': beach.amenities,
            'image_url': beach.image_url,
            'last_updated': beach.last_updated,
            'data_source': beach.data_source
        }

    def batch_upload(self, beaches: List[BeachData]) -> None:
        """
        Upload beach data in batches
        
        Args:
            beaches: List of BeachData objects to upload
        """
        try:
            batch = self.db.batch()
            count = 0
            
            for beach in beaches:
                beach_data = self.format_beach_data(beach)
                beach_ref = self.beaches_ref.document(beach.id)
                batch.set(beach_ref, beach_data)
                count += 1
                
                if count % self.batch_size == 0:
                    batch.commit()
                    self.logger.info(f"Uploaded batch of {self.batch_size} locations")
                    batch = self.db.batch()
            
            # Commit remaining documents
            if count % self.batch_size != 0:
                batch.commit()
                self.logger.info(f"Uploaded final batch of {count % self.batch_size} locations")
            
            self._update_metadata(count)
            
        except Exception as e:
            self.logger.error(f"Batch upload failed: {str(e)}")
            raise

    def _update_metadata(self, count: int) -> None:
        """Update database metadata"""
        try:
            metadata_ref = self.db.collection('metadata').document('coastal_locations')
            metadata_ref.set({
                'last_updated': firestore.SERVER_TIMESTAMP,
                'total_locations': count
            }, merge=True)
        except Exception as e:
            self.logger.error(f"Failed to update metadata: {str(e)}")

    def query_beaches_by_location(self, lat: float, lon: float, radius_km: float) -> List[Dict]:
        """
        Query beaches within radius of coordinates
        
        Args:
            lat: Latitude of center point
            lon: Longitude of center point
            radius_km: Search radius in kilometers
        """
        try:
            # Convert radius to lat/lon bounds (approximate)
            lat_radius = radius_km / 111.32
            lon_radius = radius_km / (111.32 * abs(math.cos(math.radians(lat))))
            
            # Query within bounding box
            results = (self.beaches_ref
                .where('latitude', '>=', lat - lat_radius)
                .where('latitude', '<=', lat + lat_radius)
                .get())
            
            locations = []
            for doc in results:
                location_data = doc.to_dict()
                location_lat = location_data['latitude']
                location_lon = location_data['longitude']
                
                # Calculate actual distance
                distance = self._calculate_distance(lat, lon, location_lat, location_lon)
                if distance <= radius_km:
                    location_data['distance'] = round(distance, 2)
                    locations.append(location_data)
            
            return sorted(locations, key=lambda x: x['distance'])
            
        except Exception as e:
            self.logger.error(f"Location query failed: {str(e)}")
            raise

    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using Haversine formula"""
        R = 6371  # Earth's radius in kilometers
        
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        return R * c

    def get_beach_by_id(self, beach_id: str) -> Dict[str, Any]:
        """
        Retrieve a beach by its ID
        
        Args:
            beach_id: The ID of the beach to retrieve
            
        Returns:
            Dict containing beach data or None if not found
        """
        try:
            doc = self.beaches_ref.document(beach_id).get()
            if doc.exists:
                return doc.to_dict()
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving beach {beach_id}: {str(e)}")
            raise

    def update_beach(self, beach_id: str, updates: Dict[str, Any]) -> None:
        """
        Update specific fields of a beach document
        
        Args:
            beach_id: ID of the beach to update
            updates: Dictionary of fields to update
        """
        try:
            self.beaches_ref.document(beach_id).update(updates)
            self.logger.info(f"Updated beach {beach_id}")
        except Exception as e:
            self.logger.error(f"Error updating beach {beach_id}: {str(e)}")
            raise

    def delete_beach(self, beach_id: str) -> None:
        """
        Delete a beach document
        
        Args:
            beach_id: ID of the beach to delete
        """
        try:
            self.beaches_ref.document(beach_id).delete()
            self.logger.info(f"Deleted beach {beach_id}")
        except Exception as e:
            self.logger.error(f"Error deleting beach {beach_id}: {str(e)}")
            raise

    def search_beaches(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Search beaches by name
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of matching beach documents
        """
        try:
            # Perform case-insensitive search
            results = (self.beaches_ref
                .where('name', '>=', query)
                .where('name', '<=', query + '\uf8ff')
                .limit(limit)
                .get())
            
            return [doc.to_dict() for doc in results]
        except Exception as e:
            self.logger.error(f"Error searching beaches: {str(e)}")
            raise