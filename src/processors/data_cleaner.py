# src/processors/data_cleaner.py
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
from ..collectors.base_collector import BeachData
import logging

class DataCleaner:
    """Cleans and standardizes raw beach data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def clean_beach_data(self, beach: BeachData) -> BeachData:
        """Clean and standardize beach data"""
        try:
            return BeachData(
                id=beach.id,
                name=self._clean_name(beach.name),
                latitude=self._clean_coordinate(beach.latitude),
                longitude=self._clean_coordinate(beach.longitude),
                rating=self._clean_rating(beach.rating),
                description=self._clean_description(beach.description),
                country=self._clean_country(beach.country),
                region=self._clean_region(beach.region),
                amenities=self._clean_amenities(beach.amenities),
                last_updated=datetime.now(),
                data_source=beach.data_source
            )
        except Exception as e:
            self.logger.error(f"Error cleaning beach data: {str(e)}")
            raise

    def _clean_name(self, name: str) -> str:
        """Standardize beach name format"""
        if not name:
            return "Unnamed Beach"
        
        # Remove extra whitespace and standardize format
        name = " ".join(name.strip().split())
        
        # Ensure "Beach" is properly capitalized if present
        name = re.sub(r'\bbeach\b', 'Beach', name, flags=re.IGNORECASE)
        
        return name

    def _clean_coordinate(self, coord: float) -> float:
        """Validate and round coordinates"""
        if coord is None:
            raise ValueError("Coordinate cannot be None")
        return round(float(coord), 6)

    def _clean_rating(self, rating: Optional[float]) -> Optional[float]:
        """Standardize rating to 0-5 scale"""
        if rating is None:
            return None
            
        try:
            rating = float(rating)
            # Convert to 5-star scale if necessary
            if rating > 5:
                rating = (rating / 10) * 5
            return round(min(max(rating, 0), 5), 1)
        except (ValueError, TypeError):
            return None

    def _clean_description(self, description: Optional[str]) -> Optional[str]:
        """Clean and format description text"""
        if not description:
            return None
            
        # Remove extra whitespace
        description = " ".join(description.strip().split())
        
        # Capitalize first letter
        description = description[0].upper() + description[1:]
        
        # Ensure proper punctuation
        if description and not description.endswith(('.', '!', '?')):
            description += '.'
            
        return description

    def _clean_country(self, country: Optional[str]) -> Optional[str]:
        """Standardize country names"""
        if not country:
            return None
            
        country = country.strip().upper()
        # Add country name standardization logic here
        return country

    def _clean_region(self, region: Optional[str]) -> Optional[str]:
        """Clean and standardize region names"""
        if not region:
            return None
        return region.strip()

    def _clean_amenities(self, amenities: List[str]) -> List[str]:
        """Standardize amenity names and remove duplicates"""
        if not amenities:
            return []
            
        cleaned = []
        for amenity in amenities:
            # Standardize format
            amenity = amenity.strip().lower()
            amenity = " ".join(amenity.split())
            amenity = amenity.title()
            
            if amenity and amenity not in cleaned:
                cleaned.append(amenity)
                
        return sorted(cleaned)


# src/processors/geo_processor.py
from typing import Dict, Tuple, List
import math
from geopy.distance import geodesic
import logging

class GeoProcessor:
    """Handles geographic data processing and calculations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def calculate_distance(self, coord1: Tuple[float, float], 
                         coord2: Tuple[float, float]) -> float:
        """Calculate distance between two coordinates in kilometers"""
        try:
            return geodesic(coord1, coord2).kilometers
        except Exception as e:
            self.logger.error(f"Error calculating distance: {str(e)}")
            raise

    def get_nearby_points(self, latitude: float, longitude: float, 
                         radius_km: float) -> Dict[str, Tuple[float, float]]:
        """Get bounding box coordinates for a radius search"""
        # Earth's radius in kilometers
        EARTH_RADIUS = 6371.0
        
        try:
            # Convert latitude/longitude to radians
            lat_rad = math.radians(latitude)
            lon_rad = math.radians(longitude)
            
            # Angular radius
            angular_radius = radius_km / EARTH_RADIUS
            
            # Calculate min/max latitudes
            min_lat = lat_rad - angular_radius
            max_lat = lat_rad + angular_radius
            
            # Calculate min/max longitudes
            delta_lon = math.asin(math.sin(angular_radius) / math.cos(lat_rad))
            min_lon = lon_rad - delta_lon
            max_lon = lon_rad + delta_lon
            
            # Convert back to degrees
            return {
                'min_lat': math.degrees(min_lat),
                'max_lat': math.degrees(max_lat),
                'min_lon': math.degrees(min_lon),
                'max_lon': math.degrees(max_lon)
            }
        except Exception as e:
            self.logger.error(f"Error calculating nearby points: {str(e)}")
            raise

    def create_geohash(self, latitude: float, longitude: float, precision: int = 8) -> str:
        """Create geohash for location-based queries"""
        try:
            # Implementation of geohash algorithm
            # This is a simplified version - you might want to use a library like python-geohash
            base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
            lat_range = (-90.0, 90.0)
            lon_range = (-180.0, 180.0)
            geohash = []
            
            bits = [16, 8, 4, 2, 1]
            is_even = True
            bit = 0
            ch = 0
            
            while len(geohash) < precision:
                if is_even:
                    mid = (lon_range[0] + lon_range[1]) / 2
                    if longitude > mid:
                        ch |= bits[bit]
                        lon_range = (mid, lon_range[1])
                    else:
                        lon_range = (lon_range[0], mid)
                else:
                    mid = (lat_range[0] + lat_range[1]) / 2
                    if latitude > mid:
                        ch |= bits[bit]
                        lat_range = (mid, lat_range[1])
                    else:
                        lat_range = (lat_range[0], mid)
                    
                is_even = not is_even
                
                if bit < 4:
                    bit += 1
                else:
                    geohash.append(base32[ch])
                    bit = 0
                    ch = 0
                    
            return ''.join(geohash)
            
        except Exception as e:
            self.logger.error(f"Error creating geohash: {str(e)}")
            raise


# src/processors/rating_processor.py
from typing import List, Dict, Optional
import statistics
import logging

class RatingProcessor:
    """Processes and normalizes beach ratings"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def normalize_rating(self, ratings: List[float]) -> Optional[float]:
        """Normalize multiple ratings into a single score"""
        try:
            if not ratings:
                return None
                
            valid_ratings = [r for r in ratings if r is not None]
            if not valid_ratings:
                return None
                
            # Calculate weighted average if we have multiple ratings
            return round(statistics.mean(valid_ratings), 1)
            
        except Exception as e:
            self.logger.error(f"Error normalizing ratings: {str(e)}")
            return None

    def calculate_rating_stats(self, ratings: List[float]) -> Dict[str, float]:
        """Calculate rating statistics"""
        try:
            valid_ratings = [r for r in ratings if r is not None]
            if not valid_ratings:
                return {
                    'average': None,
                    'median': None,
                    'std_dev': None,
                    'count': 0
                }
                
            return {
                'average': round(statistics.mean(valid_ratings), 1),
                'median': round(statistics.median(valid_ratings), 1),
                'std_dev': round(statistics.stdev(valid_ratings), 2) if len(valid_ratings) > 1 else 0,
                'count': len(valid_ratings)
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating rating stats: {str(e)}")
            raise