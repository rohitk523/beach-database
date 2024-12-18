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


