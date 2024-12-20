# src/processors/data_cleaner.py
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
from collectors.base_collector import BeachData
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
                image_url=self._clean_image_url(beach.image_url),
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

    def _clean_image_url(self, image_url: str) -> str:
        """Clean and validate image URL"""
        if not image_url:
            return ""
        return image_url.strip()