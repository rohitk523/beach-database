# src/collectors/osm_collector.py
import overpy
from typing import List, Dict, Any
from datetime import datetime
from .base_collector import BaseCollector, BeachData
import logging

class OSMCollector(BaseCollector):
    def __init__(self):
        self.api = overpy.Overpass()
        self.logger = logging.getLogger(__name__)

    def collect(self, region: Dict[str, float]) -> List[BeachData]:
        """
        Collect beach data from OpenStreetMap for a given region
        
        Args:
            region: Dict with 'south', 'west', 'north', 'east' coordinates
        """
        try:
            query = self._build_query(region)
            result = self.api.query(query)
            
            beaches = []
            for way in result.ways:
                try:
                    beach_data = self.process_data(way)
                    if self.validate_data(beach_data):
                        beaches.append(beach_data)
                except Exception as e:
                    self.logger.warning(f"Error processing beach {way.id}: {str(e)}")
                    continue
                    
            return beaches
            
        except Exception as e:
            self.logger.error(f"Error collecting data: {str(e)}")
            raise

    def _build_query(self, region: Dict[str, float]) -> str:
        """Build Overpass QL query for beach data"""
        return f"""
            [out:json][timeout:25];
            (
              way["natural"="beach"]
                ({region['south']},{region['west']},
                 {region['north']},{region['east']});
              relation["natural"="beach"]
                ({region['south']},{region['west']},
                 {region['north']},{region['east']});
            );
            out body;
            >;
            out skel qt;
        """

    def validate_data(self, data: BeachData) -> bool:
        """
        Validate beach data meets minimum requirements
        """
        if not data.name or not data.latitude or not data.longitude:
            return False
            
        if not -90 <= data.latitude <= 90:
            return False
            
        if not -180 <= data.longitude <= 180:
            return False
            
        return True

    def process_data(self, raw_data: Any) -> BeachData:
        """
        Process OSM way data into BeachData format
        """
        tags = raw_data.tags
        
        return BeachData(
            id=f"osm_{raw_data.id}",
            name=tags.get("name", f"Beach {raw_data.id}"),
            latitude=float(raw_data.center_lat),
            longitude=float(raw_data.center_lon),
            rating=None,  # OSM doesn't provide ratings
            description=self._generate_description(tags),
            country=tags.get("addr:country"),
            region=tags.get("addr:state") or tags.get("addr:region"),
            amenities=self._extract_amenities(tags),
            last_updated=datetime.now(),
            data_source="OpenStreetMap"
        )

    def _generate_description(self, tags: Dict) -> str:
        """Generate human-readable description from OSM tags"""
        parts = []
        
        if "description" in tags:
            parts.append(tags["description"])
        
        if "surface" in tags:
            parts.append(f"Beach surface: {tags['surface']}")
            
        if "access" in tags:
            parts.append(f"Access: {tags['access']}")
            
        return " ".join(parts) if parts else None

    def _extract_amenities(self, tags: Dict) -> List[str]:
        """Extract available amenities from OSM tags"""
        amenities = []
        relevant_tags = [
            "shower", "toilets", "parking", "drinking_water",
            "restaurant", "cafe", "lifeguard", "changing_room"
        ]
        
        for tag in relevant_tags:
            if f"amenity:{tag}" in tags or tag in tags:
                amenities.append(tag.replace("_", " ").title())
                
        return amenities