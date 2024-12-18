# src/collectors/osm_collector.py
from typing import List, Dict, Any
from datetime import datetime
import logging
from collectors.base_collector import BaseCollector, BeachData
import overpy
from processors.data_enrichment import DataEnrichmentService
import time

class OSMCollector(BaseCollector):
    def __init__(self):
        self.api = overpy.Overpass()
        self.logger = logging.getLogger(__name__)
        self.enrichment_service = DataEnrichmentService()

    def collect(self, region: Dict[str, float]) -> List[BeachData]:
        """Collect and enrich beach data for a given region"""
        try:
            self.logger.info(f"Starting collection for region: {region.get('name', 'unnamed')}")
            query = self._build_query(region)
            
            try:
                result = self.api.query(query)
            except Exception as e:
                if "timeout" in str(e).lower():
                    self.logger.warning(f"Timeout in region {region.get('name', 'unnamed')}, splitting region")
                    return self._collect_split_region(region)
                raise
            
            beaches = []
            skipped_count = 0
            
            for way in result.ways:
                try:
                    coords = self._extract_coordinates(way)
                    if coords:
                        beach_data = self.process_data(way)
                        if self.validate_data(beach_data):
                            enriched_beach = self.enrichment_service.enrich_beach_data(beach_data)
                            beaches.append(enriched_beach)
                        else:
                            skipped_count += 1
                    else:
                        self.logger.warning(f"No coordinates found for beach {way.id}")
                except Exception as e:
                    self.logger.warning(f"Error processing beach {way.id}: {str(e)}")
                    continue
            
            self.logger.info(f"Region {region.get('name', 'unnamed')}: "
                           f"Collected {len(beaches)} valid beaches, "
                           f"Skipped {skipped_count} invalid entries")
            return beaches
            
        except Exception as e:
            self.logger.error(f"Error collecting data: {str(e)}")
            raise

    def _collect_split_region(self, region: Dict[str, float]) -> List[BeachData]:
        """Handle large regions by splitting them into smaller parts"""
        mid_lat = (region['south'] + region['north']) / 2
        mid_lon = (region['west'] + region['east']) / 2
        
        subregions = [
            {
                'name': f"{region.get('name', 'unnamed')}-SW",
                'south': region['south'],
                'north': mid_lat,
                'west': region['west'],
                'east': mid_lon
            },
            {
                'name': f"{region.get('name', 'unnamed')}-SE",
                'south': region['south'],
                'north': mid_lat,
                'west': mid_lon,
                'east': region['east']
            },
            {
                'name': f"{region.get('name', 'unnamed')}-NW",
                'south': mid_lat,
                'north': region['north'],
                'west': region['west'],
                'east': mid_lon
            },
            {
                'name': f"{region.get('name', 'unnamed')}-NE",
                'south': mid_lat,
                'north': region['north'],
                'west': mid_lon,
                'east': region['east']
            }
        ]
        
        all_beaches = []
        for subregion in subregions:
            try:
                # Add delay between subregion queries to avoid rate limiting
                time.sleep(2)
                beaches = self.collect(subregion)
                all_beaches.extend(beaches)
            except Exception as e:
                self.logger.error(f"Error in subregion {subregion['name']}: {str(e)}")
                continue
                
        return all_beaches

    def process_data(self, way: overpy.Way) -> BeachData:
        """
        Process OSM way data into BeachData format
        
        Args:
            way: Overpass way object
        Returns:
            BeachData object
        """
        try:
            coords = self._extract_coordinates(way)
            if not coords:
                raise ValueError("No coordinates available")
                
            lat, lon = coords
            tags = way.tags
            
            # Get the beach name, don't provide default to allow validation to catch it
            name = tags.get("name")
            
            return BeachData(
                id=f"osm_{way.id}",
                name=name,
                latitude=lat,
                longitude=lon,
                rating=None,  # OSM doesn't provide ratings
                description=self._generate_description(tags, name if name else ""),
                country=tags.get("addr:country"),
                region=tags.get("addr:state") or tags.get("addr:region"),
                amenities=self._extract_amenities(tags),
                last_updated=datetime.now(),
                data_source="OpenStreetMap"
            )
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            raise

    def validate_data(self, data: BeachData) -> bool:
        """
        Validate beach data meets minimum requirements
        
        Args:
            data: BeachData object to validate
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            # Name validation
            if not data.name:
                self.logger.debug("Skipping: Beach has no name")
                return False
                
            # Skip auto-generated names
            if data.name.startswith("Beach ") or data.name.lower() == "unnamed beach":
                self.logger.debug(f"Skipping: Auto-generated or unnamed beach: {data.name}")
                return False
            
            # Skip very short names (likely abbreviations or codes)
            if len(data.name) < 3:
                self.logger.debug(f"Skipping: Name too short: {data.name}")
                return False
                
            # Skip names that are just numbers
            if data.name.isdigit():
                self.logger.debug(f"Skipping: Numeric name: {data.name}")
                return False
                
            # Coordinate validation
            if not isinstance(data.latitude, (int, float)) or not isinstance(data.longitude, (int, float)):
                self.logger.debug(f"Skipping: Invalid coordinate types for {data.name}")
                return False
                
            if not -90 <= data.latitude <= 90:
                self.logger.debug(f"Skipping: Invalid latitude for {data.name}")
                return False
                
            if not -180 <= data.longitude <= 180:
                self.logger.debug(f"Skipping: Invalid longitude for {data.name}")
                return False
                
            return True
            
        except Exception as e:
            self.logger.warning(f"Validation error: {str(e)}")
            return False

    def _extract_coordinates(self, way: overpy.Way) -> tuple[float, float] | None:
        """Extract coordinates from way data"""
        try:
            if hasattr(way, 'center_lat') and hasattr(way, 'center_lon'):
                if way.center_lat is not None and way.center_lon is not None:
                    return float(way.center_lat), float(way.center_lon)

            if way.nodes:
                lats = []
                lons = []
                for node in way.nodes:
                    if hasattr(node, 'lat') and hasattr(node, 'lon'):
                        lats.append(float(node.lat))
                        lons.append(float(node.lon))
                
                if lats and lons:
                    return sum(lats) / len(lats), sum(lons) / len(lons)
                    
            return None
            
        except (ValueError, AttributeError) as e:
            self.logger.debug(f"Error extracting coordinates for way {way.id}: {str(e)}")
            return None

    def _generate_description(self, tags: Dict, beach_name: str) -> str | None:
        """Generate description from OSM tags"""
        parts = []
        
        if "description" in tags:
            parts.append(tags["description"])
        
        if "surface" in tags:
            parts.append(f"{beach_name} has a {tags['surface']} surface.")
            
        if "access" in tags:
            parts.append(f"Access is {tags['access']}.")
            
        amenities = self._extract_amenities(tags)
        if amenities:
            parts.append(f"Available amenities include: {', '.join(amenities)}.")
            
        return " ".join(parts) if parts else None

    def _build_query(self, region: Dict[str, float]) -> str:
        """Build Overpass QL query for beach data"""
        return f"""
            [out:json][timeout:60];
            (
              way["natural"="beach"]
                ({region['south']},{region['west']},
                 {region['north']},{region['east']});
              relation["natural"="beach"]
                ({region['south']},{region['west']},
                 {region['north']},{region['east']});
            );
            out body center;
            >;
            out skel qt;
        """

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