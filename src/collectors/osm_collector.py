# src/collectors/osm_collector.py
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import logging
import time
from math import ceil, sqrt
import overpy
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from collectors.base_collector import BaseCollector, BeachData
from processors.data_enrichment import DataEnrichmentService

class OSMCollector(BaseCollector):
    """
    Collector for beach data from OpenStreetMap using Overpass API.
    Handles large regions, timeouts, and rate limiting automatically.
    """
    
    def __init__(self):
        self.api = overpy.Overpass()
        self.logger = logging.getLogger(__name__)
        self.enrichment_service = DataEnrichmentService()
        
        # Configuration for region splitting
        self.max_area = 4.0  # Maximum area in square degrees before splitting
        self.min_area = 0.25  # Minimum area to prevent infinite splitting
        self.query_timeout = 60  # Default timeout for Overpass queries
        self.max_retries = 3  # Maximum number of retries for failed queries
        
        # Delays for rate limiting (in seconds)
        self.query_delay = 2  # Delay between queries
        self.split_delay = 3  # Delay between split region queries

    def collect(self, region: Dict[str, float]) -> List[BeachData]:
        """
        Collect and enrich beach data for a given region.
        Handles large regions by splitting them automatically.
        """
        try:
            area = self._calculate_area(region)
            region_name = region.get('name', 'unnamed')
            self.logger.info(f"Starting collection for region: {region_name}")
            
            if area > self.max_area and area > self.min_area:
                self.logger.info(f"Region {region_name} too large ({area:.2f} sq deg), splitting...")
                return self._collect_split_region(region)
            
            beaches = self._collect_with_retry(region)
            
            if not beaches and area > self.min_area:
                self.logger.info(f"No results for {region_name}, trying split collection...")
                return self._collect_split_region(region)
            
            return beaches
            
        except Exception as e:
            self.logger.error(f"Error collecting data: {str(e)}")
            if "timeout" in str(e).lower():
                return self._handle_timeout(region)
            raise

    def should_retry_exception(exc: Exception) -> bool:
        """Determine if an exception should trigger a retry"""
        return isinstance(exc, overpy.exception.OverpassTooManyRequests)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception(should_retry_exception)
    )
    def _collect_with_retry(self, region: Dict[str, float]) -> List[BeachData]:
        """
        Collect data with retry logic for rate limiting and temporary failures.
        """
        beaches = []
        skipped_count = 0
        
        try:
            query = self._build_query(region)
            result = self.api.query(query)
            
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
                        self.logger.debug(f"No coordinates found for beach {way.id}")
                except Exception as e:
                    self.logger.debug(f"Error processing way {way.id}: {str(e)}")
                    continue
            
            self.logger.info(
                f"Region {region.get('name', 'unnamed')}: Collected {len(beaches)} "
                f"valid beaches, Skipped {skipped_count} invalid entries"
            )
            
            time.sleep(self.query_delay)
            return beaches
            
        except overpy.exception.OverpassGatewayTimeout:
            self.logger.warning(f"Timeout for region {region.get('name', 'unnamed')}")
            return []

    def _handle_timeout(self, region: Dict[str, float]) -> List[BeachData]:
        """Handle timeout by splitting region and retrying"""
        area = self._calculate_area(region)
        if area <= self.min_area:
            self.logger.warning(f"Region {region.get('name', 'unnamed')} too small to split further")
            return []
            
        return self._collect_split_region(region)

    def _collect_split_region(self, region: Dict[str, float]) -> List[BeachData]:
        """Split region into smaller parts and collect from each"""
        splits = self._calculate_optimal_splits(region)
        all_beaches = []
        
        for subregion in splits:
            try:
                time.sleep(self.query_delay)
                beaches = self._collect_with_retry(subregion)
                all_beaches.extend(beaches)
                time.sleep(self.split_delay)
                
            except Exception as e:
                self.logger.error(f"Error in subregion {subregion.get('name', 'unnamed')}: {str(e)}")
                continue
                
        return all_beaches

    def _calculate_optimal_splits(self, region: Dict[str, float]) -> List[Dict[str, float]]:
        """Calculate optimal way to split region based on area"""
        area = self._calculate_area(region)
        splits_needed = ceil(area / self.max_area)
        
        if splits_needed <= 1:
            return [region]
            
        splits = []
        lat_range = region['north'] - region['south']
        lon_range = region['east'] - region['west']
        
        lat_splits = ceil(sqrt(splits_needed)) if lat_range > lon_range else 1
        lon_splits = ceil(splits_needed / lat_splits)
        
        lat_step = lat_range / lat_splits
        lon_step = lon_range / lon_splits
        
        for i in range(lat_splits):
            for j in range(lon_splits):
                split_region = {
                    'name': f"{region.get('name', 'unnamed')}-{i}-{j}",
                    'south': region['south'] + (i * lat_step),
                    'north': region['south'] + ((i + 1) * lat_step),
                    'west': region['west'] + (j * lon_step),
                    'east': region['west'] + ((j + 1) * lon_step)
                }
                splits.append(split_region)
                
        return splits

    def _calculate_area(self, region: Dict[str, float]) -> float:
        """Calculate approximate area of region in square degrees"""
        return (region['north'] - region['south']) * (region['east'] - region['west'])

    def _build_query(self, region: Dict[str, float]) -> str:
        """Build Overpass QL query with appropriate timeout"""
        area = self._calculate_area(region)
        timeout = min(180, max(60, int(area * 30)))
        
        return f"""
            [out:json][timeout:{timeout}];
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

    def _extract_coordinates(self, way: overpy.Way) -> Optional[Tuple[float, float]]:
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

    def process_data(self, way: overpy.Way) -> BeachData:
        """Process OSM way data into BeachData format"""
        try:
            coords = self._extract_coordinates(way)
            if not coords:
                raise ValueError("No coordinates available")
                
            lat, lon = coords
            tags = way.tags
            name = tags.get("name")
            
            return BeachData(
                id=f"osm_{way.id}",
                name=name,
                latitude=lat,
                longitude=lon,
                rating=None,
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
        """Validate beach data meets minimum requirements"""
        try:
            if not data.name:
                return False
                
            if data.name.startswith("Beach ") or data.name.lower() == "unnamed beach":
                return False
            
            if len(data.name) < 3:
                return False
                
            if data.name.isdigit():
                return False
                
            if not isinstance(data.latitude, (int, float)) or not isinstance(data.longitude, (int, float)):
                return False
                
            if not -90 <= data.latitude <= 90:
                return False
                
            if not -180 <= data.longitude <= 180:
                return False
                
            return True
            
        except Exception as e:
            self.logger.warning(f"Validation error: {str(e)}")
            return False

    def _generate_description(self, tags: Dict, beach_name: str) -> Optional[str]:
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