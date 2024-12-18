# src/processors/data_enrichment.py
import requests
import time
from typing import Dict, Optional, List
import logging
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from collectors.base_collector import BeachData

class DataEnrichmentService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.geolocator = Nominatim(user_agent="beach_data_collector")
        self.nominatim_delay = 1.0  # Respect Nominatim's usage policy
        self.last_nominatim_call = 0

    def enrich_beach_data(self, beach: BeachData) -> BeachData:
        """Enrich beach data with additional information from various sources"""
        try:
            # Add location details
            location_info = self._get_location_details(beach.latitude, beach.longitude)
            if location_info:
                beach.country = location_info.get('country')
                beach.region = location_info.get('state') or location_info.get('region')
                
            # Add local info and description
            wiki_info = self._get_wiki_info(beach.name, beach.latitude, beach.longitude)
            if wiki_info and not beach.description:
                beach.description = wiki_info
                
            # Add climate info
            climate_info = self._get_climate_info(beach.latitude, beach.longitude)
            if climate_info:
                beach.climate_info = climate_info
                
            # Add water quality if available
            water_quality = self._get_water_quality(beach.latitude, beach.longitude)
            if water_quality:
                beach.water_quality = water_quality
                
            return beach
            
        except Exception as e:
            self.logger.warning(f"Error enriching data for beach {beach.name}: {str(e)}")
            return beach

    def _get_location_details(self, lat: float, lon: float) -> Optional[Dict]:
        """Get detailed location information using Nominatim"""
        try:
            # Respect rate limiting
            current_time = time.time()
            time_since_last_call = current_time - self.last_nominatim_call
            if time_since_last_call < self.nominatim_delay:
                time.sleep(self.nominatim_delay - time_since_last_call)
            
            location = self.geolocator.reverse(f"{lat}, {lon}", exactly_one=True)
            self.last_nominatim_call = time.time()
            
            if location and location.raw.get('address'):
                return {
                    'country': location.raw['address'].get('country'),
                    'state': location.raw['address'].get('state'),
                    'region': location.raw['address'].get('region'),
                    'city': location.raw['address'].get('city'),
                    'suburb': location.raw['address'].get('suburb')
                }
            return None
            
        except GeocoderTimedOut:
            self.logger.warning(f"Timeout getting location details for {lat}, {lon}")
            return None
        except Exception as e:
            self.logger.warning(f"Error getting location details: {str(e)}")
            return None

    def _get_wiki_info(self, name: str, lat: float, lon: float) -> Optional[str]:
        """Get description from Wikipedia/Wikidata"""
        try:
            # Using Wikipedia's API to find nearby articles
            url = "https://en.wikipedia.org/w/api.php"
            params = {
                "action": "query",
                "format": "json",
                "list": "geosearch",
                "gscoord": f"{lat}|{lon}",
                "gsradius": 1000,
                "gslimit": 1
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if 'query' in data and 'geosearch' in data['query'] and data['query']['geosearch']:
                page_id = data['query']['geosearch'][0]['pageid']
                
                # Get page extract
                params = {
                    "action": "query",
                    "format": "json",
                    "prop": "extracts",
                    "exintro": True,
                    "explaintext": True,
                    "pageids": page_id
                }
                
                response = requests.get(url, params=params)
                data = response.json()
                
                if 'query' in data and 'pages' in data['query']:
                    page = data['query']['pages'][str(page_id)]
                    if 'extract' in page:
                        return page['extract']
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error getting wiki info: {str(e)}")
            return None

    def _get_climate_info(self, lat: float, lon: float) -> Optional[Dict]:
        """Get climate information from OpenWeather API"""
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {
                "lat": lat,
                "lon": lon,
                "appid": "YOUR_API_KEY"  # Would need to be configured
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if response.status_code == 200:
                return {
                    'temperature': data['main']['temp'],
                    'conditions': data['weather'][0]['description'],
                    'wind_speed': data['wind']['speed']
                }
            return None
            
        except Exception as e:
            self.logger.warning(f"Error getting climate info: {str(e)}")
            return None

    def _get_water_quality(self, lat: float, lon: float) -> Optional[str]:
        """Get water quality information if available"""
        # This would need to be implemented with a specific water quality API
        # Could use local government data where available
        return None