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