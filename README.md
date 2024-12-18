# beach-database

beach_database/
│
├── src/
│   ├── collectors/
│   │   ├── __init__.py
│   │   ├── osm_collector.py      # OpenStreetMap data collection
│   │   ├── nominatim_collector.py # For reverse geocoding
│   │   └── base_collector.py     # Abstract base class for collectors
│   │
│   ├── processors/
│   │   ├── __init__.py
│   │   ├── data_cleaner.py      # Clean and standardize data
│   │   ├── geo_processor.py     # Handle geographic data
│   │   └── rating_processor.py  # Process and normalize ratings
│   │
│   ├── database/
│   │   ├── __init__.py
│   │   ├── firebase_manager.py  # Firebase operations
│   │   └── schema.py           # Database schema definitions
│   │
│   └── utils/
│       ├── __init__.py
│       ├── logger.py           # Logging configuration
│       └── config.py          # Configuration management
│
├── tests/
│   ├── test_collectors/
│   ├── test_processors/
│   └── test_database/
│
├── config/
│   ├── logging_config.yaml
│   └── app_config.yaml
│
└── scripts/
    ├── run_collection.py      # Main data collection script
    └── update_database.py     # Database update script



# Beach Data Collection Project

Collects and maintains a database of world beaches using OpenStreetMap data.

## Setup

1. Prerequisites:
   - Docker
   - Docker Compose
   - Firebase credentials

2. Configuration:
   - Place your Firebase credentials in `config/firebase_cred.json`
   - Update regions in `config/app_config.yaml` if needed

3. Running:
   ```bash
   docker-compose up --build