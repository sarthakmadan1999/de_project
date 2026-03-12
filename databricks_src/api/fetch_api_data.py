import requests
import json
import os
import sys
from datetime import datetime
from src.utils.logger import get_logger
import yaml
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(BASE_DIR)

logger = get_logger(__name__, log_file="logs/fetch_api_data.log")

def load_config():
    with open(os.path.join(BASE_DIR, "config.yaml"), "r") as f:
        return yaml.safe_load(f)

def config_value():
    config_val = load_config()
    RAW_DATA_PATH = config_val['databricks']['raw_data_path']
    CONFIG_PATH = config_val['databricks']['config_path']
    url = config_val['shared']['api_url']
    return RAW_DATA_PATH, CONFIG_PATH, url


def fetch_aircraft_data(RAW_DATA_PATH, CONFIG_PATH, url):

    logger.info("Fetching aircraft data from OpenSky API...")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed: {str(e)}")
        sys.exit(1)

    data = response.json()

    # Extract only the states list
    states = data.get("states", [])
    record_count = len(states)
    logger.info(f"Total records fetched: {record_count}")

    # Validate record count before saving
    if record_count == 0:
        logger.error("No records fetched from API. Aborting.")
        sys.exit(1)
    if record_count < 1000:
        logger.error(f"Record count too low ({record_count}). Expected at least 1000. Aborting ingestion.")
        sys.exit(1)
    # Create raw folder if it doesn't exist
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    logger.info(f"Ensured raw data directory exists: {RAW_DATA_PATH}")

    # Generate timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save raw JSON file with timestamp
    file_name = f"aircraft_data_{timestamp}.json"
    file_path = os.path.join(RAW_DATA_PATH, file_name)

    with open(file_path, "w") as f:
        json.dump(states, f, indent=4)

    logger.info(f"Data saved successfully: {file_path}")

    # Save shared run config for Silver layer to consume
    config = {
        "run_timestamp": timestamp,
        "raw_file_path": file_path,
        "total_records": len(states)
    }

    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=4)

    logger.info(f"Run config saved: {CONFIG_PATH}")

    return file_path


if __name__ == "__main__":
    RAW_DATA_PATH, CONFIG_PATH, url = config_value()
    fetch_aircraft_data(RAW_DATA_PATH, CONFIG_PATH, url)