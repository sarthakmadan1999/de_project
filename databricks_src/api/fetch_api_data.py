import requests
import json
import os
import yaml
from datetime import datetime
from databricks_src.utils.logger import get_logger

# ---------------------------------------
# Config
# ---------------------------------------
WORKSPACE_PATH = os.environ.get("WORKSPACE_PATH")

def load_config():
    with open(f"{WORKSPACE_PATH}/config.yaml", "r") as f:
        return yaml.safe_load(f)

def config_value():
    config_val = load_config()
    RAW_DATA_PATH = config_val['databricks']['raw_data_path']
    CONFIG_PATH   = config_val['databricks']['config_path']
    LOG_PATH      = config_val['databricks']['bronze_log']
    url           = config_val['shared']['api_url']
    return RAW_DATA_PATH, CONFIG_PATH, LOG_PATH, url


# ---------------------------------------
# Fetch Aircraft Data
# ---------------------------------------
def fetch_aircraft_data(RAW_DATA_PATH, CONFIG_PATH, url, logger):

    logger.info("Fetching aircraft data from OpenSky API...")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed: {str(e)}")
        raise Exception(f"API call failed: {str(e)}")

    data   = response.json()
    states = data.get("states", [])
    logger.info(f"Total records fetched: {len(states)}")

    if len(states) == 0:
        logger.error("No records fetched from API. Aborting.")
        raise Exception("No records fetched from API. Aborting.")

    if len(states) < 1000:
        logger.error(f"Record count too low ({len(states)}). Aborting.")
        raise Exception(f"Record count too low ({len(states)}). Aborting.")

    # Generate timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f"aircraft_data_{timestamp}.json"
    file_path = f"{RAW_DATA_PATH}{file_name}"

    # Write JSON to ADLS
    dbutils.fs.put(file_path, json.dumps(states, indent=4), overwrite=True)
    logger.info(f"Data saved to ADLS: {file_path}")

    # Write run_config to ADLS
    run_config = {
        "run_timestamp": timestamp,
        "raw_file_path": file_path,
        "total_records": len(states)
    }
    dbutils.fs.put(CONFIG_PATH, json.dumps(run_config, indent=4), overwrite=True)
    logger.info(f"Run config saved: {CONFIG_PATH}")

    return file_path


# ---------------------------------------
# Main
# ---------------------------------------
def main():
    RAW_DATA_PATH, CONFIG_PATH, LOG_PATH, url = config_value()
    logger = get_logger(__name__, LOG_PATH)
    fetch_aircraft_data(RAW_DATA_PATH, CONFIG_PATH, url, logger)

main()