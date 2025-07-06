import requests as req
import json
from pandas import json_normalize
import pandas as pd
import os
from pathlib import Path
import logging

# logging set
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extract.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# paths
BASE_DIR = Path(__file__).parent.parent
API_DATA_DIR = BASE_DIR / 'database' / 'api_data'
TRANSFORMED_DATA_DIR = BASE_DIR / 'database' / 'transformed_data'

# making dir if doesnot exist
os.makedirs(API_DATA_DIR, exist_ok=True)
os.makedirs(TRANSFORMED_DATA_DIR, exist_ok=True)

def fetch_flight_data(api_key: str) -> dict:
    url = "http://api.aviationstack.com/v1/flights"
    params = {"access_key": api_key}
    
    try:
        logger.info("Receiving data from API...")
        response = req.get(url, params=params)
        response.raise_for_status()  # Checking HTTP Errors
        data = response.json()
        logger.info("Data was successfully received from the API.")
        return data
    except req.exceptions.RequestException as e:
        logger.error(f"Error receiving data from API:{str(e)}")
        raise

def save_json_data(data: dict, filename: str) -> None:
    try:
        filepath = API_DATA_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f) # ensure_ascii=False, indent=4
        logger.info("Data saved successfully.")
    except (IOError, json.JSONEncodeError) as e:
        logger.error(f"Error saving json:{str(e)}")
        raise

def load_json_data(filename: str) -> dict:
    try:
        filepath = API_DATA_DIR / filename
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Data successfuly loaded")
        return data
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Error loading json: {str(e)}")
        raise

def normalize_and_save_data(data: dict, output_filename: str) -> None:
    try:
        # normalizing data
        df_normalized = json_normalize(data['data'], sep='_')
        
        # saving it as csv
        output_path = TRANSFORMED_DATA_DIR / output_filename
        df_normalized.to_csv(output_path, index=False, encoding='utf-8')
        logger.info("Normalized data successfuly saved")
    except Exception as e:
        logger.error(f"Error in normalization and saving:{str(e)}")
        raise

def main():
    try:
        API_KEY = "187b8cc2b421cb258090252787989971"
        
        flight_data = fetch_flight_data(API_KEY)

        save_json_data(flight_data, "api_data.json")
        
        loaded_data = load_json_data("api_data.json")
        
        normalize_and_save_data(loaded_data, "transformed_data.csv")
        
        logger.info("Extracting completed successfuly")
    except Exception as e:
        logger.error(f"Error in extracting: {str(e)}")
        raise

if __name__ == "__main__":
    main()