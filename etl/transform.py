import pandas as pd
import numpy as np
from pyod.models.iforest import IForest
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
API_DATA = BASE_DIR / 'database' / 'api_data' / 'api_data.csv'
TRANSFORMED_DATA = BASE_DIR / 'database' / 'transformed_data' / 'transformed_data.csv'
TABLES_DATA_OUTPUT = BASE_DIR / 'database' / 'tables_data'
# making dir if doesnot exist
os.makedirs(TABLES_DATA_OUTPUT, exist_ok=True)

def load_data():
    # Load the raw data from CSV file.
    try:
        data = pd.read_csv(API_DATA)
        logger.info("Data successfuly loaded")
        return data
    except Exception as e:
        logger.error(f"Error:{str(e)}")


def drop_empty_columns(data):
    # Drop columns that are completely empty (100% null values).
    null = {col: int(data[col].isnull().sum()) for col in data.columns}
    columns_to_drop = [col for col in null if null[col] == 100]
    return data.drop(columns_to_drop, axis=1)


def handle_datetime_columns(data):
    # Convert datetime columns to proper datetime format.
    datetime_columns = [
        'departure_scheduled',
        'departure_estimated',
        'departure_actual',
        'departure_estimated_runway',
        'departure_actual_runway',
        'arrival_scheduled',
        'flight_date'
    ]
    data[datetime_columns] = data[datetime_columns].apply(pd.to_datetime, errors='coerce')
    return data


def calculate_flight_duration(data):
    # Calculate flight duration based on scheduled departure and arrival times.
    data['flight_duration'] = np.where(
        data['arrival_scheduled'] > data['departure_scheduled'],
        (data['arrival_scheduled'] - data['departure_scheduled']).dt.total_seconds()/3600,
        (data['departure_scheduled'] - data['arrival_scheduled']).dt.total_seconds()/3600
    )
    data['flight_duration'] = data['flight_duration'].fillna(data['flight_duration'].mean())
    return data


def fill_missing_values(data):
    # Fill flight status
    data['flight_status'] = data['flight_status'].fillna("unknown")
    
    # Fill terminal/gate information
    terminal_columns = ['departure_terminal', 'arrival_terminal', 'departure_gate', 'arrival_gate', 'arrival_baggage']
    data[terminal_columns] = data[terminal_columns].fillna("unknown")
    
    # Fill delays
    data['departure_delay'] = data['departure_delay'].fillna(0)
    
    # Handle departure actual times based on flight status
    data['departure_actual'] = np.where(
        data['flight_status'] == 'active',
        data['departure_actual'].fillna(data['departure_scheduled']),
        np.where(
            data['flight_status'] == 'scheduled',
            data['departure_actual'].fillna('null'),
            data['departure_actual']
        )
    )
    
    # Handle runway times
    data['departure_estimated_runway'] = np.where(
        data['flight_status'] == 'active',
        data['departure_estimated_runway'].fillna(data['departure_actual']),
        np.where(
            data['flight_status'] == 'scheduled',
            data['departure_estimated_runway'].fillna('null'),
            data['departure_estimated_runway']
        )
    )
    
    data['departure_actual_runway'] = np.where(
        data['flight_status'] == 'active',
        data['departure_actual_runway'].fillna(data['departure_actual']),
        np.where(
            data['flight_status'] == 'scheduled',
            data['departure_actual_runway'].fillna('null'),
            data['departure_actual_runway']
        )
    )
    
    # Fill codeshared columns
    codeshare_columns = [
        'flight_codeshared_airline_name',
        'flight_codeshared_airline_iata',
        'flight_codeshared_airline_icao',
        'flight_codeshared_flight_number',
        'flight_codeshared_flight_iata',
        'flight_codeshared_flight_icao'
    ]
    data[codeshare_columns] = data[codeshare_columns].fillna('N/A')
    
    # Fill arrival timezone
    data['arrival_timezone'] = data['arrival_timezone'].fillna(data['arrival_iata'])
    
    # Fill flight codes
    data['flight_number'] = data['flight_number'].fillna(0)
    data['flight_iata'] = data['flight_iata'].fillna(data['flight_icao'])
    data['flight_icao'] = data['flight_icao'].fillna(data['flight_iata'])
    
    if data['flight_iata'].isnull().sum() + data['flight_icao'].isnull().sum() != 0:
        data['flight_iata'] = data['flight_iata'].fillna(data['flight_number'])
        data['flight_icao'] = data['flight_icao'].fillna(data['flight_number'])
    
    # Fill remaining nulls
    data['arrival_airport'] = data['arrival_airport'].fillna(data['flight_number'])
    
    # Handle airline codes
    data['airline_iata'] = data['airline_iata'].fillna(data['airline_icao'])
    data['airline_icao'] = data['airline_icao'].fillna(data['airline_iata'])
    
    if data['airline_iata'].isnull().sum() + data['airline_icao'].isnull().sum() != 0:
        data['airline_iata'] = data['airline_iata'].fillna(data['airline_name'])
        data['airline_icao'] = data['airline_icao'].fillna(data['airline_name'])
    
    return data
    

def detect_outliers(data):
    # Delay outliers
    delay_model = IForest()
    delay_model.fit(data[['departure_delay']].values)
    data['delay_outlier'] = delay_model.predict(data[['departure_delay']].values).astype(bool)
    
    # Flight duration outliers
    duration_model = IForest()
    duration_model.fit(data[['flight_duration']].values)
    data['flight_duration_outlier'] = duration_model.predict(data[['flight_duration']].values).astype(bool)
    return data
    

def save_transformed_data(data):
    # Save the transformed data to CSV.
    try:
        data.to_csv(TRANSFORMED_DATA, encoding="utf-8", index=False)
        logger.info("Normalized data successfuly saved")
    except Exception as e:
        logger.error(f"Error in saving:{str(e)}")
        raise

def create_tables_data(data):
    # Airlines table
    try:
        airlines = data[['airline_name', 'airline_iata', 'airline_icao']].rename(
            columns={'airline_name': 'name', 'airline_iata': 'iata_code', 'airline_icao': 'icao_code'}
        )
        airlines.to_csv(TABLES_DATA_OUTPUT/'airlines.csv', encoding="utf-8", index=False)
        logger.info("Table data successfuly created")
    except Exception as e:
        logger.error(f"Error in saving:{str(e)}")
        raise
    # Airports table
    try:
        airports = data[['departure_airport', 'departure_iata', 'departure_icao', 'departure_timezone']].rename(
            columns={'departure_airport': 'name', 'departure_iata': 'iata_code', 
                     'departure_icao': 'icao_code', 'departure_timezone': 'timezone'}
        )
        airports.to_csv(TABLES_DATA_OUTPUT/'airports.csv', encoding="utf-8", index=False)
        logger.info("Table data successfuly created")
    except Exception as e:
        logger.error(f"Error in saving:{str(e)}")
        raise
    # Flights table
    try:
        flights = data[['flight_date', 'flight_status', 'flight_number', 'flight_iata', 
                        'flight_icao', 'flight_duration', 'delay_outlier', 'flight_duration_outlier']]
        flights.to_csv(TABLES_DATA_OUTPUT/'flights.csv', encoding="utf-8", index=False)
        logger.info("Table data successfuly created")
    except Exception as e:
        logger.error(f"Error in saving:{str(e)}")
        raise
    
    # Departure detail table
    try:
        departure_detail = data[['departure_gate', 'departure_terminal', 'departure_delay', 
                                 'departure_scheduled', 'departure_estimated', 'departure_actual',
                                 'departure_estimated_runway', 'departure_actual_runway']].rename(
            columns={
             'departure_gate': 'gate',
                'departure_terminal': 'terminal',
                'departure_delay': 'delay',
                'departure_scheduled': 'scheduled',
               'departure_estimated': 'estimated',
                'departure_actual': 'actual',
                'departure_estimated_runway': 'estimated_runway',
               'departure_actual_runway': 'actual_runway'
            }
        )
        departure_detail.to_csv(TABLES_DATA_OUTPUT/'departure_detail.csv', encoding="utf-8", index=False)
        logger.info("Table data successfuly created")
    except Exception as e:
        logger.error(f"Error in saving:{str(e)}")
        raise
    
    # Arrival detail table
    try:
        arrival_detail = data[['arrival_gate', 'arrival_terminal', 'arrival_baggage', 'arrival_scheduled']].rename(
            columns={
                'arrival_gate': 'gate',
                'arrival_terminal': 'terminal',
                'arrival_baggage': 'baggage',
                'arrival_scheduled': 'scheduled'
            }
        )
        arrival_detail.to_csv(TABLES_DATA_OUTPUT/'arrival_detail.csv', encoding="utf-8", index=False)
        logger.info("Table data successfuly created")
    except Exception as e:
        logger.error(f"Error in saving:{str(e)}")
        raise

def main():
    # Load data
    data = load_data()
    
    # Data transformation pipeline
    data = drop_empty_columns(data)
    data = handle_datetime_columns(data)
    data = calculate_flight_duration(data)
    data = fill_missing_values(data)
    data = detect_outliers(data)
    
    # Save results
    save_transformed_data(data)
    create_tables_data(data)
    
    print("Data transformation completed successfully!")

if __name__ == "__main__":
    main()
