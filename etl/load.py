from sqlalchemy import create_engine
import pandas as pd
import logging
from pathlib import Path


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
TABLE_DATA_DIR = BASE_DIR / 'database' / 'tables_data'

def load_data():
    # Database connection
    try:
        db_connection = create_engine('postgresql+psycopg2://parsa:Parsa12345@localhost:5432/Flights_data')
        logger.info("Successfully connected to the database.")
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return # i didnot use raise beacause i wanna exit the function if connection fails

    
    # List of files to load with their corresponding table names
    files_to_load = [
        ('airlines.csv', 'airlines'),
        ('airports.csv', 'airports'),
        ('flights.csv', 'flights'),
        ('departure_detail.csv', 'departure_detail'),  # Note: Fixed typo from notebook
        ('arrival_detail.csv', 'arrival_detail')
    ]

    for file_name, table_name in files_to_load:
        file_path = TABLE_DATA_DIR / file_name
        try:
            # Read CSV file
            logger.info(f"Loading {file_name}...")
            df = pd.read_csv(file_path)
            
            # Load data into database
            df.to_sql(
                table_name,
                db_connection,
                if_exists="append",
                index=False
            )
            logger.info(f"Successfully loaded {file_name} into {table_name} table.")
        except Exception as e:
            logger.error(f"Error loading {file_name}: {e}")
    # closing database connection 
    db_connection.dispose()
    logger.info("Data loading process completed.")

if __name__ == "__main__":
    load_data()