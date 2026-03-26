import os 
import sys
import json
import certifi
import pandas as pd, numpy as np,pymongo
import inspect

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logger

from dotenv import load_dotenv

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL_KEY")

ca = certifi.where()

class NetworkDataExtract:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def csv_to_json(self,file_path):
        """
    Convert a CSV file into a list of JSON records.

    This function reads a CSV file from the given file path,
    processes it into a pandas DataFrame, resets the index,
    and converts the data into a list of JSON-compatible
    dictionary records.

    Args:
        file_path (str): Path to the input CSV file.

    Returns:
        list[dict]: A list of dictionaries where each dictionary
        represents a row in the CSV file.

    Raises:
        NetworkSecurityException: If any error occurs during file
        reading, processing, or conversion.

    Logs:
        - Entry into the function
        - File read status
        - Index reset confirmation
        - Record creation confirmation
        - Return status

    Example:
        >>> records = obj.csv_to_json("data.csv")
        >>> print(records[0])
        {'column1': 'value1', 'column2': 'value2'}
    """
        try:
            func_name = inspect.currentframe().f_code.co_name
            logger.info(f"Enter the function: {func_name}")
            data = pd.read_csv(file_path,)
            logger.info(f"Read the file: {file_path}")
            data.reset_index(drop=True, inplace=True) 
            logger.info(f"Resseted the index of the dataframe.")
            records = list(json.loads(data.T.to_json()).values())
            logger.info(f"Created records for the data.")
            logger.info(f"Return the data")
            return records
             
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def insert_data_mongodb(self, records, database, collection):
        """
        Inserts multiple records into a specified MongoDB collection.

        Args:
            records (list[dict]): A list of documents to insert into the collection.
            database (str): The name of the MongoDB database to connect to.
            collection (str): The name of the collection within the database.

        Returns:
            int: The number of records successfully inserted.

        Raises:
            NetworkSecurityException: If any error occurs during the database 
            connection or insertion process.
        """
        try:
            logger.info(f"Connecting to MongoDB at database: '{database}', collection: '{collection}'")
            
            self.database = database
            self.collection = collection
            self.records = records
            
            
            self.mongo_client = pymongo.MongoClient(
                MONGO_DB_URL,
                tlsCAFile=certifi.where()
            )
            
            logger.info("MongoDB connection established successfully")
            
            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]
            
            logger.info(f"Attempting to insert {len(self.records)} record(s) into the collection")
            
            self.collection.insert_many(self.records)
            
            logger.info(f"Successfully inserted {len(self.records)} record(s)")
            return len(self.records)
            
        except Exception as e:
            logger.error(f"Failed to insert records into MongoDB: {e}")
            raise NetworkSecurityException(e,sys)
        
if __name__ == "__main__":
    FILE_PATH = r"Network_Data\phisingData.csv"
    DATABSE = "SANKET"
    COLLECTION = "NetworkData"
    
    network_obj = NetworkDataExtract()
    records = network_obj.csv_to_json(FILE_PATH)
    print(type(records))
    no_of_records = network_obj.insert_data_mongodb(records, DATABSE, COLLECTION)
    print(f"No of records: {no_of_records}")