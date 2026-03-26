from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging import logger
import os, sys
import pymongo 
import numpy as np, pandas as pd
from typing import List
from sklearn.model_selection import train_test_split
# Configuration 
from networksecurity.entity.config_entity import DataIngestionConfig
#Artifact Config
from networksecurity.entity.artifact_entity import DataIngestionArtifact

from dotenv import load_dotenv

load_dotenv()
MONGODB_URL: str = os.getenv("MONGO_DB_URL_KEY")

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
            
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    
    def export_collection_as_df(self):
        """
        Extracts data from a specified MongoDB collection and converts it into a pandas DataFrame.

        This method connects to the MongoDB database using the provided configuration,
        retrieves all documents from the specified collection, and converts them into
        a pandas DataFrame. It also performs basic preprocessing:
        
        - Drops the '_id' column if present.
        - Replaces string values 'na' with NumPy NaN values.

        Returns:
            pandas.DataFrame: A DataFrame containing the collection data after preprocessing.

        Raises:
            NetworkSecurityException: If any error occurs during the database connection,
                                      data retrieval, or transformation process.
        """
        try:
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            
            logger.info(f"Connecting to MongoDB | Database: '{database_name}' | Collection: '{collection_name}'")
            self.mongo_client = pymongo.MongoClient(MONGODB_URL)
            collection = self.mongo_client[database_name][collection_name]
            
            df = pd.DataFrame(list(collection.find()))
            logger.info(f"Fetched {len(df)} records from collection '{collection_name}'")
            
            if "_id" in df.columns.to_list():
                df = df.drop(columns=['_id'])  
                
            df.replace({'na':np.nan}, inplace=True)
            logger.info(f"Preprocessing done | Shape: {df.shape} | Missing values: {df.isna().sum().sum()}")
            
            return df
                        
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        finally:
            if hasattr(self, "mongo_client"):
                self.mongo_client.close()
       
    def export_data_into_feature_store(self, dataframe: pd.DataFrame):
        """
        Persists the given DataFrame as a CSV file in the feature store directory.

        Creates the target directory if it doesn't already exist, then saves the
        DataFrame to the configured feature store file path without the index column.

        Args:
            dataframe (pd.DataFrame): The preprocessed DataFrame to be saved.

        Returns:
            pandas.DataFrame: The same DataFrame passed in, returned for chaining.

        Raises:
            NetworkSecurityException: If any error occurs during directory creation or file export.
        """
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
        
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            logger.info(f"Feature store export complete | Path: '{feature_store_file_path}' | Shape: {dataframe.shape}")
            
            return dataframe
         
        except Exception as e:
            raise NetworkSecurityException(e,sys)     
    
    def split_data(self, dataframe: pd.DataFrame):
        """
        Splits the input DataFrame into training and testing datasets and saves them as CSV files.

        This method performs a train-test split using the ratio specified in the configuration,
        creates the required directory if it does not exist, and exports both datasets to their
        respective file paths.

        Args:
            dataframe (pd.DataFrame): The input dataset to be split.

        Returns:
            tuple: A tuple containing the training and testing DataFrames (train_set, test_set).

        Raises:
            NetworkSecurityException: If any error occurs during splitting or file operations.
        """
        try:
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logger.info(f"Train-test split completed | Train: {len(train_set)} rows | Test: {len(test_set)} rows | Ratio: {self.data_ingestion_config.train_test_split_ratio}")
            
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)
            logger.info("Exporting train and test data.")
            
            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True 
            )
            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )
            
            logger.info(f"Train data saved to: '{self.data_ingestion_config.training_file_path}'")
            logger.info(f"Test data saved to: '{self.data_ingestion_config.testing_file_path}'")
            
            return train_set, test_set
            
        except Exception as e:
            raise NetworkSecurityException(e, sys)
            
    def initiate_data_ingestion(self):
        """
        Orchestrates the full data ingestion pipeline.

        Sequentially executes the following steps:
        1. Fetches raw data from MongoDB and converts it to a DataFrame.
        2. Persists the DataFrame to the feature store as a CSV file.
        3. Splits the data into train and test sets and saves them.

        Returns:
            tuple: A tuple of (train_set, test_set) DataFrames.

        Raises:
            NetworkSecurityException: If any step in the pipeline fails.
        """
        try:
            logger.info("Starting data ingestion pipeline.")
            
            dataframe = self.export_collection_as_df()
            dataframe = self.export_data_into_feature_store(dataframe)
            train_set, test_set = self.split_data(dataframe)
            
            logger.info("Data ingestion pipeline completed successfully.")
            data_ingestion_artifact = DataIngestionArtifact(
                training_file_path=self.data_ingestion_config.training_file_path,
                testing_file_path=self.data_ingestion_config.testing_file_path,
            )
            return data_ingestion_artifact
            
        except Exception as e:
            raise NetworkSecurityException(e,sys)