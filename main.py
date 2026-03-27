from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.logging import logger 
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig, DataValidationConfig
from networksecurity.components.data_validation import DataValidation


import sys

if __name__ == "__main__":
    try:
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config =  DataIngestionConfig(training_pipeline_config)
        data_validation_config = DataValidationConfig(training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config)
        logger.info(f"initiate the data ingestion pipeline.")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        print(data_ingestion_artifact)
        logger.info("Executing data validation")
        data_validation = DataValidation(data_ingestion_artifact, data_validation_config)
        data_validation_artifact = data_validation.initiate_data_validation()
        logger.info("Data validation completed.")
        print(data_validation_artifact)
        
    except Exception as e:
        raise NetworkSecurityException(e,sys)