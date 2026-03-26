from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.entity.artifact_entity import DataIngestionArtifact
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.utils.main_utils.utils import read_yaml_file
from networksecurity.logging import logger
from scipy.stats import ks_2samp
import pandas as pd
import os, sys

class DataValidation:
    def __init__(self, data__ingestion_artifact:DataIngestionArtifact):
        data_validation_config: DataValidationConfig
    
        try:
            self.data_ingestion_artifact = data__ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e,sys)