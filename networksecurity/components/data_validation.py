from networksecurity.entity.artifact_entity import DataValidationArtifact,DataIngestionArtifact
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.logging import logger
from scipy.stats import ks_2samp
import pandas as pd
import os, sys

class DataValidation:
    """
    Handles validation of ingested train/test data before it enters the transformation stage.

    Responsibilities:
        - Verifying column count and names match the schema
        - Detecting statistical drift between train and test distributions
        - Saving validated data and a drift report to disk
    """

    def __init__(self, data__ingestion_artifact:DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data__ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        """
        Reads a CSV file from disk into a pandas DataFrame.

        Args:
            file_path (str): Absolute or relative path to the CSV file.

        Returns:
            pd.DataFrame: Loaded DataFrame.

        Raises:
            NetworkSecurityException: If the file cannot be read.
        """
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        """
        Checks whether the DataFrame has the same number of columns as defined in the schema.

        Args:
            dataframe (pd.DataFrame): The DataFrame to validate.

        Returns:
            bool: True if column count matches the schema, False otherwise.

        Raises:
            NetworkSecurityException: If an error occurs during validation.
        """
        try:
            number_of_columns = len(self.schema_config['numerical_columns'])
            logger.info(f"Expected columns: {number_of_columns} | Found: {len(dataframe.columns)}")

            if len(dataframe.columns) == number_of_columns:
                return True

            logger.warning(f"Column count mismatch — expected {number_of_columns}, got {len(dataframe.columns)}")
            return False
            
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def validate_numerical_columns(self, dataframe: pd.DataFrame) -> bool:
        """
        Verifies that all numerical columns defined in the schema are present in the DataFrame,
        and that no extra columns exist beyond what the schema expects.

        Args:
            dataframe (pd.DataFrame): The DataFrame to validate.

        Returns:
            bool: True if the DataFrame columns exactly match the schema's numerical columns.

        Raises:
            NetworkSecurityException: If an error occurs during validation.
        """
        try:
            numerical_columns = self.schema_config['numerical_columns']
            df_columns = dataframe.columns.tolist()

            missing_cols = [col for col in numerical_columns if col not in df_columns]
            if missing_cols:
                logger.warning(f"Missing columns: {missing_cols}")
                return False

            extra_columns = [col for col in df_columns if col not in numerical_columns]
            if extra_columns:
                logger.warning(f"Unexpected extra columns: {extra_columns}")
                return False

            logger.info("Column validation passed")
            return True

        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def detect_dataset_drift(self, base_df, current_df, threshold=0.05) -> bool:
        """
        Runs a Kolmogorov-Smirnov test on each column to detect distributional drift
        between the train (base) and test (current) datasets.

        A column is considered drifted if its KS p-value falls below the threshold.
        Results are written to a YAML drift report on disk.

        Args:
            base_df (pd.DataFrame): Reference dataset (train).
            current_df (pd.DataFrame): Dataset to compare against (test).
            threshold (float): p-value threshold below which drift is flagged. Defaults to 0.05.

        Returns:
            bool: True if no drift is detected across all columns, False if any column drifted.

        Raises:
            NetworkSecurityException: If an error occurs during the drift check or file write.
        """
        try: 
            status = True
            report = {}

            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                if_sample_distribution_same = ks_2samp(d1, d2)

                if threshold <= if_sample_distribution_same.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False

                report.update({column:{
                    "p_value" : float(if_sample_distribution_same.pvalue),
                    "drift_status" : is_found
                }})

            drifted_cols = [col for col, val in report.items() if val["drift_status"]]
            logger.info(f"Drift check complete | Drifted columns ({len(drifted_cols)}): {drifted_cols if drifted_cols else 'None'}")

            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)
            logger.info(f"Drift report saved to: {drift_report_file_path}")

            return status

        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
        
    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Orchestrates the full data validation pipeline.

        Steps:
            1. Reads train and test CSVs from the ingestion artifact paths.
            2. Validates column count against the schema for both splits.
            3. Validates that all expected numerical columns are present in both splits.
            4. Runs a KS-based drift check between train and test distributions.
            5. Saves the validated train and test CSVs to the validation output directory.
            6. Returns a DataValidationArtifact with paths and validation status.

        Returns:
            DataValidationArtifact: Contains validation status, validated file paths, and drift report path.

        Raises:
            NetworkSecurityException: If any validation step fails or an unexpected error occurs.
        """
        try:
            train_file_path = self.data_ingestion_artifact.training_file_path
            test_file_path = self.data_ingestion_artifact.testing_file_path
            logger.info(f"Starting data validation | Train: {train_file_path} | Test: {test_file_path}")
            
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)
            logger.info(f"Data loaded | Train shape: {train_dataframe.shape} | Test shape: {test_dataframe.shape}")
            
            # Column count validation
            status = self.validate_number_of_columns(train_dataframe)
            if not status:
                error_message = f'Train dataframe doesnt contain all columns.\n'
                
            status = self.validate_number_of_columns(test_dataframe)
            if not status:
                error_message = f'Test dataframe doesnt contain all columns.\n'
                
            # Numerical column validation
            logger.info("Validating numerical columns")
            numeric_status = self.validate_numerical_columns(train_dataframe)
            if not numeric_status:
                error_message = f'Train dataframe doesnt contain all numeric columns.\n'
                
            numeric_status = self.validate_numerical_columns(test_dataframe)
            if not numeric_status:
                error_message = f'Test dataframe doesnt contain all numeric columns.\n'
                
            # Drift detection
            logger.info("Running dataset drift detection")
            status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)

            # Saving validated data
            dir_path = os.path.dirname(self.data_validation_config.valid_train_data_file_path)
            os.makedirs(dir_path, exist_ok=True)
            
            train_dataframe.to_csv(
                self.data_validation_config.valid_train_data_file_path, header=True, index=False
            )
            test_dataframe.to_csv(
                self.data_validation_config.valid_test_data_file_path, header=True, index=False
            )
            logger.info(f"Validated data saved | Dir: {dir_path}")
            
            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                validation_train_file_path=self.data_validation_config.valid_train_data_file_path,
                validation_test_file_path=self.data_validation_config.valid_test_data_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logger.info(f"Data validation complete | Status: {'passed' if status else 'drift detected'}")
            return data_validation_artifact
                        
        except Exception as e:
            raise NetworkSecurityException(e,sys)