from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    training_file_path: str
    testing_file_path: str

@dataclass
class DataValidationArtifact:
    validation_status: bool
    validation_train_file_path: str
    validation_test_file_path: str
    invalid_train_file_path: str
    invalid_test_file_path: str
    drift_report_file_path: str