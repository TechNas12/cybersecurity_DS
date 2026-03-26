from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging import logger
import numpy as np
import pickle
import os,sys
import yaml
import dill

def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise NetworkSecurityException(e, sys) from e
 
