""" import traceback
from utils.exceptions import AWSAccountsLoadException
from scripts.logging_config import setup_logger

logger = setup_logger('handler_collect_metrics')

def handle_aws_account_load_error(error: Exception):
    if isinstance(error, AWSAccountsLoadException):
        logger.error(f"Error loading AWS account information: {error.message}\n{traceback.format_exc()}")

        
        
class AWSAccountsLoadException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = "Error loading AWS account information"        
        
        
"""