""" Model for aws account information """
from pydantic import ConfigDict
from pydantic.dataclasses import dataclass


@dataclass(config=ConfigDict(validate_assignment=True))
class AccountModel:
    """Class representing an AWS account"""
    account_id: str
    project_name: str
    project_environment: str
    bucket_name: str
    collection_interval: int
    metrics_collection_period: int = 1

    #TODO: se puede agregar exception to ValidationError?
