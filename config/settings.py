""" Module to load environment variables """
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings class to load environment variables"""
    model_config = SettingsConfigDict(env_prefix='')

    ACCOUNTS_LIST_PATH: str
    CLOUDWATCH_CONFIG_PATH: str
    AWS_REGION: str = 'us-east-1'
    ROLE_NAME: str = 'OrganizationAccountAccessRole'
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    ELASTICSEARCH_URL: str
    ELASTICSEARCH_USER: str = 'elastic'
    ELASTICSEARCH_PASSWORD: str
    MAX_THREADS: int = 5
    BUCKET_MONITORING_ACCOUNT: str
    #TODO: LOG_LEVEL: str = 'DEBUG'

settings = Settings()
