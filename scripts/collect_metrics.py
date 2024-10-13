""" Script to collect AWS metrics and save them to S3"""

import boto3
import time
import argparse
import polars as pl
import random
import string
#import pandas as pd
import tempfile
import sys
import traceback
from typing import List, Optional
from botocore.exceptions import ClientError
from pydantic import ValidationError
from datetime import datetime, timedelta, timezone
from models.account_info import AccountModel
from elasticsearch import Elasticsearch, helpers
from zoneinfo import ZoneInfo
from config.settings import settings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
import collections
import threading
import os
import logging
from logging_config import setup_logger
from models.time_interval import TimeIntervalModel
from utils.exceptions import (
    AwsExpiredTokenException,
    DateValidationException,
    ElasticSearchClientException,
    FileLoadException,
    AwsClientErrorException,
    AwsCollectMetricsException,
    PeriodException,
)

global logger
global config


def load_accounts_config(config_path: str) -> List[AccountModel]:
    """Loads AWS account configurations from a JSON file.

    Args:
        config_path (str): Path to the JSON file containing the AWS account configurations.

    Raises:
        FileLoadException: If there are errors in loading the accounts information.

    Returns:
        List[Account]: A list of Account objects containing the loaded AWS account information.
    """

    try:
        with open(config_path) as file:
            data = json.load(file)
        logger.debug("Account configuration loaded successfully.")
        accounts = [AccountModel(**account) for account in data["accounts"]]
        return accounts

    except FileNotFoundError as error:
        exception_info = {
            "message": f"File with AWS account information not found: {str(error)}",
            "detail": traceback.format_exc(),
        }
        raise FileLoadException(exception_info)

    except ValidationError as err:
        exception_info = {
            "message": f"Validation error for AWS account information: {str(err)}",
            "detail": traceback.format_exc(),
        }
        raise FileLoadException(exception_info)

    except json.JSONDecodeError as e:
        exception_info = {
            "message": f"Error decoding JSON AWS account information: {str(e)}",
            "detail": traceback.format_exc(),
        }
        raise FileLoadException(exception_info)

    except Exception as e:
        exception_info = {
            "message": f"Failed to load AWS account information: {str(e)}",
            "detail": traceback.format_exc(),
        }
        raise FileLoadException(exception_info)


def load_configurations(config_file: str) -> dict:
    """
    Loads the CloudWatch and agent configurations from the specified file.

    Args:
        config_file (str): The path to the file containing the CloudWatch and agent configurations.

    Returns:
        dict: A dictionary containing the configurations.

    Raises:
        FileLoadException: If there is an error loading the CloudWatch and agent configurations.
    """

    try:
        with open(config_file, "r") as file:
            configurations = json.load(file)
        return configurations

    except FileNotFoundError as error:
        exception_info = {
            "message": f"File containing CloudWatch and agent configurations not found: {str(error)}",
            "detail": traceback.format_exc(),
        }
        raise FileLoadException(exception_info)

    except ValidationError as err:
        exception_info = {
            "message": f"Validation error for cloudwatch and agent configurations: {str(err)}",
            "detail": traceback.format_exc(),
        }
        raise FileLoadException(exception_info)

    except json.JSONDecodeError as e:
        exception_info = {
            "message": f"Error decoding JSON for cloudwatch and agent configurations: {str(e)}",
            "detail": traceback.format_exc(),
        }
        raise FileLoadException(exception_info)

    except Exception as e:
        exception_info = {
            "message": f"Failed to load cloudwatch and agent configurations: {str(e)}",
            "detail": traceback.format_exc(),
        }
        raise FileLoadException(exception_info)


def parse_date_time(datetime_str: str) -> Optional[datetime]:
    """Parses a datetime string to a datetime object.

    Args:
        datetime_str (str): The datetime string to parse, in the format '%Y-%m-%dT%H:%M:%S'

    Returns:
        datetime: The parsed datetime object, or None if the input is empty or None
    """
    if datetime_str:
        return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")


def validate_dates(start_time: datetime, end_time: datetime) -> None:
    """Validates that the given datetimes are valid dates.

    Args:
        start_time (datetime): The start time to validate
        end_time (datetime): The end time to validate

    Raises:
        DateValidationException: If the given dates are invalid
    """
    now = datetime.now()

    if start_time >= now:
        raise DateValidationException(
            f"start_time {start_time} cannot be in the future. Current time: {now}"
        )

    if end_time > now:
        raise DateValidationException(
            f"end_time {end_time} cannot be in the future. Current time: {now}"
        )

    if end_time < start_time:
        raise DateValidationException(
            f"end_time {end_time} cannot be earlier than start_time {start_time}"
        )

    if start_time == end_time:
        raise DateValidationException(
            f"start_time {start_time} cannot be equal to end_time {end_time}"
        )


def initial_configurations() -> tuple[List[AccountModel], dict, argparse.Namespace]:
    accounts = load_accounts_config(settings.ACCOUNTS_LIST_PATH)

    if accounts is None:
        raise FileLoadException(
            "Accounts information is None, failed to load account configurations."
        )

    if len(accounts) == 0:
        raise FileLoadException(
            "Accounts list is empty, no accounts configurations found."
        )

    configurations = load_configurations(settings.CLOUDWATCH_CONFIG_PATH)

    if configurations is None:
        raise FileLoadException("Failed to load configurations.")

    env_vars = configurations.get("environment_variables", {})

    parser = argparse.ArgumentParser(
        description="Fetch and store AWS CloudWatch metrics."
    )

    for var_name, var_data in env_vars.items():
        parser.add_argument(
            f'--{var_data["args_variable"]}',
            default="" if var_data["default"] == "" else eval(var_data["default"]),
            type=eval(var_data["type"]),
            help=f'{var_name}: {var_data["help"]}',
        )

    args = parser.parse_args()

    return accounts, configurations, args


def create_es_client() -> Elasticsearch:
    """
    Creates an Elasticsearch client instance using the configured variables.

    Returns:
        Elasticsearch: The Elasticsearch client instance.

    Raises:
        ElasticSearchClientException: If there is an error creating the Elasticsearch client.
    """
    try:
        es = Elasticsearch(
            hosts=settings.ELASTICSEARCH_URL,
            basic_auth=(settings.ELASTICSEARCH_USER, settings.ELASTICSEARCH_PASSWORD),
        )
        logger.debug("Elasticsearch client created successfully.")
        return es
    except Exception as e:
        exception_info = {
            "message": f"Error creating Elasticsearch client: {str(e)}",
            "detail": traceback.format_exc(),
        }
        raise ElasticSearchClientException(exception_info)


def get_organization_session() -> boto3.Session:
    """
    Retrieves an AWS session for the organization account in the specified AWS region.

    Raises:
        AwsClientErrorException: If there is a problem with the AWS client.

    Returns:
        boto3.Session: The AWS session object for the principal account.
    """
    aws_access_key_id = settings.AWS_ACCESS_KEY_ID
    aws_access_secret_key = settings.AWS_SECRET_ACCESS_KEY

    if aws_access_key_id is None or aws_access_secret_key is None:
        raise AwsClientErrorException(
            "AWS access credentials not found in environment variables"
        )

    try:
        return boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_access_secret_key,
            region_name=settings.AWS_REGION,
        )

    except ClientError as error:
        exception_info = {
            "message": f"Failed to create session in principal account",
            "detail": f"{error.response['ResponseMetadata']['HTTPStatusCode']} {error.response['Error']['Code']}: {error.response['Error']['Message']}",
        }
        raise AwsClientErrorException(exception_info)


def assume_role(account_id: str, org_session: boto3.Session) -> boto3.Session:
    """
    Assume role in the AWS account and return a session.

    Args:
        account_id (str): The AWS account ID where the role is located.
        org_session (boto3.Session): The session object for the organization account.

    Returns:
        boto3.Session: A boto3 session object for the specified AWS account.

    Raises:
        AwsClientErrorException: If there is a problem with the AWS client.
    """
    role_arn = f"arn:aws:iam::{account_id}:role/{settings.ROLE_NAME}"

    try:
        sts_client = org_session.client("sts")
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName="AssumeRoleSession"
        )
        credentials = assumed_role_object["Credentials"]

        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=settings.AWS_REGION,
        )

    except sts_client.exceptions.RegionDisabledException as err:
        exception_info = {
            "message": f"The region {settings.AWS_REGION} is disabled for account {account_id}",
            "detail": f"{error.response['ResponseMetadata']['HTTPStatusCode']} {error.response['Error']['Code']}: {error.response['Error']['Message']}",
        }
        raise AwsClientErrorException(exception_info)

    except ClientError as error:
        exception_info = {
            "message": f"Failed to assume role in account {account_id}",
            "detail": f"{error.response['ResponseMetadata']['HTTPStatusCode']} {error.response['Error']['Code']}: {error.response['Error']['Message']}",
        }
        raise AwsClientErrorException(exception_info)

    except Exception as err:
        logger.error(f"Error assuming role in account {account_id}: {str(err)}")
        raise err


def create_aws_client(
    account_session: boto3.Session, service_name: str
) -> boto3.client:
    """
    Creates an AWS client for the specified service using the provided account session.

    Args:
        account_session (boto3.Session): The AWS account session to use for creating the client.
        service_name (str): The name of the AWS service for which to create the client.

    Returns:
        boto3.client: The created AWS client.

    Raises:
        AwsClientErrorException: If there is an error creating the client.
    """

    try:
        return account_session.client(service_name)

    except ClientError as error:
        exception_info = {
            "message": f"Failed to create client",
            "detail": f"{error.response['ResponseMetadata']['HTTPStatusCode']} {error.response['Error']['Code']}: {error.response['Error']['Message']}",
        }
        raise AwsClientErrorException(exception_info)


def calculate_period(duration: float) -> int:
    """
    Calculate the default period based on the duration.

    Args:
        start_time (datetime): The start time.
        end_time (datetime): The end time.

    Returns:
        int: The calculated period in seconds.

    Raises:
        PeriodException: If the end time is not greater than the start time.
    """

    if duration > config["cloudwatch_limits"]["455_DAYS"]:
        raise PeriodException("Metrics are available up to 455 days.")
    elif duration <= 0:
        raise PeriodException("End time must be greater than start time.")
    elif duration <= config["cloudwatch_limits"]["3_HOURS"]:
        return 60
    elif duration <= config["cloudwatch_limits"]["15_DAYS"]:
        return 60
    elif duration <= config["cloudwatch_limits"]["63_DAYS"]:
        return 300
    else:
        return 3600


def validate_metrics_collection_period(
    period: int, start_time: datetime, end_time: datetime
) -> int:
    """
    Validate the metrics collection period.

    Args:
        period (int): The period to validate.
        start_time (datetime): The start time.
        end_time (datetime): The end time.

    Returns:
        int: The validated period.
    """

    duration = (end_time - start_time).total_seconds()
    period_seconds = period * 60
    default_period = calculate_period(duration)

    times_period = period_seconds // default_period

    if times_period <= 1:
        return default_period
    return times_period * default_period


def split_time_range(
    current_start: datetime, end_time: datetime, initial_period: int
) -> List[TimeIntervalModel]:
    """
    Split a time range into smaller intervals based on the given period.

    Args:
        current_start (datetime): The start time.
        end_time (datetime): The end time.
        initial_period (int): The period in seconds.

    Returns:
        List[DateRange]: A list of date ranges.
    """
    intervals: List[TimeIntervalModel] = []

    while current_start < end_time:
        period = validate_metrics_collection_period(
            initial_period, current_start, end_time
        )
        max_duration = config["cloudwatch_limits"]["MAX_DATAPOINTS"] * period
        current_end = current_start + timedelta(seconds=max_duration)

        if current_end > end_time:
            current_end = end_time

        interval = TimeIntervalModel(current_start, current_end, period)
        intervals.append(interval)

        current_start = current_end

    return intervals


def get_all_cloudwatch_metrics(cloudwatch_client: boto3.client) -> List:
    """Get all CloudWatch metrics for a given client.

    Args:
        cloudwatch_client (boto3.client): The CloudWatch client to use to retrieve the metrics.

    Returns:
        list: A list of CloudWatch metrics.

    Raises:
        AwsCollectMetricsException: If there is an error while retrieving the metrics.
        InternalServiceFault: If there is an internal service fault while retrieving from.
        InvalidParameterValue: If an invalid parameter value is encountered while retrieving from.
    """

    metrics_list: list = []

    try:
        paginator = cloudwatch_client.get_paginator("list_metrics")
        for page in paginator.paginate():
            metrics_list.extend(page["Metrics"])
        return metrics_list

    except cloudwatch_client.exceptions.InternalServiceFault as error:
        exception_info = {
            "message": f"Couldn't get metrics for cloudwatch client due to an internal service fault.",
            "detail": f"{error.response['ResponseMetadata']['HTTPStatusCode']} {error.response['Error']['Code']}: {error.response['Error']['Message']}",
        }
        raise AwsCollectMetricsException(exception_info)

    except cloudwatch_client.exceptions.InvalidParameterValueException as error:
        exception_info = {
            "message": f"Couldn't get metrics for cloudwatch client due to an invalid parameter value.",
            "detail": f"{error.response['ResponseMetadata']['HTTPStatusCode']} {error.response['Error']['Code']}: {error.response['Error']['Message']}",
        }
        raise AwsCollectMetricsException(exception_info)

    except ClientError as e:
        exception_info = {
            "message": f"Client error fetching all cloudwatch metrics.",
            "detail": f"{error.response['ResponseMetadata']['HTTPStatusCode']} {error.response['Error']['Code']}: {error.response['Error']['Message']}",
        }
        raise AwsCollectMetricsException(exception_info)

    except Exception as e:
        exception_info = {
            "message": f"Unexpected error fetching all cloudwatch metrics.",
            "detail": f"{error.response['ResponseMetadata']['HTTPStatusCode']} {error.response['Error']['Code']}: {error.response['Error']['Message']}",
        }
        raise AwsCollectMetricsException(exception_info)


def process_interval(
    organization_session: boto3.Session,
    cloudwatch_client: boto3.client,
    es_client: Elasticsearch,
    metrics: List,
    account: AccountModel,
    interval: TimeIntervalModel
):
    logger.debug(f'Starting to fetch metrics for interval {interval}')
    no_datapoints_counter = collections.defaultdict(int)
    counter_lock = threading.Lock()
    total_no_datapoints = 0
    
    consolidated_metrics = []
    consolidate_es_documents = []
    try:
        for metric in metrics:
            
            response = get_metrics_data(cloudwatch_client, metric, interval)
            
                
            if not response:
                logger.warning(
                    f"No response found for namespace {metric['Namespace']} and metric '{metric['MetricName']}' in account {account.account_id}"
                )

            response["Dimensions"] = metric.get("Dimensions", [])
            response["Namespace"] = metric["Namespace"][4:]
            response["Project"] = account.project_name
            response["Environment"] = account.project_environment

            consolidated_metrics.append(response)

            datapoints = response.get("Datapoints", [])

            if not datapoints or len(datapoints) == 0:
                with counter_lock:
                    no_datapoints_counter[metric['Namespace']] += 1
                    total_no_datapoints += 1

                
                #logger.info(
                #    f"No datapoints found for namespace {metric['Namespace']} and metric '{metric['MetricName']}' in account {account.account_id}"
                #)
            if datapoints:

                for datapoint in datapoints:

                    timestamp_utc = datapoint.get("Timestamp")
                    if timestamp_utc and (
                        timestamp_utc.tzinfo is None
                        or timestamp_utc.tzinfo.utcoffset(timestamp_utc) is None
                    ):
                        timestamp_utc = timestamp_utc.replace(tzinfo=timezone.utc)
                    datapoint["Timestamp"] = timestamp_utc.isoformat()

                    document = create_document(response, datapoint, account.account_id)
                    consolidate_es_documents.append(document)
        
        try:
            account_session = assume_role(settings.BUCKET_MONITORING_ACCOUNT, organization_session)
            s3_client = create_aws_client(account_session, "s3")
        except Exception as e:
            logger.error(f"Error creating S3 client: {e}")
            
        save_metrics_to_s3(s3_client, consolidated_metrics, account, interval.start_date)
        logger.debug(f"Saving {len(consolidated_metrics)} metrics to S3")

        index_to_elasticsearch(es_client, consolidate_es_documents, account.account_id, interval)
        
        logger.info(f"------ Metrics Summary without Datapoints for account {account.account_id} ------")
        logger.info(f"Total metrics without datapoints: {total_no_datapoints}")
        for namespace, count in no_datapoints_counter.items():
            logger.info(f"{namespace}: {count} metrics")


    except AwsExpiredTokenException as error:
        logger.warning("Reinitiating session due to expired token")
        organization_session = get_organization_session()
        account_session = assume_role(account.account_id, organization_session)
        cloudwatch_client = create_aws_client(account_session, "cloudwatch")
        
        for metric in metrics:
            
            response = get_metrics_data(cloudwatch_client, metric, interval)
            
                
            if not response:
                logger.warning(
                    f"No response found for namespace {metric['Namespace']} and metric '{metric['MetricName']}' in account {account.account_id}"
                )

            response["Dimensions"] = metric.get("Dimensions", [])
            response["Namespace"] = metric["Namespace"][4:]
            response["Project"] = account.project_name
            response["Environment"] = account.project_environment

            consolidated_metrics.append(response)

            datapoints = response.get("Datapoints", [])

            #TODO: agregar validación si el responsemedatada es =! 200
            """ {
                "Label":"Aurora_pq_request_not_chosen_below_min_rows",
                "Datapoints":[],
                "ResponseMetadata":{
                    "RequestId":"67s6df78-8cb2cd1bdf7a",
                    "HTTPStatusCode":200,
                    "HTTPHeaders":{
                        "x-amzn-requestid":"67s6df78-8cb2cd1bdf7a",
                        "content-type":"text/xml",
                        "content-length":"366",
                        "date":"Sun, 29 Sep 2024 04:27:01 GMT",
                        "connection":null
                    },
                    "RetryAttempts":0
                },
                "Dimensions":[
                    {
                        "Name":"DatabaseClass",
                        "Value":"db.r6g.2xlarge"
                    }
                ],
                "Namespace":"RDS",
                "Project":"ESC",
                "Environment":"production"
            } """

            if not datapoints or len(datapoints) == 0:
                with counter_lock:
                    no_datapoints_counter[metric['Namespace']] += 1
                #logger.info(
                #    f"No datapoints found for namespace {metric['Namespace']} and metric '{metric['MetricName']}' in account {account.account_id}"
                #)
            if datapoints:

                for datapoint in datapoints:

                    timestamp_utc = datapoint.get("Timestamp")
                    if timestamp_utc and (
                        timestamp_utc.tzinfo is None
                        or timestamp_utc.tzinfo.utcoffset(timestamp_utc) is None
                    ):
                        timestamp_utc = timestamp_utc.replace(tzinfo=timezone.utc)
                    datapoint["Timestamp"] = timestamp_utc.isoformat()

                    document = create_document(response, datapoint, account.account_id)
                    consolidate_es_documents.append(document)
        
        try:
            account_session = assume_role(settings.BUCKET_MONITORING_ACCOUNT, organization_session)
            s3_client = create_aws_client(account_session, "s3")
        except Exception as e:
            logger.error(f"Error creating S3 client: {e}")
            
        save_metrics_to_s3(s3_client, consolidated_metrics, account, interval.start_date)
        logger.debug(f"Saving {len(consolidated_metrics)} metrics to S3")

        index_to_elasticsearch(es_client, consolidate_es_documents, account.account_id, interval)
        
    
    """ try:
        es.index(
            index=f"metrics-test-aws-{account.account_id}", document=document
        )
    # TODO: crear exception por error en indexación de datos
    except Exception as error:
        logger.error(
            f"Error processing data. No se pudo cargar account {account.account_id}: Namespace {metric['Namespace']} - metric {metric['MetricName']}"
        )
        logger.error(error) """

    return consolidated_metrics, consolidate_es_documents



def index_to_elasticsearch(es_client: Elasticsearch, documents, account_id: str, interval: TimeIntervalModel):
    index_name = f"metrics-testing-{account_id}"
    if not documents:
        logger.info(f"No documents to index for account {account_id} in interval {interval.start_date} - {interval.end_date}.")
        return
    
    actions = [
        {
            "_op_type": "create",
            "_index": index_name,
            "_source": document
        }
        for document in documents
    ]
    
    failed_documents = []
    failed_documents_details = []
    
    try:
        success, failed = helpers.bulk(es_client, actions, raise_on_error=False, stats_only=False, chunk_size=5000,max_retries=3)
        logger.debug(f"Successfully indexed {success} documents for account {account_id} in interval {interval.start_date} - {interval.end_date}.")
        
        for error in failed:
            action = error['index']
            status = error['status']
            error_reason = error['error']['reason']
            failed_doc = action.get('_source')
            
            if failed_doc:
                failed_documents.append(failed_doc)
                failed_documents_details.append({
                    "document": failed_doc,
                    "status": status,
                    "error": error_reason
                })
        
        if failed_documents:
            logger.warning(f"{len(failed_documents)} documents failed to index for account {account_id} in interval {interval.start_date} - {interval.end_date}. Retrying...")
            
            # Guardar detalles de los documentos fallidos
            error_log_path = os.path.join(tempfile.gettempdir(), f"failed_documents_{account_id}_{interval.start_date.strftime('%Y%m%d_%H%M%S')}.json")
            with open(error_log_path, 'w') as f:
                json.dump(failed_documents_details, f, indent=4)
            logger.info(f"Failed documents details saved at {error_log_path}.")
            
            # Reintentar indexación
            retry_actions = [
                {
                    "_op_type": "create",
                    "_index": index_name,
                    "_source": doc
                }
                for doc in failed_documents
            ]
            
            retry_failed_documents = []
            retry_failed_documents_details = []
            
            retry_success, retry_failures = helpers.bulk(es_client, retry_actions, raise_on_error=False, stats_only=False, chunk_size=5000, max_retries=3)
            logger.info(f"Successfully retried and indexed {retry_success} documents for account {account_id} in interval {interval.start_date} - {interval.end_date}.")
            
            for error in retry_failures:
                action = error['index']
                status = error['status']
                error_reason = error['error']['reason']
                failed_doc = action.get('_source')
                
                if failed_doc:
                    retry_failed_documents.append(failed_doc)
                    retry_failed_documents_details.append({
                        "document": failed_doc,
                        "status": status,
                        "error": error_reason
                    })
            
            if retry_failed_documents:
                logger.error(f"{len(retry_failed_documents)} documents failed to index even after retry for account {account_id} in interval {interval.start_date} - {interval.end_date}.")
                # Guardar detalles de los documentos que fallaron después del reintento
                final_error_log_path = os.path.join(tempfile.gettempdir(), f"final_failed_documents_{account_id}_{interval.start_date.strftime('%Y%m%d_%H%M%S')}.json")
                with open(final_error_log_path, 'w') as f:
                    json.dump(retry_failed_documents_details, f, indent=4)
                logger.info(f"Final failed documents details saved at {final_error_log_path}.")
    
    except Exception as e:
        logger.error(f"Error during bulk indexing for account {account_id} in interval {interval.start_date} - {interval.end_date}: {e}")




def calculate_time_interval(
    start_time: datetime, end_time: datetime, period: int
) -> datetime:
    """Calculate the time interval between two given timestamps.

    Args:
        start_time (datetime): The starting timestamp.
        end_time (datetime): The ending timestamp.
        period (int): The period used to calculate the interval.

    Returns:
        datetime: The calculated time interval between the start and end timestamps.
    """

    calculate_date: float = (
        start_time.timestamp() + config["cloudwatch_limits"]["MAX_DATAPOINTS"] * period
    )
    current_end_time: datetime = datetime.fromtimestamp(calculate_date)

    return min(current_end_time, end_time)


def get_metricas_data_test(client: boto3.client, metric_data_queries, start_time, end_time):
    try:
        response = client.get_metric_data(
            MetricDataQueries=metric_data_queries,
            StartTime=start_time,
            EndTime=end_time,
            ScanBy="TimestampDescending",
        )
        print(response)

        return response["MetricDataResults"]
    except ClientError as e:
        logging.error(f"Error fetching metric data: {e}")
        raise


def get_metrics_data(
    cloudwatch_client: boto3.client,
    metric: dict,
    interval: TimeIntervalModel,
) -> dict:
    """Retrieve metric data from CloudWatch.

    Args:
        cloudwatch_client (boto3.client): The CloudWatch client to use.
        metric (dict): The metric to retrieve data for.
        start_time (datetime): The start time to retrieve data from.
        end_time (datetime): The end time to retrieve data until.
        period (int): The period to retrieve data in seconds.
        project (str): The name of the project.
        environment (str): The name of the environment.

    Raises:
        - AttributeError: If there is an error retrieving the metric data due attribute error.
        - Exception: If there is an error retrieving the metric data.

    Returns:
        dict: The retrieved metric data.
    """
    if interval is None:
        raise AwsCollectMetricsException(
            f'Time interval not defined for namespace {metric["Namespace"]} - metric {metric["MetricName"]}'
        )

    try:
        return cloudwatch_client.get_metric_statistics(
            Namespace=metric["Namespace"],
            MetricName=metric["MetricName"],
            Dimensions=metric["Dimensions"],
            Statistics=config["statistics"],
            StartTime=interval.start_date,
            EndTime=interval.end_date,
            Period=interval.period,
        )

    except AttributeError as error:
        logger.error("An attribute error occurred while get metric data: %s", error)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ExpiredToken':
            logger.warning(f" {e.response['Error']['Code']}: Session has expired get metric data")
            raise AwsExpiredTokenException("Session has expired")
        raise AwsClientErrorException("Failed to get metric data: %s" % e)
    except Exception as error:
        logger.error(
            "Couldn't get metric data %s of namespace %s: %s",
            metric["MetricName"],
            metric["Namespace"],
            error,
        )


# bucket_name = 'monitoring-metrics-bucket'


def create_document(response: dict, datapoint, account_id: str):
    """
    Crear un documento formateado para Elasticsearch a partir de un datapoint.

    Args:
        datapoint (dict): Datapoint de CloudWatch con estadísticas.
        namespace (str): Namespace de la métrica.
        dimensions (list): Lista de dimensiones de la métrica.
        project (str): Nombre del proyecto.
        environment (str): Entorno del proyecto.
        account_id (str): ID de la cuenta de AWS.

    Returns:
        dict: Documento formateado para Elasticsearch.
    """
    service_name = response["Namespace"].lower()
    data = {
        "cloud": {
            "provider": "AWS",
            "region": "us-east-1",
            "account": {
                "name": response["Project"],
                "id": account_id,
                "environment": response["Environment"],
            },
        },
        "@timestamp": datapoint["Timestamp"],
        "aws": {
            "cloudwatch": {"namespace": response["Namespace"]},
            "dimensions": { dim["Name"]: dim["Value"] for dim in response["Dimensions"] },
            service_name: {
                "metrics": {
                    response["Label"]: {
                        "avg": datapoint.get("Average"),
                        "sum": datapoint.get("Sum"),
                        "max": datapoint.get("Maximum"),
                        "min": datapoint.get("Minimum"),
                        "unit": datapoint.get("Unit"),
                    }
                }
            },
        },
    }

    document = json.dumps(data)
    return document


def save_metrics_to_s3(
    s3_client: boto3.client, metrics: List, account: AccountModel, timestamp: datetime
) -> None:

    try:
        timestamp_str = timestamp.strftime(
            "%Y%m%d_%H%M%S"
        )
        random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=settings.random_s3_name_length))
        s3_key = f"{account.project_environment}_{timestamp_str}_{random_str}.parquet.gzip"
        s3_filename = f"metrics/{account.project_environment}_{timestamp_str}_{random_str}.parquet.gzip"

        df = pl.DataFrame(metrics)
        df.write_parquet(s3_key, compression="gzip")
        s3_client.upload_file(s3_key, account.bucket_name, s3_filename)
        logger.debug(f"Saving metrics to S3 at '{s3_key}' for account {account.account_id} ...")

    except Exception as e:
        logger.error(f"Error saving metrics to S3 for account {account.account_id}: {e}")
        #TODO: exception nombre repetido de archivo
        raise


def validate_metrics_collection_periods(
    metrics_collection_period, start_time, end_time
):

    allowed_periods = [1, 5, 10, 30, 60]
    # Retention periods in seconds
    retention_mapping = {
        1: 15 * 24 * 3600,  # 15 days
        5: 63 * 24 * 3600,  # 63 days
        10: 63 * 24 * 3600,  # 63 days
        30: 63 * 24 * 3600,  # 63 days
        60: 455 * 24 * 3600,  # 455 days (15 months)
    }

    now = datetime.now()
    duration_seconds = (end_time - start_time).total_seconds()
    print(
        f"Duration between start_time and end_time: {duration_seconds} seconds"
    )  # Debug

    if metrics_collection_period is not None:
        print(
            f"Validating provided metrics_collection_period: {metrics_collection_period} minutes"
        )  # Debug
        if (
            metrics_collection_period in allowed_periods
            or metrics_collection_period % 60 == 0
        ):
            if metrics_collection_period in retention_mapping:
                retention = retention_mapping[metrics_collection_period]
                print(
                    f"Retention period for {metrics_collection_period} minutes: {retention} seconds"
                )  # Debug
            elif metrics_collection_period % 60 == 0:
                # For multiples of 60 not explicitly defined, use the retention of 60 minutes
                retention = retention_mapping[60]
                print(
                    f"Retention period for {metrics_collection_period} minutes (multiple of 60): {retention} seconds"
                )  # Debug
            else:
                # This case should not occur due to the earlier check
                retention = retention_mapping[60]
                print(
                    f"Defaulting retention period to 60 minutes: {retention} seconds"
                )  # Debug

            if duration_seconds > retention:
                raise ValueError(
                    f"The specified metrics_collection_period of {metrics_collection_period} minutes "
                    f"exceeds the CloudWatch retention period of {retention / (24 * 3600)} days for this granularity."
                )
            else:
                print(
                    f"metrics_collection_period of {metrics_collection_period} minutes is valid."
                )  # Debug
                return metrics_collection_period
        else:
            raise ValueError(
                f"Invalid metrics_collection_period: {metrics_collection_period}. "
                f"It must be one of {allowed_periods} or a multiple of 60."
            )
    else:
        # Calculate automatically based on duration
        print(
            "Calculating metrics_collection_period automatically based on duration."
        )  # Debug
        sorted_periods = sorted(allowed_periods)
        for period in sorted_periods:
            retention = retention_mapping.get(period, retention_mapping[60])
            print(
                f"Checking if duration <= retention for {period} minutes: {duration_seconds} <= {retention}"
            )  # Debug
            if duration_seconds <= retention:
                print(f"Selected metrics_collection_period: {period} minutes")  # Debug
                return period
        # If no suitable period is found, default to 60 minutes
        print(
            "No suitable metrics_collection_period found within retention limits. Defaulting to 60 minutes."
        )  # Debug
        return 60


def collect_metrics_test(
    accounts: List[AccountModel],
    org_session: boto3.Session,
    es_client: Elasticsearch,
    start_time: datetime,
    end_date: datetime,
) -> None:

    all_consolidated_metrics = []
    all_es_documents = []
    """ for account in accounts:
        account_s3 = '838477461307'
        account_session = assume_role(account_s3, org_session)
        s3_client = create_aws_client(account_session, "s3")
        save_metrics_to_s3(s3_client, consolidated_metrics, account, end_time) """

    for account in accounts:
        logger.debug(f"Starting to collect metrics for account {account.project_name}.")
        account_id = account.account_id
        start_date = (
            (end_date - timedelta(minutes=account.collection_interval))
            if start_time == None
            else start_time
        )

        try:
            validate_dates(start_date, end_date)
            account_session = assume_role(account_id, org_session)
            cloudwatch_client = create_aws_client(account_session, "cloudwatch")
        except DateValidationException as error:
            logger.error(error)
        except AwsClientErrorException as error:
            logger.error(error)

        try:
            intervals = split_time_range(
                start_date, end_date, account.metrics_collection_period
            )
            logger.debug(f"Total intervals to process: {len(intervals)}")

            metrics = get_all_cloudwatch_metrics(cloudwatch_client)
            logger.debug(f"Found {len(metrics)} metrics")

            with ThreadPoolExecutor(max_workers=settings.MAX_THREADS) as executor:
                futures = [
                        executor.submit(
                            process_interval,
                            org_session,
                            cloudwatch_client,
                            es_client,
                            metrics,
                            account,
                            interval
                        )
                    for interval in intervals
                ]

                for future in as_completed(futures):
                    try:
                        consolidated_metrics, es_documents = future.result()
                        all_consolidated_metrics.extend(consolidated_metrics)
                    
                        if es_documents and len(es_documents) > 0:
                            all_es_documents.extend(es_documents)
                        else:
                            logger.info(f"No ES documents found for account {account_id} in this interval")
                    #TODO: Add exception handling   
                    except Exception as e:
                        logger.error(f"Error processing an interval for account {account_id}: {e}")
        
            if all_consolidated_metrics and len(all_consolidated_metrics) > 0:
                try:
                    account_session = assume_role(settings.BUCKET_MONITORING_ACCOUNT, org_session)
                    s3_client = create_aws_client(account_session, "s3")
                    save_metrics_to_s3(s3_client, all_consolidated_metrics, account, start_date)
                    logger.debug(f"Saving {len(all_consolidated_metrics)} metrics to S3")

                except AwsClientErrorException as error:
                    logger.error(error)

            index_name = f"metrics-testing-{account_id}"
            #if all_es_documents and len(all_es_documents) > 0:

            actions = [
                {
                    "_op_type": "create",
                    "_index": index_name,
                    "_source": document
                }
                for document in all_es_documents
            ]

            try:
                success, failed = helpers.bulk(es_client, actions, raise_on_error=False)

                print(f"Documentos indexados con éxito: {success}")
                print(f"Documentos que fallaron al indexar: {len(failed)}")

                # Imprimir los errores de los documentos que fallaron
                for error in failed:
                    print(f"Error: {error}")
            except Exception as e:
                print(f"Error al indexar documentos: {e}")
                #logger.info(f"Saved {len(all_es_documents)} documents for account {account_id}")
                #except Exception as e:
                #    logger.error(f"Error saving documents to ES: {e}")

        except PeriodException as error:
            logger.error(error)
        except AwsCollectMetricsException as error:
            logger.error(error)

        """ if consolidated_metrics and len(consolidated_metrics) > 0:
            account_s3 = "838477461307"
            account_session = assume_role(account_s3, org_session)
            s3_client = create_aws_client(account_session, "s3")
            save_metrics_to_s3(s3_client, consolidated_metrics, account, start_time) """

    """ if metric_data is None or len(metric_data) == 0 :
        raise AwsCollectMetricsException(
            f"No metrics found for account: {account_id}"
        ) """

    # for service_name in ['ecs', 's3', 'rds']:
    # print(service_name)

    # metrics_data = fetch_metrics(account_id, service_name)
    # save_metrics_to_s3(metrics_data, account_id, service_name)



def collect_metrics(
    accounts: List[AccountModel],
    org_session: boto3.Session,
    es_client: Elasticsearch,
    start_time: datetime,
    end_date: datetime,
) -> None:

    with ThreadPoolExecutor(max_workers=settings.MAX_THREADS) as executor:
        futures = []

        for account in accounts:
            account_id = account.account_id
            start_date = (
                (end_date - timedelta(minutes=account.collection_interval))
                if start_time == None
                else start_time
            )

            try:
                validate_dates(start_date, end_date)
                #account_session = assume_role(account_id, org_session)
                #cloudwatch_client = create_aws_client(account_session, "cloudwatch")
                intervals = split_time_range(
                start_date, end_date, account.metrics_collection_period
                )

                print('Hola')
                for i in intervals:
                    print(i)
                break
                
                metrics = get_all_cloudwatch_metrics(cloudwatch_client)
                logger.debug(f"Account {account_id} has {len(intervals)} intervals and {len(metrics)} metrics found")

            except DateValidationException as error:
                logger.error(error)
            except AwsClientErrorException as error:
                logger.error(error)
            except PeriodException as error:
                logger.error(error)
            except AwsCollectMetricsException as error:
                logger.error(error)
            

            for interval in intervals:
                futures.append(
                    executor.submit(
                        process_interval,
                        org_session,
                        cloudwatch_client,
                        es_client,
                        metrics,
                        account,
                        interval
                    )
                )

        for future in as_completed(futures):
            try:
                future.result()
            except ClientError as e:
                if e.response['Error']['Code'] == 'ExpiredToken':
                    logger.warning("Token expired. Re-authenticating.", e)
                #logging.warning("Token expirado durante el procesamiento. Reintentando con una nueva sesión.")
                # Implementar lógica para reintentar con una nueva sesión si es necesario
                # Esto podría incluir re-asumir el rol y volver a procesar el intervalo
            except Exception as e:
                logger.error(f"Error processing a future: {e}")


def collect_metrics_old(
    accounts: List[AccountModel],
    org_session: boto3.Session,
    es_client: Elasticsearch,
    start_time: datetime,
    end_date: datetime,
) -> None:
        all_consolidated_metrics = []
        all_es_documents = []
        
        for account in accounts:
            logger.debug(f"Starting to collect metrics for account {account.project_name}.")
            account_id = account.account_id
            start_date = (
                (end_date - timedelta(minutes=account.collection_interval))
                if start_time == None
                else start_time
            )

            try:
                validate_dates(start_date, end_date)
                account_session = assume_role(account_id, org_session)
                cloudwatch_client = create_aws_client(account_session, "cloudwatch")
                intervals = split_time_range(
                    start_date, end_date, account.metrics_collection_period
                )
                logger.debug(f"Total intervals to process: {len(intervals)}")

                metrics = get_all_cloudwatch_metrics(cloudwatch_client)
                logger.debug(f"Found {len(metrics)} metrics")
            except DateValidationException as error:
                logger.error(error)
            except AwsClientErrorException as error:
                logger.error(error)
            except PeriodException as error:
                logger.error(error)
            except AwsCollectMetricsException as error:
                logger.error(error)



            try:
                intervals = split_time_range(
                    start_date, end_date, account.metrics_collection_period
                )
                logger.debug(f"Total intervals to process: {len(intervals)}")

                metrics = get_all_cloudwatch_metrics(cloudwatch_client)
                logger.debug(f"Found {len(metrics)} metrics")

                with ThreadPoolExecutor(max_workers=settings.MAX_THREADS) as executor:
                    futures = [
                        executor.submit(
                            process_interval,
                            cloudwatch_client,
                            es_client,
                            metrics,
                            account,
                            interval
                        )
                        for interval in intervals
                    ]

                    for future in as_completed(futures):
                        try:
                            consolidated_metrics, es_documents = future.result()
                            all_consolidated_metrics.extend(consolidated_metrics)
                        
                            if es_documents and len(es_documents) > 0:
                                all_es_documents.extend(es_documents)
                            else:
                                logger.info(f"No ES documents found for account {account_id} in this interval")
                        #TODO: Add exception handling   
                        except Exception as e:
                            logger.error(f"Error processing an interval for account {account_id}: {e}")
            
                if all_consolidated_metrics and len(all_consolidated_metrics) > 0:
                    try:
                        account_session = assume_role(settings.BUCKET_MONITORING_ACCOUNT, org_session)
                        s3_client = create_aws_client(account_session, "s3")
                        save_metrics_to_s3(s3_client, all_consolidated_metrics, account, start_date)
                        logger.debug(f"Saving {len(all_consolidated_metrics)} metrics to S3")

                    except AwsClientErrorException as error:
                        logger.error(error)

                index_name = f"metrics-testing-{account_id}"
                #if all_es_documents and len(all_es_documents) > 0:

                actions = [
                    {
                        "_op_type": "create",
                        "_index": index_name,
                        "_source": document
                    }
                    for document in all_es_documents
                ]

                try:
                    success, failed = helpers.bulk(es_client, actions, raise_on_error=False, chunk_size=5000, max_retries=3)

                    print(f"Documentos indexados con éxito: {success}")
                    print(f"Documentos que fallaron al indexar: {len(failed)}")

                    # Imprimir los errores de los documentos que fallaron
                    for error in failed:
                        print(f"Error: {error}")
                except Exception as e:
                    print(f"Error al indexar documentos: {e}")
                    #logger.info(f"Saved {len(all_es_documents)} documents for account {account_id}")
                    #except Exception as e:
                    #    logger.error(f"Error saving documents to ES: {e}")

            except PeriodException as error:
                logger.error(error)
            except AwsCollectMetricsException as error:
                logger.error(error)



def fetch_metrics(session, service_name):
    """Fetch metrics from CloudWatch for a specific service."""
    try:
        cloudwatch_client = session.client("cloudwatch", region_name="us-east-1")
        # Define specific metric names for each service
        metric_names = {
            "ecs": ["CPUUtilization", "MemoryUtilization"],
            "s3": ["BucketSizeBytes", "NumberOfObjects"],
            "rds": ["CPUUtilization", "FreeStorageSpace"],
        }

        metrics_data = []
        for metric_name in metric_names.get(service_name, []):
            response = cloudwatch_client.get_metric_data(
                MetricDataQueries=[
                    {
                        "Id": f"{service_name.lower()}_{metric_name.lower()}",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": f"AWS/{service_name.upper()}",
                                "MetricName": metric_name,
                            },
                            "Period": 300,  # 5 minutes
                            "Stat": "Average",
                        },
                        "ReturnData": True,
                    },
                ],
                StartTime=datetime.utcnow() - timedelta(hours=1),
                EndTime=datetime.utcnow(),
            )
            metrics_data.extend(response["MetricDataResults"])
        logging.info(f"Fetched metrics for service {service_name}.")
        return metrics_data
    except ClientError as e:
        logging.error(f"Client error fetching metrics for service {service_name}: {e}")
        raise
    except Exception as e:
        logging.error(
            f"Unexpected error fetching metrics for service {service_name}: {e}"
        )
        raise


""" def save_metrics_to_s3(metrics_data, account_id, service_name):
    # Convert data to a Pandas DataFrame
    df = pd.DataFrame(metrics_data)
    
    # Convert to Parquet format
    table = pa.Table.from_pandas(df)
    file_path = f'/tmp/{account_id}_{service_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet'
    pq.write_table(table, file_path)
    
    # Upload file to S3
    s3_path = f'{account_id}/{service_name}/{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet'
    s3_client.upload_file(file_path, bucket_name, s3_path) """


if __name__ == "__main__":
    logger = setup_logger(__name__)
    logger.debug("Starting CloudWatch Metrics Extraction")
    start_process_time = time.time()

    try:
        accounts, config, args = initial_configurations()
        es_client = create_es_client()
        session = get_organization_session()

    except FileLoadException as error:
        logger.error(error)
    except AwsClientErrorException as error:
        logger.error(error)
    except ElasticSearchClientException as error:
        logger.error(error)

    end_date = args.end_time
    start_time = args.start_time

    start_collect_metrics = time.time()
    try:
        collect_metrics(accounts, session, es_client, start_time, end_date)
    except Exception as error:
        logger.error(error)
    end_process_time = time.time()

    collect_metrics_time = end_process_time - start_collect_metrics
    logger.warning(f"Tiempo de ejecución métricas: {collect_metrics_time:.4f} segundos")

    elapsed_time = end_process_time - start_process_time
    logger.warning(f"Tiempo de ejecución: {elapsed_time:.4f} segundos")
    logger.info("Finalizando la operación")
