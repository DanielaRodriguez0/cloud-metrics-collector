""" Custom exceptions for the agents """

class FileLoadException(Exception):
    """Exception raised for errors in loading configurations or AWS account information."""
    def __init__(self, message: str):
        """
        Initializes the FileLoadException.

        Arguments:
            message: The error message from the exception.
        """
        self.general_message = "Error loading configurations or accountinformation"
        super().__init__(f"{self.general_message}: {message}")


class AwsClientErrorException(Exception):
    """Exception raised for errors in creating AWS client or session."""
    def __init__(self, message: str):
        """
        Initializes the AwsClientErrorException.

        Arguments:
            message: The error message from the AWS client or session.
        """
        self.general_message = "ClientError"
        super().__init__(f"{self.general_message}: {message}")


class ElasticSearchClientException(Exception):
    """Exception raised for errors in creating ElasticSearch client."""
    def __init__(self, message: str):
        """
        Initializes the ElasticSearchClientException.

        Arguments:
            message: The error message from the ElasticSearch client.
        """
        self.general_message = "ElasticSearchClientError"
        super().__init__(f"{self.general_message}: {message}")


class DateValidationException(Exception):
    """Exception raised for date validation."""
    def __init__(self, message: str):
        """
        Initializes the DateValidationException.

        Arguments:
            message: explanation of the error.
        """
        self.general_message = "Date validation error"
        super().__init__(f"{self.general_message}: {message}")


class PeriodException(Exception):
    """Exception raised for errors encountered while calculating the period."""
    def __init__(self, message: str):
        """
        Initializes the PeriodException.

        Arguments:
            message: explanation of the error.
        """
        self.general_message = "Error calculating the period"
        super().__init__(f"{self.general_message}: {message}")



class AwsCollectMetricsException(Exception):
    """ Exception raised for errors in collecting cloudwatch metrics. """
    def __init__(self, message: str):
        """
        Initializes the AwsCollectMetricsException.

        Arguments:
            message: The error message from the AWS client or session.
        """
        self.general_message = "Error collecting cloudwatch metrics"
        super().__init__(f"{self.general_message}: {message}")


class AwsParamValidationErrorException(Exception):
    def __init__(self, message: str):
        """
        Initializes the AwsClientException.

        Arguments:
            message: The error message from the AWS client or session.
        """
        
        self.general_message = "ParamValidationError"
        super().__init__(f"{self.general_message}: {message}")
