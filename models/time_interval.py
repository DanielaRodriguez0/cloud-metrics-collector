""" Model for date ranges """
from datetime import datetime
from pydantic import ConfigDict
from pydantic.dataclasses import dataclass


@dataclass(config=ConfigDict(validate_assignment=True))
class TimeIntervalModel:
    """Class representing an intervals for collecting metrics"""
    start_date: datetime
    end_date: datetime
    period: int
