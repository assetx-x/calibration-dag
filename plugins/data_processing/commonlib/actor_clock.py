#!/usr/bin/env python
# coding:utf-8
"""
  Author:  Napoleon Hernandez --<n.hernandez@datacapitalmanagement.com>
  Purpose: First prototype of DCM Intuition trading system
  Created: 7/16/2015
"""

__author__ = "Napoleon Hernandez, Ali Nazari, Jack Kim, Karina Uribe"
__copyright__ = "Copyright (C) 2015, The Data Capital Management Intuition project - All Rights Reserved"
__credits__ = ["Napoleon Hernandez", "Ali Nazari", "Jack Kim",
               "Karina Uribe"]
__license__ = "Proprietary and confidential. Unauthorized copying of this file, via any medium is strictly prohibited"
__version__ = "1.0.0b"
__maintainer__ = "Napoleon Hernandez"
__email__ = "it@datacapitalmanagement.com"
__status__ = "Development"

from commonlib.intuition_base_objects  import DCMIntuitionObject
import numpy as np
from commonlib.intuition_loggers import *
from warnings import warn
from pandas import Timestamp

__clock = None


def create_clock():
    """ Creates the master/global clock saved into a module level variable,
        effectively making it a singleton.  This won't block you from
        creating other instances of ActorClock, though.
    """
    global __clock
    if __clock is None:
        __clock = ActorClock()
    else:
        warn("create_clock called, but has already been previously created")
    return get_clock()


def get_clock():
    if __clock is not None:
        return __clock
    else:
        return create_clock()


def set_clock(clock):
    global __clock
    __clock = clock


def to_date(timestamp):
    return timestamp.tz_localize(tz=None).normalize()


class ActorClock(DCMIntuitionObject):
    """ Tracks date and time accross the platform.

    Attributes
    ----------
        time: datetime.time or pd.Timestamp
            Shows date and time: for real time trading it shows current time, for backtesting it shows date in the past

    """

    EXCLUDED_ATTRS_FOR_STATE = ["log", "time_offset"]

    def __init__(self):
        self.time = None
        self.time_in_64bit_format = None
        self.unix_timestamp = None
        self.date = None
        self.time_offset=None

    def set_time_offset(self, timedelta):
        self.time_offset = timedelta

    def update_time(self, datetime_update):
        """ Updates date and time (in 64-bit format).
        Args:
            datetime_update : datetime.datetime or pd.Timestamp
                A variable to set up the market
        Returns:
            None
        """
        datetime_update = datetime_update if not self.time_offset else datetime_update - self.time_offset
        self.time = datetime_update
        self.date = None
        try:
            self.time_in_64bit_format = self.time.to_datetime64()
            self.unix_timestamp = Timestamp(self.time).asm8.astype(np.int64)
        except AttributeError:
            self.time_in_64bit_format = self.time
            self.unix_timestamp = self.time
        log.error("Changing time of clock to %s", self.time)

    def get_time(self):
        """ Returns tic-time for market transactions.
        """
        return self.time

    def get_unix_timestamp(self):
        return self.unix_timestamp

    def get_time_64bit(self):
        """ Returns time in 64 bit format for market transactions.
        """
        return self.time_in_64bit_format

    def get_date(self):
        if self.date is None:
            self.date = to_date(self.time)
        return self.date

    def __getinitargs__(self):
        return ()
