#!/usr/bin/env python
# coding:utf-8
"""
  Author:  Chris Ingrassia --<c.ingrassia@datacapitalmanagement.com>
  Purpose: DataExtraction configuration container
  Created: 7/16/2015
"""
from os import path

__author__ = "Chris Ingrassia, Ali Nazari, Jack Kim, Karina Uribe"
__copyright__ = "Copyright (C) 2016, The Data Capital Management Intuition project - All Rights Reserved"
__credits__ = ["Chris Ingrassia", "Napoleon Hernandez", "Ali Nazari", "Jack Kim",
               "Karina Uribe"]
__license__ = "Proprietary and confidential. Unauthorized copying of this file, via any medium is strictly prohibited"
__version__ = "1.0.0"
__maintainer__ = "Chris Ingrassia"
__email__ = "it@datacapitalmanagement.com"
__status__ = "Development"

from commonlib.config import get_config, set_config, load

set_config(load(defaults=None,
                app_config=path.join(path.dirname(__file__), "data_extraction.conf"),
                user_config="None",
                load_and_setup_logging=False))


DateLabel = "Date"
VolumeLabel = "Volume"
OpenLabel = "Open"
CloseLabel = "Close"
HighLabel = "High"
LowLabel = "Low"
TickerLabel = "Ticker"
TickDataTickerLabel = "ticker"
TickDataAsof = "as_of"

EquityPriceLabels = [VolumeLabel, OpenLabel, CloseLabel, HighLabel, LowLabel]
TickDataEquityPriceLabels = [VolumeLabel, OpenLabel, CloseLabel, HighLabel, LowLabel, TickDataTickerLabel, TickDataAsof]
FirstDateToCheck = "2004-01-02"
