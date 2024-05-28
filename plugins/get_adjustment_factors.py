import pandas as pd
from core_classes import GCPReader, download_yahoo_data, DataReaderClass
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path, construct_destination_path


class SQLReaderAdjustmentFactors(GCPReader):
    '''

    Reads daily equity adjustment factors from redshift provided a date range and universe date

    '''

    PROVIDES_FIELDS = ["adjustment_factor_data"]

    def __init__(self, start_date, end_date):
        base_query = """
        select ticker, dcm_security_id, cob as date, split_factor, split_div_factor
        from marketdata.equity_adjustment_factors
        where (cob >= date('{}') and cob < date('{}'))
        and as_of_end is NULL
        """
        table_name = "equity_adjustment_factors"
        self.base_query = base_query
        self.table_name = table_name
        self.start_date = start_date
        self.end_date = end_date

    def _prepare_to_pull_data(self, **kwargs):
        self.query_client = bigquery.Client()

    def _pull_data(self, **kwargs):
        job_config = bigquery.QueryJobConfig(allow_large_results=True)
        final_query = self.compose_query()
        data = self.query_client.query(final_query, job_config=job_config)
        data = data.to_dataframe()
        return data

    def compose_query(self):
        date_st = self.start_date
        date_end = self.end_date
        final_query = self.base_query.format(date_st, date_end)
        return final_query

    def _post_process_pulled_data(self, data, **kwargs):
        return data

    @classmethod
    def get_default_config(cls):
        return {"table_name": "equity_adjustment_factors"}


SQLReaderAdjustmentFactors_params = {
    'params': {"start_date": "2000-01-03", "end_date": RUN_DATE},
    'class': SQLReaderAdjustmentFactors,
    'start_date': RUN_DATE,
    'provided_data': {
        'adjustment_factor_data': construct_destination_path('get_adjustment_factors')
    },
    'required_data': {},
}
