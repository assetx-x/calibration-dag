import pandas as pd
from luigi import Task, Parameter

from base import DCMTaskParams, HdfStorageMixin


class MergeFundamentalsTask(DCMTaskParams, HdfStorageMixin, Task):
    def __init__(self, *args, **kwargs):
        super(MergeFundamentalsTask, self).__init__(*args, **kwargs)
        self.new_records = None
        self.data_to_update = None
        self.records_to_retry = None

    def run(self):
        input_df = self._load_new_data()

        bitemporal_df = self._load_df('bitemporal_data')
        if bitemporal_df is None:
            bitemporal_df = self._load_existing_data()
            self._store_df(bitemporal_df, 'bitemporal_data')

        self._merge_data(input_df, bitemporal_df)
        self._store_merged_data()

    def _merge_data(self, input_df, bitemporal_df):
        # assert bitemporal_df.groupby(["metric", "ticker", "date"]).agg(len).max(axis=1).max().astype(int) == 1
        # assert input_df.groupby(["metric", "ticker", "date"]).agg(len).max(axis=1).max().astype(int) == 1
        records_to_analyze = self._get_records_to_analyze(input_df, bitemporal_df)
        self.data_to_update = self._get_records_to_update(records_to_analyze)
        self.records_to_retry = self._get_records_to_retry(records_to_analyze)
        self.new_records = self._get_records_to_insert(input_df, bitemporal_df)

    def _get_records_to_analyze(self, input_df, bitemporal_df):
        self.logger.info("Merging existing and new data frames left")
        merged_data = pd.merge(bitemporal_df, input_df, on=["metric", "ticker", "date"],
                               suffixes=("_old", "_new"), how="left")
        new_matched_columns = merged_data.columns[merged_data.columns.str.endswith("_new")]

        self.logger.info("Getting rid of nulls")
        missing_pulled_values_mask = (pd.isnull(merged_data[new_matched_columns])).all(axis=1)
        return merged_data[~missing_pulled_values_mask]

    def _get_records_to_update(self, records_to_analyze):
        self.logger.info("Calculating records to update")
        rta = records_to_analyze
        # Take all records that have the old value of NaN or the the value has changed by more than epsilon
        rtu = rta[rta["value_old"].isnull() | ((rta["value_old"] - rta["value_new"]).abs() > 0.001)]
        # Operator '>' above will filter out most NaNs, but if the old value was NaN the new may be as well
        # So filtering those out will keep old NaN values that received a new value
        return rtu[rtu["value_new"].notnull()]

    def _get_records_to_retry(self, records_to_analyze):
        self.logger.info("Calculating records to retry")
        rta = records_to_analyze
        return rta[rta["raw_value_old"].notnull() & rta["raw_value_new"].isnull()]

    def _get_records_to_insert(self, input_df, bitemporal_df):
        self.logger.info("Merging existing and new data frames right")
        merged_data = pd.merge(bitemporal_df, input_df, on=["metric", "ticker", "date"],
                               suffixes=("_old", "_new"), how="right")
        old_matched_columns = merged_data.columns[merged_data.columns.str.endswith("_old")]

        self.logger.info("Getting rid of nulls 2")
        new_records_mask = (pd.isnull(merged_data[old_matched_columns])).all(axis=1)

        self.logger.info("Calculating records to insert")
        return merged_data[new_records_mask]

    def _load_new_data(self):
        input_path = self.input()["new_fundamentals"].path
        self.logger.info("Loading new fundamentals data from hdf file " + input_path)
        input_df = pd.read_hdf(input_path, key="fundamentals_data")
        self.logger.info("Done loading new fundamentals")
        # return input_df[input_df['ticker'] == 'FB']
        return input_df

    def _load_existing_data(self):
        query = """
                SELECT *
                FROM {table_name}
                WHERE as_of_end IS NULL
                ORDER BY ticker, date
                """.format(table_name=self.input()["bitemporal_data"].table)
        # and ticker = 'FB'
        self.logger.info("Loading existing fundamentals data from redshift table:")
        self.logger.info(query)
        bitemporal_df = pd.read_sql(query, self.input()["bitemporal_data"].uri())
        self.logger.info("Done loading existing fundamentals")
        return bitemporal_df

    def _store_merged_data(self):
        self._store_df(self.new_records, 'new_records')
        self._store_df(self.data_to_update, 'data_to_update')
        self._store_df(self.records_to_retry, 'records_to_retry')
