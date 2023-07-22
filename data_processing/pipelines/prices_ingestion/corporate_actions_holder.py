import pandas as pd

from io import BytesIO
from commonlib.market_timeline import marketTimeline
from google.cloud import storage
from pipelines.prices_ingestion.config import get_config
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_data_keys_between_dates


class EquityETLBaseClass(object):
    pass


class CorporateActionsHolder(EquityETLBaseClass):
    pass


class S3TickDataCorporateActionsHolder(CorporateActionsHolder):
    TICKDATA_S3_LOCATION_PREFIX = get_config()["raw_data_pull"]["TickData"]["CA"]["data_location_prefix"]

    @staticmethod
    def verify_no_duplicates_ca(ca_data_normalized):
        def get_ticker_info(ca_data):  # type: (pd.DataFrame) -> tuple
            mask_footer_info = (ca_data.index.get_level_values('ACTION') == 'FOOTER')
            return tuple(ca_data.loc[mask_footer_info].reset_index()[['SYMBOL', 'ACTION DATE']].values.flatten())

        mask_action_dividend = (ca_data_normalized.index.get_level_values('ACTION') == 'DIVIDEND')
        if any(ca_data_normalized.loc[mask_action_dividend].index.duplicated()):
            # Aggregate duplicates in DIVIDENDs (if any: e.g. JPM, MRGR, AEO, etc.)
            ca_data_normalized.loc[mask_action_dividend, ['GROSS AMOUNT', 'ADJUSTMENT FACTOR']] = \
                ca_data_normalized.loc[mask_action_dividend].groupby(level=['ACTION', 'ACTION DATE']).agg(
                    {'GROSS AMOUNT': 'sum', 'ADJUSTMENT FACTOR': pd.np.prod})

        if any(ca_data_normalized.loc[~mask_action_dividend].index.duplicated()):
            print("Duplicate corporate action record for Ticker '%s' in CA file as_of '%s'" % get_ticker_info(
                ca_data_normalized))

        return ca_data_normalized.drop_duplicates().sort_index()

    @staticmethod
    def parse_tickdata_data(ca_file):
        df = pd.read_csv(ca_file, header=None, sep="^")

        # Select header (records that starts with //) and replace the old format naming to the new format
        # Tickdata Changed Columns in CA file on 2017/08/09/: 'COMPANY NAME' -> 'NAME',
        # 'CURRENCY TYPE' -> 'CURRENCY', 'LAST DATE' -> 'DATE'
        mask = df[0].str.startswith("//")
        df[0].loc[mask] = df[0].loc[mask].apply(lambda x: x.replace('COMPANY NAME', 'NAME').replace(
            'CURRENCY TYPE', 'CURRENCY').replace('LAST DATE', 'DATE'))

        df = df[0].str.split(",", expand=True)
        df = df.replace({"": pd.np.NaN, None: pd.np.NaN})
        df.columns = range(len(df.columns))

        # Normalizing data of CA
        data_columns = df.ix[df[0].str.startswith("//"), :].groupby(0).apply(
            lambda x: x.apply(lambda y: list(y.values[1:]), axis=1).values[0])
        data_columns.index = data_columns.index.str.replace("//", "")
        data_info = df.ix[~df[0].str.startswith("//"), :]
        all_actions_per_type = [data_info[data_info[0] == data_columns.index[k]].rename(
            columns=dict(zip(range(1, len(df.columns) + 1), data_columns.iloc[k]))) for k in range(len(data_columns))]
        ca_data_normalized = pd.concat([k.ix[:, k.columns.map(pd.notnull).tolist()] for k in all_actions_per_type])
        ca_data_normalized["ACTION DATE"] = df[2].reindex(ca_data_normalized.index)

        date_columns = ca_data_normalized.filter(like='DATE').columns.tolist()
        if "CUSIP" not in ca_data_normalized.columns:
            raise KeyError("S3TickDataCorporateActionsHolder.parse_tickdata_data - the CA files does not contain CUSIP."
                           "Aborting parsing")
        non_numeric_columns = ["CUSIP", "NEW VALUE", "OLD VALUE"]
        non_date_columns = list(set(ca_data_normalized.columns).difference(set(date_columns + non_numeric_columns)))
        ca_data_normalized[non_date_columns] = ca_data_normalized[non_date_columns].apply(pd.to_numeric, errors="ignore")
        ca_data_normalized[date_columns] = ca_data_normalized[date_columns].apply(pd.to_datetime, errors="ignore")

        ca_data_normalized = ca_data_normalized.rename(columns={0: "ACTION"})
        ca_data_normalized = ca_data_normalized.set_index(["ACTION", "ACTION DATE"])
        ca_data_normalized = S3TickDataCorporateActionsHolder.verify_no_duplicates_ca(ca_data_normalized)
        ca_data_normalized.sort_index(level=["ACTION DATE", "ACTION"], inplace=True)
        return ca_data_normalized, data_columns

    def __init__(self, ticker, run_date=None, as_of_date=None, override_with_lineage=None, start_dt=None, end_dt=None):
        super(S3TickDataCorporateActionsHolder, self).__init__()
        self.override_s3_source = override_with_lineage
        self.ticker = ticker
        self.run_date = run_date if run_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        self.start_dt = start_dt or get_config()["adjustments"]["data_start"]
        self.end_dt = end_dt or get_config()["adjustments"]["data_end"]
        self.data_lineage = None
        self.ca_data_normalized = None
        self.data_columns = None

        self._get_tickdata_ca()
        self.full_adjustment_factor = self.__calculate_full_adjustment_factor()
        self.split_adjustment_factor = self.__calculate_split_adjustment_factor()

    def __get_ca_key(self):
        bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
        data_location_prefix = get_config()["raw_data_pull"]["TickData"]["CA"]["data_location_prefix"]
        key_iterator = get_data_keys_between_dates(bucket_id, data_location_prefix,
                                                   identifier=self.ticker,
                                                   start_date=pd.Timestamp(self.run_date),
                                                   end_date=pd.Timestamp(self.run_date),
                                                   as_of_dt=pd.Timestamp(self.as_of_date))
        if not key_iterator:
            raise RuntimeError("ERROR! could not find suitable Tickdata CA to read as of {0} for ticker {1} "
                               "on bucket {2}, location {3} on date {4}".format(self.as_of_date, self.ticker,
                                                                                bucket_id, data_location_prefix,
                                                                                self.run_date))
        dt, s3_key_handler = max([k for k in key_iterator], key=lambda x:x[0])
        if s3_key_handler.size == 0:
            raise RuntimeError("S3TickDataCorporateActionsHolder._get_tickdata_ca - File {0} has zero size"
                                   .format(s3_key_handler.name))
        return bucket_id, s3_key_handler.name

    def _get_tickdata_ca(self):
        bucket_id, s3_key = self.__get_ca_key()

        # Reading data; it contains variable number of columns so need to read line per row, then split and populate NaN
        storage_client = storage.Client()
        ca_file = BytesIO(storage_client.bucket(bucket_id).blob(s3_key).download_as_string())

        self.ca_data_normalized, self.data_columns = self.parse_tickdata_data(ca_file)
        self.data_lineage = "gs://{0}/{1}".format(bucket_id, s3_key)

    def __calculate_disc_factor(self, adj_factors):
        trading_days = marketTimeline.get_trading_days(self.start_dt.date(), self.end_dt.date())
        factor_ts = adj_factors.groupby("ACTION DATE").agg({"ADJUSTMENT FACTOR": pd.np.prod})["ADJUSTMENT FACTOR"]\
            .reindex(trading_days).sort_index().shift(-1).fillna(1.0).astype(float)
        adj_factor = (1.0 / factor_ts.sort_index(ascending=False).cumprod()).sort_index()
        return adj_factor

    def __calculate_full_adjustment_factor(self):
        adj_factors = pd.concat([self.ca_data_normalized["RATIO"].dropna().reset_index().drop_duplicates()
                                 .rename(columns={"RATIO":"ADJUSTMENT FACTOR"}),
                                 self.ca_data_normalized["ADJUSTMENT FACTOR"].dropna().reset_index().drop_duplicates()])
        return self.__calculate_disc_factor(adj_factors)

    def __calculate_split_adjustment_factor(self):
        adj_factors = pd.concat([self.ca_data_normalized["RATIO"].dropna().reset_index().drop_duplicates()
                                 .rename(columns={"RATIO":"ADJUSTMENT FACTOR"})])
        return self.__calculate_disc_factor(adj_factors)

    def get_full_adjustment_factor(self):
        return self.full_adjustment_factor

    def get_split_adjustment_factor(self):
        return self.split_adjustment_factor

    def get_data_lineage(self):
        return self.data_lineage

    def get_ca_data_normalized(self):
        return self.ca_data_normalized

    def get_ca_information(self, label):
        result = self.ca_data_normalized.ix[label,:].dropna(how="all", axis=1) \
            if label in self.ca_data_normalized.index else (None if label not in self.data_columns.index
                                                            else pd.DataFrame(columns=filter(lambda x: not pd.isnull(x),
                                                                                             self.data_columns[label])))
        return result


class S3YahooCorporateActionsHolder(CorporateActionsHolder):
    def __init__(self, ticker, run_date=None, as_of_date=None, override_with_lineage=None):
        self.override_s3_source = override_with_lineage
        CorporateActionsHolder.__init__(self)
        self.data_lineage = None
        self.ticker = ticker
        self.run_date = run_date if run_date else get_config()["adjustments"]["data_end"]
        self.as_of_date = as_of_date if as_of_date else get_config()["adjustments"]["as_of_date"]
        self.ca_data = self.__read_yahoo_corporate_actions()

    def __read_yahoo_corporate_actions(self):
        if not self.override_s3_source:
            bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
            data_location_prefix = get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["ca_data_location_prefix"]
            key_iterator = get_data_keys_between_dates(bucket_id, data_location_prefix, self.ticker,
                                                       pd.Timestamp(self.run_date), pd.Timestamp(self.run_date),
                                                       pd.Timestamp(self.as_of_date))
            if not key_iterator:
                raise RuntimeError("ERROR! could not find suitable data to read as of {0} for ticker {1} on bucket {2}"
                                   ", location {3} on date {4}".format(self.as_of_date, self.ticker,
                                                           bucket_id, data_location_prefix, self.run_date))
            dt, s3_key_handler = max([k for k in key_iterator], key=lambda x:x[0])
            if s3_key_handler.size == 0:
                raise RuntimeError("S3YahooCorporateActionsHolder.__read_yahoo_corporate_actions - File {0} has zero size"
                                   .format(s3_key_handler.key))
            s3_key = s3_key_handler.name
        else:
            s3_location = self.override_s3_source.replace("gs://","")
            bucket_id = s3_location.split("/")[0]
            s3_key = s3_location.replace("{0}/".format(bucket_id),"")
        print(s3_key)
        storage_client = storage.Client()
        ca_file = BytesIO(storage_client.bucket(bucket_id).blob(s3_key).download_as_string())
        res = pd.read_csv(ca_file, parse_dates=["Date"]).set_index("Date")
        self.data_lineage = "gs://{0}/{1}".format(bucket_id, s3_key)
        return res

    def get_ca_data(self):
        return self.ca_data

    def get_data_lineage(self):
        return self.data_lineage


class S3TickDataCorporateActionsHandler(object):

    def __init__(self):
        pass

    @staticmethod
    def _get_ca_key(ticker, start_date, end_date, as_of_dt):
        bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
        data_location_prefix = get_config()["raw_data_pull"]["TickData"]["CA"]["data_location_prefix"]

        # Note: corporate actions data is available on s3 at around 06:30 AM UTC every day (if no issues in ingestion
        # pipelines encountered on that day). So, to see the history of ticker's symbols change, one need to set
        # hour & minutes of as_of_dt later than 06:30 AM UTC on the requested day. Sometimes, though, it takes longer
        # time for pipeline to complete - one need to be more conservative with as_of_dt, e.g. '2018-01-03 08:30:00'
        all_data_keys = get_data_keys_between_dates(bucket_id, data_location_prefix,
                                                    identifier=ticker,
                                                    start_date=end_date,
                                                    end_date=end_date,
                                                    as_of_dt=as_of_dt)
        if not all_data_keys:
            raise RuntimeError("ERROR! could not find suitable Tickdata CA to read as of {0} for ticker {1} on "
                               "bucket {2}, location {3}".format(as_of_dt, ticker, bucket_id, data_location_prefix))

        found_objects = [k[1] for k in all_data_keys]
        if len(found_objects) > 1:
            raise RuntimeError("ERROR! Found too many CA files as of {0} for ticker {1} on bucket {2}, location {3}: "
                               "{4} files".format(as_of_dt, ticker, bucket_id, data_location_prefix, len(found_objects)))

        s3_object = found_objects[0]
        if s3_object.size == 0:
            raise RuntimeError("S3TickDataCorporateActionsHolder._get_tickdata_ca - File {0} has zero size"
                               .format(s3_object.name))
        return bucket_id, s3_object.name

    @staticmethod
    def _get_tickdata_ca(ticker, start_date, end_date, as_of_dt):
        bucket_id, s3_key = S3TickDataCorporateActionsHandler._get_ca_key(ticker, start_date, end_date, as_of_dt)
        storage_client = storage.Client()
        ca_file = BytesIO(storage_client.bucket(bucket_id).blob(s3_key).download_as_string())
        ca_data_normalized, data_columns = S3TickDataCorporateActionsHolder.parse_tickdata_data(ca_file)
        return ca_data_normalized, data_columns

    @staticmethod
    def build_ticker_name_change_history(ca_data_normalized):  # type: (pd.DataFrame) -> pd.DataFrame
        COLUMNS_IN_CA_AFFECTING_SYMBOL = ["HEADER", "TICKER SYMBOL CHANGE", "FOOTER"]
        DESIRED_COLUMNS = ["SYMBOL", "COMPANY ID", "NEW VALUE", "OLD VALUE"]

        mask = ca_data_normalized.index.get_level_values(0).isin(COLUMNS_IN_CA_AFFECTING_SYMBOL)
        ticker_history = ca_data_normalized.loc[mask, DESIRED_COLUMNS].sort_index(level='ACTION DATE').reset_index().copy()
        ticker_history.rename(columns={"SYMBOL": "Symbol", "COMPANY ID": "CompanyID", "ACTION": "Action"}, inplace=True)

        # Preparing symbols' as_of_start and as_of_end columns
        ticker_history["as_of_start"] = ticker_history["ACTION DATE"]
        ticker_history["as_of_end"] = ticker_history["as_of_start"].shift(-1)

        # Setting Symbol from NEW VALUE column
        mask = (ticker_history['Action'] == 'TICKER SYMBOL CHANGE')
        ticker_history.loc[mask, 'Symbol'] = ticker_history.loc[mask, 'NEW VALUE']
        ticker_history['Symbol'] = ticker_history['Symbol'].str.replace('[^a-zA-Z]', '')  # symbol JW_A, BF.B, BRK_B
        ticker_history = ticker_history.loc[ticker_history['Action'] != 'FOOTER', ['CompanyID', 'Symbol', 'as_of_start',
                                                                                   'as_of_end']]

        # Each symbol used by a given ticker is defined on time range defined by as_of_start and as_of_end (inclusive)
        # Shift in 1 business day except latest record needed to apply queries using BETWEEN statement (i.e. inclusive)
        as_of_end_idx = ticker_history.columns.get_loc("as_of_end")  # index of column used for .iloc[]
        ticker_history.iloc[:-1, as_of_end_idx] = ticker_history.iloc[:-1, as_of_end_idx].map(
            lambda x: marketTimeline.get_previous_trading_day(x))

        # Below is verification that there is one and only one unique company ID in the DataFrame for a given company.
        # This check is needed because history of symbols change is based exclusively on TickDataCA file which exhibits
        # time-to-time some errors...
        if ticker_history["CompanyID"].unique().size > 1:
            raise ValueError("DataFrame with the ticker symbol change history should have only 1 unique CompanyID. "
                             "Instead got %d IDs. Symbol(s): %s" % (ticker_history["CompanyID"].unique().size,
                                                                    list(ticker_history["Symbol"].unique())))
        return ticker_history

    @staticmethod
    def get_tickdata_ca_ticker_name_change_history(ticker, start_date, end_date, as_of_dt):
        ca_data_normalized, _ = S3TickDataCorporateActionsHandler._get_tickdata_ca(ticker, start_date, end_date, as_of_dt)
        ticker_history = S3TickDataCorporateActionsHandler.build_ticker_name_change_history(ca_data_normalized)
        return ticker_history


def _main():
    # Note: corporate actions data is available on s3 at around 06:30 AM UTC every day (if no issues in ingestion
    # pipelines encountered on that day). So, to see the history of ticker's symbols change, one need to set
    # hour & minutes of as_of_dt later than 06:30 AM UTC on the requested day. Sometimes, though, it takes longer
    # time for pipeline to complete - one need to be more conservative with as_of_dt, e.g. '2018-01-03 08:30:00'

    # ticker = 'USCI'
    # start_date = pd.Timestamp('2019-01-28')
    # end_date = pd.Timestamp('2019-01-28')
    # # as_of_dt = pd.Timestamp('2019-01-30')
    # as_of_dt = pd.Timestamp('2019-01-29 05:39:52')

    ticker = 'EGHT'
    as_of_dt = pd.Timestamp("2019-02-10")
    start_date = pd.Timestamp("2000-01-03")
    end_date = pd.Timestamp("2019-02-08")  # pd.Timestamp("2017-07-13")

    ticker_history = S3TickDataCorporateActionsHandler.get_tickdata_ca_ticker_name_change_history(
        ticker, start_date, end_date, as_of_dt)
    print(ticker_history)


if __name__ == "__main__":
    _main()
