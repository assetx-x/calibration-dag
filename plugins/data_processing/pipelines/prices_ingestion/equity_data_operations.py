import pandas as pd
import numpy as np
from collections import defaultdict
from pipelines.prices_ingestion.etl_workflow_aux_functions import create_timeline
from pipelines.prices_ingestion.config import get_config
from pipelines.prices_ingestion.corporate_actions_holder import EquityETLBaseClass
from pipelines.prices_ingestion.equity_data_holders import (EquityDataHolder, S3YahooEquityDataHolder, VolumeLabel, CloseLabel, OpenLabel,
                                 LowLabel, HighLabel, EquityPriceLabels)

REPORT_ERROR_LABEL = get_config()["etl_equities_results_labels"]["REPORT_ERROR_LABEL"]
REPORT_WARNING_LABEL = get_config()["etl_equities_results_labels"]["REPORT_WARNING_LABEL"]
REPORT_DESCRIPTION_LABEL = get_config()["etl_equities_results_labels"]["REPORT_DESCRIPTION_LABEL"]
REPORT_DETAILS_LABEL = get_config()["etl_equities_results_labels"]["REPORT_DETAILS_LABEL"]


class DerivedEquityDataHolder(EquityDataHolder):
    def read_data(self, **kwargs):
        pass

    def set_internal_flags(self):
        pass

    def _do_additional_initialization(self,**kwargs):
        if "equity_data" not in kwargs:
            raise RuntimeError("Parameter data needs to be available on kwargs for DerivedEquityDataHolder")
        if "adjusted_data_flag" not in kwargs:
            raise RuntimeError("Parameter adjusted_data_flag needs to be available on kwargs for "
                               " DerivedEquityDataHolder")
        if "daily_frequency_flag" not in kwargs:
            raise RuntimeError("Parameter daily_frequency_flag needs to be available on kwargs for "
                               "DerivedEquityDataHolder")
        self.adjusted_data_flag = kwargs["adjusted_data_flag"]
        self.daily_frequency_flag = kwargs["daily_frequency_flag"]
        self.equity_data = kwargs["equity_data"]

    def __init__(self, ticker, equity_data, daily_frequency_flag, adjusted_data_flag, as_of_date):
        EquityDataHolder.__init__(self, ticker, start_date=str(equity_data.index.min().date()),
                                  end_date=str(equity_data.index.max().date()), as_of_date = as_of_date,
                                  equity_data=equity_data, daily_frequency_flag=daily_frequency_flag,
                                  adjusted_data_flag=adjusted_data_flag)

    def set_data_lineage(self, data_lineage):
        self.data_lineage = data_lineage


class DataOperation(EquityETLBaseClass):
    def __init__(self, name):
        self.name = name

    def verify_compatibility(self, dh):
        raise NotImplementedError("Method DataOperation.verify_compatibility needs to be overriden in derived class")

    def _make_adjustment(self, dh):
        raise NotImplementedError("Method DataOperation._make_adjustment needs to be overriden in derived class")

    def get_flags(self, dh):
        raise NotImplementedError("Method DataOperation.get_flags needs to be overriden in derived class")

    def do_operation_over_data(self, dh):
        if self.verify_compatibility(dh):
            new_data = self._make_adjustment(dh)
            daily_frequency_flag, adjusted_data_flag = self.get_flags(dh)
            result = DerivedEquityDataHolder(dh.get_ticker(), new_data, daily_frequency_flag, adjusted_data_flag,
                                             as_of_date=dh.as_of_date)
            # if not pd.np.allclose(result.get_data(), new_data):
            #    raise ValueError("DataOperation.do_operation_over_data - The derived equity data holder does not have "
            #                     "the correct adjusted information, please verify")
            result.set_data_lineage("{0}({1})".format(self.__class__.__name__, dh.get_data_lineage()))
            return result
        else:
            raise RuntimeError("Operator {0} is not compatible with data holder {1} with lineage {2}"
                               .format(self.__class__.__name__, dh.__class__.__name__, dh.get_data_lineage()))

    def __call__(self, dh):
        return self.do_operation_over_data(dh)


class DataTransformations(DataOperation):
    pass


class MinuteDataAggregator(DataTransformations):
    def __init__(self, aggregate_in_timezone="US/Eastern"):
        DataTransformations.__init__(self, "-> Aggregating minute data")
        self.aggregate_in_timezone = aggregate_in_timezone

    def verify_compatibility(self, dh):
        return not dh.is_daily_data()

    def _make_adjustment(self, dh):
        """ Aggregates at daily level.
        Args
        ----
           data_df: pd.Dataframe

        Returns:
           A pd.Dataframe with aggregated data.
        """
        data_df = dh.get_data()
        dataframe = data_df.copy().sort_index()
        dataframe.index = dataframe.index.tz_convert(self.aggregate_in_timezone)

        # TODO: this aggregation assumes that the data times are already cut between open of market and
        #  close of market. One should verify that this is indeed the case.
        agg_dict = {OpenLabel: lambda x: x.iloc[0], HighLabel: pd.np.max,
                    LowLabel: pd.np.min, CloseLabel: lambda x: x.iloc[-1],
                    VolumeLabel: pd.np.sum}
        aggregated_minute = dataframe.groupby(dataframe.index.normalize()).agg(agg_dict)
        aggregated_minute.index = aggregated_minute.index.tz_convert(data_df.index.tzinfo).normalize()
        return aggregated_minute[EquityPriceLabels]

    def get_flags(self, dh):
        return True, dh.is_data_adjusted()


class SplitAndDividendEquityAdjuster(DataTransformations):
    def __init__(self, full_multiplicative_ratio, split_multiplicative_ratio, from_unadjusted_to_adjusted=True):
        """
            Attributes
            ----------
               full_multiplicative_ratio: pd.Series
               split_multiplicative_ratio: pd.Series
        """
        DataTransformations.__init__(self, "-> Applying adjustment factors to data")
        self.full_multiplicative_ratio = full_multiplicative_ratio
        self.full_multiplicative_ratio.name = "Full_ratio"
        self.split_multiplicative_ratio = split_multiplicative_ratio
        self.split_multiplicative_ratio.name = "Split_ratio"
        self.from_unadjusted_to_adjusted = from_unadjusted_to_adjusted

    def verify_compatibility(self, dh):
        return self.from_unadjusted_to_adjusted == (not dh.is_data_adjusted())

    def __adjust_received_data_for_splits(self, dataframe, ticker):
        dataframe["DateTime"] = pd.DatetimeIndex(dataframe.index)
        dataframe["Date_only"] = dataframe.index.tz_localize(None).normalize()
        daily_data_factors = pd.merge(self.full_multiplicative_ratio.to_frame(),
                                      self.split_multiplicative_ratio.to_frame(), right_index=True,
                                      left_index=True)
        daily_data_factors["Date"] = daily_data_factors.index
        merged_df = pd.merge(dataframe, daily_data_factors, 'left', left_on=["Date_only"], right_on="Date")
        merged_df["Open_adjusted"] = merged_df[OpenLabel]  / merged_df["Full_ratio"]
        merged_df["Hi_adjusted"] = merged_df[HighLabel] / merged_df["Full_ratio"]
        merged_df["Lo_adjusted"] = merged_df[LowLabel]  / merged_df["Full_ratio"]
        merged_df["Close_adjusted"] = merged_df[CloseLabel] / merged_df["Full_ratio"]
        merged_df["Vol_adjusted"] = merged_df[VolumeLabel]* merged_df["Split_ratio"]
        merged_df.drop([OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel], inplace=True, axis=1)
        merged_df.set_index("DateTime", inplace=True)
        dict_columns = {"Open_adjusted": OpenLabel, "Hi_adjusted": HighLabel, "Lo_adjusted": LowLabel,
                        "Close_adjusted": CloseLabel, "Vol_adjusted": VolumeLabel}
        merged_df.rename(columns=dict_columns, inplace=True)
        return merged_df[EquityPriceLabels]

    def __unadjust_received_data_for_splits(self, dataframe, ticker):
        dataframe["DateTime"] = pd.to_datetime(dataframe.index)
        dataframe["Date_only"] = pd.to_datetime(dataframe.index.date)
        daily_data_factors = pd.merge(self.full_multiplicative_ratio.to_frame(),
                                      self.split_multiplicative_ratio.to_frame(),
                                      left_index=True, right_index=True)
        daily_data_factors["Date"] = daily_data_factors.index
        merged_df = pd.merge(dataframe, daily_data_factors, 'left', left_on=["Date_only"], right_on="Date")
        merged_df["Open_adjusted"] = merged_df[OpenLabel] * merged_df["Full_ratio"]
        merged_df["Hi_adjusted"] = merged_df[HighLabel] * merged_df["Full_ratio"]
        merged_df["Lo_adjusted"] = merged_df[LowLabel] * merged_df["Full_ratio"]
        merged_df["Close_adjusted"] = merged_df[CloseLabel] * merged_df["Full_ratio"]
        merged_df["Vol_adjusted"] = merged_df[VolumeLabel] / merged_df["Split_ratio"]
        merged_df.drop([OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel], inplace=True, axis=1)
        merged_df.set_index("DateTime", inplace=True)
        dict_columns = {"Open_adjusted": OpenLabel, "Hi_adjusted": HighLabel, "Lo_adjusted": LowLabel,
                        "Close_adjusted": CloseLabel, "Vol_adjusted": VolumeLabel}
        merged_df.rename(columns=dict_columns, inplace=True)
        return merged_df[EquityPriceLabels]

    def _make_adjustment(self, dh):
        """ Adjusts equity minute level data based on dividend and split_ratio.

        Args
        ----
           dataframe: pd.Dataframe
              A minute level dataframe.

        Returns
        -------
           A dataframe with adjusted equity data.
        """
        if self.from_unadjusted_to_adjusted:
            return self.__adjust_received_data_for_splits(dh.get_data().copy(), dh.get_ticker())
        else:
            return self.__unadjust_received_data_for_splits(dh.get_data().copy(), dh.get_ticker())

    def get_flags(self, dh):
        return dh.is_daily_data(), False

    def set_adjustment_mode(self, from_unadjusted_to_adjusted_flag):
        self.from_unadjusted_to_adjusted = from_unadjusted_to_adjusted_flag


class DataVerificators(DataOperation):
    def __init__(self, name, do_operation_over_data_flag=True):
        DataOperation.__init__(self, name)
        self.do_operation_over_data_flag = do_operation_over_data_flag
        self.verification_output = None

    def add_information_to_report_dict(self, report_dict, label, category_name, description, details):
        report_dict[label][category_name] = {}
        report_dict[label][category_name][REPORT_DESCRIPTION_LABEL] = description
        report_dict[label][category_name][REPORT_DETAILS_LABEL] = details

    def verify_data(self, dh):
        """ This function is used to do data validations. """
        raise NotImplementedError("Method DataVerificators.verify_data_only needs to be overriden in derived class")

    def report_verification_results(self):
        """ This function is used to do data validations. """
        raise NotImplementedError("Method DataVerificators.report_verification_results needs to be overriden in "
                                  "derived class")

    def set_do_operation_over_data_flag(self, value):
        self.do_operation_over_data_flag = value

    def do_operation_over_data(self, dh):
        if self.verify_compatibility(dh):
            self.verify_data(dh)
            if self.do_operation_over_data_flag:
                return DataOperation.do_operation_over_data(self, dh)
            else:
                return self.report_verification_results()
        else:
            raise RuntimeError("Operator {0} is not compatible with data holder {1} with lineage {2}"
                               .format(self.__class__.__name__, dh.__class__.__name__, dh.get_data_lineage()))


class VerifyTimelineForTickDataPricesAndCA(DataVerificators):
    def __init__(self, prices_data_holder, corp_actions, cut_off_date_for_missing_data, test_start_dt, test_end_dt,
                 do_operation_over_data_flag=True):
        DataVerificators.__init__(self, "-> Verifying timeline", do_operation_over_data_flag)
        self.price_dh = prices_data_holder
        self.corp_actions = corp_actions
        self.cut_off_date_for_missing_data = cut_off_date_for_missing_data
        self.test_start_dt = test_start_dt
        self.test_end_dt = test_end_dt
        self.verification_output = {}

    def get_start_end_date_of_ticker(self):
        """ Returns start and end date for ticker from corporate actions records. """
        start_dt = pd.Timestamp(self.corp_actions.get_ca_information("HEADER").index[0]).date()
        start_dt = max(start_dt, pd.Timestamp(self.test_start_dt).date())
        end_dt = pd.Timestamp(self.corp_actions.get_ca_information("FOOTER").index[0]).date()
        end_dt = min(end_dt, pd.Timestamp(self.test_end_dt).date())
        return start_dt, end_dt

    def verify_timeline_vs_ca_timeline(self, df):
        """ Cut dataframe based on start and end dates from corporate actions records. This will ensure equity
        data used is for the current ticker. This is helpful in cases where the Ticker was reused.

        Args:
        ----
           df: pd.Dataframe
              Dataframe with equity data
           ticker: str
              Identifies the security
        """
        ca_begin_dt, ca_end_dt = self.get_start_end_date_of_ticker()
        df_begin_dt = df.index.min().date()
        df_end_dt = df.index.max().date()
        if ca_begin_dt < df_begin_dt:
            ca_begin_dt = df_begin_dt
            # self.verification_output["CA_dt_error"] = (ca_begin_dt, df_begin_dt)
        self.verification_output["CA_dates"] = (ca_begin_dt, ca_end_dt)

    def cut_timeline_based_on_ca_timeline(self, df):
        ca_begin_dt, ca_end_dt = self.verification_output["CA_dates"]
        dates_gt_begin = df.index.asi8.astype(np.int64) >= (pd.Timestamp(ca_begin_dt).normalize()
                                                        - pd.tseries.frequencies.to_offset("1n")).asm8.astype(np.int64)
        dates_lt_end = df.index.asi8.astype(np.int64) <= (pd.Timestamp(ca_end_dt).normalize().tz_localize("US/Eastern")
                                                      + pd.tseries.frequencies.to_offset("1D")).asm8.astype(np.int64)

        df = df.copy()[dates_gt_begin & dates_lt_end]
        return df

    def _make_adjustment(self, dataholder):
        """ initial_date and end_date are based are adjusted to fit the timeline in corporate actions,
        in case the ticker was re-used. """
        df = dataholder.get_data()
        if "CA_dt_error" in self.verification_output:
            raise ValueError("Corporate actions imply there are missing dates on the provided data {0}. According to "
                             "corporate actions the start date should be {1}, but the data starts on {2}"
                             .format(dataholder.get_data_lineage(), self.verification_output["CA_dt_error"][0],
                                     self.verification_output["CA_dt_error"][1]))
        if "CA_dates" in self.verification_output:
            df = self.cut_timeline_based_on_ca_timeline(df)
        return df

    def verify_compatibility(self, dh):
        return not dh.is_daily_data()

    def get_flags(self, dh):
        return False, dh.is_data_adjusted()

    def verify_data(self, dh):
        """ This function creates the flow for data validations only. """
        self.verification_output = {}
        df = dh.get_data()
        self.verify_timeline_vs_ca_timeline(df)
        initial_date = df.index.min().date()
        end_date = df.index.max().date()

    def report_verification_results(self):
        """ This function returns status objects with the outcome and result of the verification tests. """
        verif_dict = defaultdict(dict)
        if "CA_dt_error" in self.verification_output:
            error_msg = "Discrepancies with the start date according to corporate actions and available tickdata."
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "StartDate", error_msg,
                                                self.verification_output["CA_dt_error"])
        return dict(verif_dict)


class VerifyTimeline(DataVerificators):
    def __init__(self, reference_data_holder, corp_actions, cut_off_date_for_missing_data, test_start_dt, test_end_dt,
                 do_operation_over_data_flag=True):
        DataVerificators.__init__(self, "-> Verifying timeline", do_operation_over_data_flag)
        self.yahoo_dh = reference_data_holder
        self.corp_actions = corp_actions
        self.cut_off_date_for_missing_data = cut_off_date_for_missing_data
        self.test_start_dt = test_start_dt
        self.test_end_dt = test_end_dt
        self.verification_output = {}

    def get_start_end_date_of_ticker(self):
        """ Returns start and end date for ticker from corporate actions records. """
        corp_actions = self.corp_actions.get_ca_data().copy()
        corp_actions.sort_index(inplace=True)
        start_dt = corp_actions[corp_actions["Action"] == "STARTDATE"].index.date[0]
        start_dt = max(start_dt, pd.Timestamp(self.test_start_dt).date())
        end_dt = corp_actions[corp_actions["Action"] == "ENDDATE"].index.date[0]
        end_dt = min(end_dt, pd.Timestamp(self.test_end_dt).date())
        return (start_dt, end_dt)

    def verify_timeline_vs_ca_timeline(self, df):
        """ Cut dataframe based on start and end dates from corporate actions records. This will ensure equity
        data used is for the current ticker. This is helpful in cases where the Ticker was reused.

        Args:
        ----
           df: pd.Dataframe
              Dataframe with equity data
           ticker: str
              Identifies the security
        """
        ca_begin_dt, ca_end_dt = self.get_start_end_date_of_ticker()
        df_begin_dt = df.index.min().date()
        df_end_dt = df.index.max().date()
        if ca_begin_dt < df_begin_dt:
            self.verification_output["CA_dt_error"] = (ca_begin_dt, df_begin_dt)
        # if ca_begin_dt > df_begin_dt:
        #    self.log_report_message(ticker, "START_TRADING_DAY_MISMATCH", df_begin_dt, "COB", ca_begin_dt,
        #                            "Tick data was trimmed to first trading date according to CA", logging.WARNING)
        self.verification_output["CA_dates"] = (ca_begin_dt, ca_end_dt)

    def cut_timeline_based_on_ca_timeline(self, df):
        ca_begin_dt, ca_end_dt = self.verification_output["CA_dates"]
        dates_gt_begin = df.index.asi8.astype(np.int64) >= (pd.Timestamp(ca_begin_dt).normalize()
                                                        - pd.tseries.frequencies.to_offset("1n")).asm8.astype(np.int64)
        dates_lt_end = df.index.asi8.astype(np.int64) <= (pd.Timestamp(ca_end_dt).normalize().tz_localize("US/Eastern")
                                                      + pd.tseries.frequencies.to_offset("1D")).asm8.astype(np.int64)

        df = df.copy()[dates_gt_begin & dates_lt_end]
        return df

    def get_dates_in_yahoo_with_no_volume(self):
        """ Returns dates in yahoo with no volume."""
        yahoo_df = self.yahoo_dh.get_data().copy()
        dates_in_yahoo = yahoo_df.index.date
        dates_in_yahoo_with_no_volume = yahoo_df.index[yahoo_df[VolumeLabel] == 0].date
        return dates_in_yahoo_with_no_volume

    def check_dates_with_holes(self, df, date_timeline):
        """ Verifies equity data with dates where yahoo reported zero volume for dates after
        cut_off_date_for_missing_data.

        Args:
        ----
           df: pd.Dataframe
              Dataframe with equity data
           ticker: str
              Identifies the security
        """
        """
        NOTES:
        Currently there is no check of yahoo vs. market timeline, we may want to change that
        Still there is an implicit check since there is tickdata vs yahoo.
        For now an error should be thrown if yahoo has dates not on the market timeline, only via the comparison with tickdata.
        In the future these cases should be handled better.

        Since the comparison is implict, cases where yahoo doesn't match market timeline before tickdata dates is masked by other errors completely
        """
        dates_with_holes_dict = {}
        # Nothing is done when there are dates_in_yahoo_with_no_volume and tickdata reports volume since we
        # consider that tickdata has different data provider from yahoo
        dates_in_yahoo_with_no_volume = self.get_dates_in_yahoo_with_no_volume()
        dates_in_tick_data = df.index.normalize().unique().date
        date_timeline_dates = set(pd.DatetimeIndex(date_timeline).unique().date)
        non_zero_market_dates = date_timeline_dates.difference(set(dates_in_yahoo_with_no_volume))
        dates_with_holes = non_zero_market_dates.difference(set(dates_in_tick_data))
        relevant_dates_with_holes = filter(lambda x: x > \
                                           pd.Timestamp(self.cut_off_date_for_missing_data).to_pydatetime().date(),
                                           dates_with_holes)
        not_relevant_dates_with_holes = dates_with_holes.difference(set(relevant_dates_with_holes))
        dates_with_holes_dict["Non_relevant_dt_with_holes"] = not_relevant_dates_with_holes
        dates_with_holes_dict["Relevant_dt_with_holes"] = relevant_dates_with_holes
        self.verification_output["Dates_with_holes"] = dates_with_holes_dict
        missing_yahoo_dt = set(pd.DatetimeIndex(df.index).normalize().unique().tz_localize(None))\
            .difference(set(self.yahoo_dh.get_data().index))

        if len(missing_yahoo_dt) > 0:
            self.verification_output["Missmatch_yahoo_vs_tickdata_dates"] = True
            self.verification_output["Dates_missing_in_yahoo"] = missing_yahoo_dt

    def _make_adjustment(self, dataholder):
        """ initial_date and end_date are based are adjusted to fit the timeline in corporate actions,
        in case the ticker was re-used. """
        df = dataholder.get_data()
        if "CA_dt_error" in self.verification_output:
            raise ValueError("Corporate actions imply there are missing dates on the provided data {0}. According to "
                             "corporate actions the start date should be {1}, but the data starts on {2}"
                             .format(dataholder.get_data_lineage(), self.verification_output["CA_dt_error"][0],
                                     self.verification_output["CA_dt_error"][1]))
        if "CA_dates" in self.verification_output:
            df = self.cut_timeline_based_on_ca_timeline(df)
        """
        The following repeat of code is meant to handle cases such as FB where the ticker was used for two companies,
        This can be handled without calling check_dates_with_holes a second time.
        ##TODO: Handle such cases without recalling check_dates_with_holes
        """
        initial_date = df.index.min().date()
        end_date = df.index.max().date()
        timeindex, date_timeline = create_timeline(initial_date, end_date, calculate_full_timeline=False)
        self.verification_output = {}
        self.check_dates_with_holes(df, date_timeline)
        if "Dates_with_holes" in self.verification_output and \
           len(list(self.verification_output["Dates_with_holes"]["Relevant_dt_with_holes"])) != 0:
            raise ValueError("There are entire missing days on the equity data; missing days include {0}"
                             .format(self.verification_output["Dates_with_holes"]["Relevant_dt_with_holes"]))
        if "Missmatch_yahoo_vs_tickdata_dates" in self.verification_output and \
           len(self.verification_output["Dates_missing_in_yahoo"]) != 0:
            raise ValueError("There are trading dates missing in yahoo equity data. Missing dates include {0}"
                             .format(self.verification_output["Dates_missing_in_yahoo"]))
        return df

    def verify_compatibility(self, dh):
        return not dh.is_daily_data()

    def get_flags(self, dh):
        return False, dh.is_data_adjusted()

    def verify_data(self, dh):
        """ This function creates the flow for data validations only. """
        self.verification_output = {}
        df = dh.get_data()
        self.verify_timeline_vs_ca_timeline(df)
        initial_date = df.index.min().date()
        end_date = df.index.max().date()
        timeindex, date_timeline = create_timeline(initial_date, end_date, calculate_full_timeline=False)
        self.check_dates_with_holes(df, date_timeline)

    def report_verification_results(self):
        """ This function returns status objects with the outcome and result of the verification tests. """
        verif_dict = defaultdict(dict)
        if "CA_dt_error" in self.verification_output:
            error_msg = "Discrepancies with the start date according to corporate actions and available tickdata."
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "StartDate", error_msg,
                                                self.verification_output["CA_dt_error"])

        if "Dates_with_holes" in self.verification_output and \
           len(self.verification_output["Dates_with_holes"]["Relevant_dt_with_holes"]) != 0:
            error_msg = "Dates when yahoo didn't reported zero volume after cut off date but do not exist on Tickdata"
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "Holes", error_msg,
                                                self.verification_output["Dates_with_holes"]["Relevant_dt_with_holes"])

        if "Dates_with_holes" in self.verification_output and \
           len(self.verification_output["Dates_with_holes"]["Non_relevant_dt_with_holes"]) != 0:

            warn_msg = "Dates when yahoo reported zero volume before cut off date with zero volume in TickData"
            self.add_information_to_report_dict(verif_dict, REPORT_WARNING_LABEL, "Holes", warn_msg,
                                                self.verification_output["Dates_with_holes"]["Non_relevant_dt_with_holes"])

        if "Missmatch_yahoo_vs_tickdata_dates" in self.verification_output and \
           len(self.verification_output["Dates_missing_in_yahoo"]) != 0:
            error_msg = "There are dates missing in yahoo data"
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "YahooHoles", error_msg,
                                                self.verification_output["Dates_missing_in_yahoo"])
        return dict(verif_dict)


class CompleteMinutes(DataVerificators):
    def __init__(self, do_operation_over_data_flag=True, start_date=None, end_date=None):
        DataVerificators.__init__(self, "-> Fill missing minutes", do_operation_over_data_flag)
        self.start_time = pd.Timestamp(get_config()["adjustments"]["rth_start"]).time()
        self.end_time = pd.Timestamp(get_config()["adjustments"]["rth_end"]).time()
        self.start_date = start_date if start_date else get_config()["adjustments"]["data_start"]
        self.end_date = end_date if end_date else get_config()["adjustments"]["data_end"]
        self.verification_output = {}

    def normalization_timeline(self, df, timeindex, ticker):
        """ Nomalized a minute indexed dataframe. Missing values are fill with '0' for volumen and forward values
        for price data.
        Args:
        -----
           df: pd.Dataframe
              Equity minute data
           timeindex: datetime
           ticker: str

        Returns
        -------
           A pandas.Dataframe with intra-day minutes in 'UTC', missing values for volume=0 and forward values for price.
        """
        timeindex_zone = timeindex.tzinfo.zone
        df_zone = df.index.tzinfo.zone
        df.index = df.index.copy().asi8.astype(np.int64)
        timeindex = timeindex.asi8.astype(np.int64)
        normalized_minute_data = df.reindex(timeindex)
        df.index = pd.DatetimeIndex(df.index.values).tz_localize("UTC").tz_convert(df_zone)
        normalized_minute_data.index = pd.DatetimeIndex(normalized_minute_data.index.values)\
            .tz_localize("UTC").tz_convert(timeindex_zone)
        normalized_vol = normalized_minute_data[VolumeLabel]
        normalized_vol.fillna(0, inplace=True)
        normalized_price = normalized_minute_data.drop([VolumeLabel], axis=1)

        # TODO: lines below are the original logic for ffill and bfill (these should be removed and
        #  replaced with the logic currently commented out -> 2 time periods)
        normalized_price = normalized_price.groupby(normalized_price.index.date).apply(
            lambda x: x.fillna(method="ffill").fillna(method="bfill"))
        normalized_price.fillna(method="ffill", inplace=True)
        normalized_price.fillna(method='bfill', inplace=True)

        # TODO: below is a new logic for ffill and bfill (it will bfill only certain number of minutes and the
        #  rest will rest as pd.np.nan). It should be added as there are securities that starts trading at e.g.
        #  09:50 which is way to far as for bfill down to 09:30. Though, one need to check that all tests works!
        # Splitting on 2 time periods with different logic for bfill at SOD
        # bfill_limit_minutes = get_config()["adjustments"]["bfill_limit_minutes"]
        # normalization_cutoff = get_config()["adjustments"]["normalization_cutoff"]
        # mask = normalized_price.index.normalize() >= pd.Timestamp(normalization_cutoff, tz=df_zone)
        #
        # price_before = normalized_price[~mask].groupby(normalized_price[~mask].index.date).apply(
        #     lambda x: x.fillna(method="ffill").fillna(method="bfill"))
        # price_after = normalized_price[mask].groupby(normalized_price[mask].index.date).apply(
        #     lambda x: x.fillna(method="ffill").fillna(method="bfill", limit=bfill_limit_minutes))
        # normalized_price = pd.concat([price_before, price_after], axis=0)

        normalized_minute_data = pd.concat([normalized_price, normalized_vol], axis=1, ignore_index=False)
        volume_per_date = normalized_minute_data.groupby(lambda x:x.normalize()).agg({VolumeLabel: pd.np.sum})
        volume_per_date.index = volume_per_date.index.date
        bad_dates = volume_per_date.index[volume_per_date[VolumeLabel] == 0]
        if not len(bad_dates) == 0:
            # Missing days (volume = 0) fill in with NaN
            normalized_minute_data.loc[pd.Series(normalized_minute_data.index.date).isin(bad_dates).values,:] = pd.np.NaN
            normalized_minute_data.loc[pd.Series(normalized_minute_data.index.date).isin(bad_dates).values,VolumeLabel] = 0
            self.verification_output["bad_dates"] = bad_dates
            # for d in bad_dates:
            #    self.log_report_message(ticker, "NO_DAILY_VOLUME", d, VolumeLabel, [None, pd.np.NaN])
        return normalized_minute_data

    def verify_compatibility(self, dh):
        return not dh.is_daily_data()

    def get_flags(self, dh):
        return False, dh.is_data_adjusted()

    def _make_adjustment(self, dataholder):
        """ initial_date and end_date are based on the new dataframe after CA validations,
        in case the ticker was re-used. """
        df = dataholder.get_data()
        if "Missing_minutes" in self.verification_output and \
           len(self.verification_output["Missing_minutes"]) != 0 or ("Invalid_tz_index" in self.verification_output):
            initial_date = df.index.min().date()
            end_date = df.index.max().date()
            timeindex, date_timeline = create_timeline(initial_date, end_date, self.start_time, self.end_time)
            normalized_data = self.normalization_timeline(df, timeindex, dataholder.get_ticker())
            return normalized_data
        return df

    def validate_index_properties(self, df, timeindex):
        """ Verifies that the timeline for the dataframe is equal to the expected timeline and that it's in 'UTC'. """
        missing_minutes = timeindex[~timeindex.isin(df.index)]
        if len(missing_minutes) != 0:
            self.verification_output["Missing_minutes"] = missing_minutes
        if not df.index.tzinfo or df.index.tzinfo.zone != "UTC":
            self.verification_output["Invalid_tz_index"] = str(df.index.tzinfo)

    def verify_data(self, dh):
        """ This function creates the flow for data validations only. """
        self.verification_output = {}
        df = dh.get_data()
        # NG - To get reviewed this change for empty df here
        if df.empty:
            return
        initial_date = df.index.min().date()
        end_date = df.index.max().date()
        timeindex, date_timeline = create_timeline(initial_date, end_date, self.start_time, self.end_time)
        self.validate_index_properties(dh.get_data(), timeindex)

    def report_verification_results(self):
        """ This function returns status objects with the outcome and result of the verification tests. """

        verif_dict = defaultdict(dict)
        if "Missing_minutes" in self.verification_output:
            error_msg = "Missing minutes vs expected timeline"
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "MissingMinutes",
                                                error_msg, len(self.verification_output["Missing_minutes"]))

        if "Invalid_tz_index" in self.verification_output:
            error_msg = "Incorrect timezone for index"
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "IncorrectTimezone",
                                                error_msg, self.verification_output["Invalid_tz_index"])
        return verif_dict


class S3YahooPriceAdjuster(SplitAndDividendEquityAdjuster):
    def __init__(self, yahoo_data):
        if not isinstance(yahoo_data, S3YahooEquityDataHolder):
            raise RuntimeError("The constructor of S3YahooPriceAdjuster requires a S3YahooEquityDataHolder as input")
        yahoo_full_adjustment_factor = yahoo_data.get_full_multiplicative_adjustment_factor()
        yahoo_split_adjustment_factor = yahoo_data.get_split_multiplicative_adjustment_factor()
        SplitAndDividendEquityAdjuster.__init__(self, yahoo_full_adjustment_factor, yahoo_split_adjustment_factor)

    def __adjust_received_data_for_splits(self, dataframe):
        test_frame = dataframe[[CloseLabel]]

        daily_data_factors = pd.merge(self.full_multiplicative_ratio.to_frame(),
                                      self.split_multiplicative_ratio.to_frame(), right_index=True,
                                      left_index=True)
        daily_data_factors["Date"] = daily_data_factors.index
        corrected_daily_data_factors = daily_data_factors
        ref_df = yahoo_data.get_data()[[CloseLabel]] / daily_data_factors
        test_frame /= merged_df["Full_ratio"]
        aggregated_original_data = test_frame.groupby(["Date"]).agg({CloseLabel: lambda x:x.iloc[-1]})

        outlier_price_limit = get_config()["adjustments"]["corporate_action_outlier"]["outlier_price_limit"]
        differences_df = (test_frame / ref_df - 1.0).dropna().abs()
        diff_price_dates = differences_df.index[(differences_df >= outlier_price_limit).any(axis=1)]
        test_frame["Date_only"] = pd.to_datetime(dataframe.index)

        for dt in diff_price_dates:
            if agg_original_data.loc[dt, VolumeLabel] != 0:
                previous_day_index = pd.Index(daily_data_factors["Date"]).get_loc(dt) - 1
                if previous_day_index>=0:
                    previous = daily_data_factors.iloc[previous_day_index, "Date"]
                    corrected_daily_datafactors.ix[dt] = previous
                    corrected_daily_datafactors.loc[dt, "Date"] = dt

        dataframe["DateTime"] = pd.to_datetime(dataframe.index)
        dataframe["Date_only"] = pd.to_datetime(dataframe.index.date)
        merged_df = pd.merge(dataframe, corrected_daily_data_factors, 'left', left_on=["Date_only"], right_on="Date")

        merged_df["Open_adjusted"] = merged_df[OpenLabel]  / merged_df["Full_ratio"]
        merged_df["Hi_adjusted"] = merged_df[HighLabel] / merged_df["Full_ratio"]
        merged_df["Lo_adjusted"] = merged_df[LowLabel]  / merged_df["Full_ratio"]
        merged_df["Close_adjusted"] = merged_df[CloseLabel] / merged_df["Full_ratio"]
        merged_df["Vol_adjusted"] = merged_df[VolumeLabel]* merged_df["Split_ratio"]
        merged_df.drop([OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel], inplace=True, axis=1)
        merged_df.set_index("DateTime", inplace=True)
        dict_columns = {"Open_adjusted": OpenLabel, "Hi_adjusted": HighLabel, "Lo_adjusted": LowLabel,
                        "Close_adjusted": CloseLabel, "Vol_adjusted": VolumeLabel}
        merged_df.rename(columns=dict_columns, inplace=True)
        return merged_df[EquityPriceLabels]

    def __unadjust_received_data_for_splits(self, dataframe):
        test_frame = dataframe[[CloseLabel]]

        daily_data_factors = pd.merge(self.full_multiplicative_ratio.to_frame(),
                                      self.split_multiplicative_ratio.to_frame(), right_index=True,
                                      left_index=True)
        daily_data_factors["Date"] = daily_data_factors.index
        corrected_daily_data_factors = daily_data_factors
        ref_df = yahoo_data.get_data()[[CloseLabel]] * daily_data_factors
        test_frame *= merged_df["Full_ratio"]
        aggregated_original_data = test_frame.groupby(["Date"]).agg({CloseLabel: lambda x:x.iloc[-1]})

        outlier_price_limit = get_config()["adjustments"]["corporate_action_outlier"]["outlier_price_limit"]
        differences_df = (test_frame / ref_df - 1.0).dropna().abs()
        diff_price_dates = differences_df.index[(differences_df >= outlier_price_limit).any(axis=1)]
        test_frame["Date_only"] = pd.to_datetime(dataframe.index)

        for dt in diff_price_dates:
            if agg_original_data.loc[dt, VolumeLabel] != 0:
                previous = daily_data_factors[daily_data_factors["Date"]<dt].ix[-1]
                corrected_daily_datafactors.ix[dt] = previous
                corrected_daily_datafactors.loc[dt, "Date"] = dt

        dataframe["DateTime"] = pd.to_datetime(dataframe.index)
        dataframe["Date_only"] = pd.to_datetime(dataframe.index.date)
        merged_df = pd.merge(dataframe, corrected_daily_data_factors, 'left', left_on=["Date_only"], right_on="Date")

        merged_df["Open_adjusted"] = merged_df[OpenLabel]  * merged_df["Full_ratio"]
        merged_df["Hi_adjusted"] = merged_df[HighLabel] * merged_df["Full_ratio"]
        merged_df["Lo_adjusted"] = merged_df[LowLabel]  * merged_df["Full_ratio"]
        merged_df["Close_adjusted"] = merged_df[CloseLabel] * merged_df["Full_ratio"]
        merged_df["Vol_adjusted"] = merged_df[VolumeLabel] / merged_df["Split_ratio"]
        merged_df.drop([OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel], inplace=True, axis=1)
        merged_df.set_index("DateTime", inplace=True)
        dict_columns = {"Open_adjusted": OpenLabel, "Hi_adjusted": HighLabel, "Lo_adjusted": LowLabel,
                        "Close_adjusted": CloseLabel, "Vol_adjusted": VolumeLabel}
        merged_df.rename(columns=dict_columns, inplace=True)
        return merged_df[EquityPriceLabels]

    def get_flags(self, dh):
        return dh.is_daily_data(), True


class VerifyMinuteOutlierForPrice(DataVerificators):
    def __init__(self, reference_data_holder, do_operation_over_data_flag=True):
        DataVerificators.__init__(self, "-> Price outlier removal", do_operation_over_data_flag)
        self.reference_data_holder = reference_data_holder
        self.verification_output = {}
        self.aggregated_minute_data = None

    def identify_outlier_for_price(self, dataframe):
        """ Identifies the dates in which the absolute price difference with reference_data
        are greater than outlier_price_limit.

        Returns
        -------
           A pd.tseries with the dates for the ourliers identified.
        """
        data_df = dataframe.loc[:, [OpenLabel, HighLabel, LowLabel, CloseLabel]]
        # TODO: is it right to assume the reference frame is on the same timezone as the aggregated data? this would need
        #  improvements later if we get data from other timezones nto US markets
        ref_df = self.reference_data_holder.get_data()[[OpenLabel, HighLabel, LowLabel, CloseLabel]]\
            .tz_localize(data_df.index.tzinfo.zone).reindex(data_df.index).round(2)
        outlier_price_limit = get_config()["adjustments"]["verification_parameters"]["minute_outlier"]\
            ["outlier_price_limit"]

        # This should fill missing values in minutes if data is absent in the beginning
        data_df.fillna(method='ffill', inplace=True)
        data_df.fillna(method='bfill', inplace=True)
        differences_df = (data_df / ref_df - 1.0).dropna().abs()
        price_outliers_dt = differences_df.index[(differences_df >= outlier_price_limit).any(axis=1)].tolist()
        if len(price_outliers_dt):
            self.verification_output["Price_outliers_dt"] = price_outliers_dt

    def __correct_minute_data_to_daily_reference_price(self, original_data, agg_original_data):
        dataframe = original_data.copy(deep=True)
        vol_data = original_data[VolumeLabel].copy()
        dataframe.drop([VolumeLabel], inplace=True, axis=1)
        # diff_price_dates = self.identify_outlier_for_price(agg_original_data)
        diff_price_dates = self.verification_output["Price_outliers_dt"]

        dataframe["Date_only"] = dataframe.index.normalize()
        dataframe_outlier_dts = dataframe[dataframe["Date_only"].isin(diff_price_dates)].copy(deep=True)
        ref_data_outliers = self.reference_data_holder.get_data()[self.reference_data_holder.get_data()
                                                                  .index.isin(diff_price_dates)].reset_index().copy(deep=True)

        dataframe_outlier_dts["Date_only"] = dataframe_outlier_dts["Date_only"].astype(np.int64)
        ref_data_outliers["Date"] = ref_data_outliers["Date"].astype(np.int64)
        temp = pd.merge(dataframe_outlier_dts.reset_index(), ref_data_outliers, left_on=["Date_only"],
                        right_on=["Date"], suffixes=("","_ref"))

        for k in [OpenLabel, HighLabel, LowLabel, CloseLabel]:
            temp[k] = pd.np.minimum(pd.np.maximum(temp[k], temp["{0}_ref".format(LowLabel)]),
                                    temp["{0}_ref".format(HighLabel)])

        closing_time = pd.Timestamp(get_config()["adjustments"]["rth_end"]).time()
        opening_time = pd.Timestamp(get_config()["adjustments"]["rth_start"]).time()
        closings_to_fix = pd.DatetimeIndex(temp["Date"]).tz_convert("US/Eastern").indexer_at_time(closing_time)
        openings_to_fix = pd.DatetimeIndex(temp["Date"]).tz_convert("US/Eastern").indexer_at_time(opening_time)
        openings = temp[temp.index.isin(openings_to_fix)].copy(deep=True)
        closings = temp[temp.index.isin(closings_to_fix)].copy(deep=True)
        openings[OpenLabel] = openings["{0}_ref".format(OpenLabel)].copy(deep=True)
        closings[CloseLabel] = closings["{0}_ref".format(CloseLabel)].copy(deep=True)

        openings[HighLabel] = pd.np.maximum(openings[HighLabel], openings[OpenLabel])
        openings[LowLabel] = pd.np.minimum(openings[LowLabel], openings[OpenLabel])
        closings[HighLabel] = pd.np.maximum(closings[HighLabel], closings[CloseLabel])
        closings[LowLabel] = pd.np.minimum(closings[LowLabel], closings[CloseLabel])

        corrected_data = openings.combine_first(closings).combine_first(temp).set_index("Date")[EquityPriceLabels]
        final_data = corrected_data.combine_first(original_data)
        final_data[VolumeLabel] = vol_data
        data_changed = final_data[(final_data - original_data).round(2) !=0].dropna(how="all")

        return final_data[original_data.columns].sort_index()

    def _make_adjustment(self, dataholder):
        if "Negative_price_values" in self.verification_output:
            raise ValueError("Negative prices identified on the provided data {0}"
                             .format(dataholder.get_data_lineage()))
        if "Hi_greater_Lo" in self.verification_output:
            raise ValueError("High greater than low test failed on the provided data {0}"
                             .format(dataholder.get_data_lineage()))
        if "Price_outliers_dt" in self.verification_output:
            corrected_data = self.__correct_minute_data_to_daily_reference_price(dataholder.get_data(),
                                                                                 self.aggregated_minute_data.get_data())
            return corrected_data
        return dataholder.get_data()

    def get_flags(self, dh):
        return dh.is_daily_data(), dh.is_data_adjusted()

    def verify_compatibility(self, dh):
        return dh.is_data_adjusted() == self.reference_data_holder.is_data_adjusted()

    def verify_non_negative_values(self, dataframe):
        test = (dataframe.min() < 0).any()
        if test:
            self.verification_output["Negative_price_values"] = True

    def verify_hi_greater_low(self, dataframe):
        """ Verify high price data is greater than low price data. """
        if any(dataframe[HighLabel] < dataframe[LowLabel]):
            self.verification_output["Hi_greater_Lo"] = False

    def verify_close_ratios_minute_adjusted_vs_daily(self, dataframe):
        ref_closes = self.reference_data_holder.get_data()[CloseLabel].round(2)
        closing_time = pd.Timestamp(get_config()["adjustments"]["rth_end"]).time()
        df_closes = dataframe.ix[dataframe.index.tz_convert("US/Eastern").indexer_at_time(closing_time), CloseLabel]
        df_closes.index = df_closes.index.normalize().tz_localize(None)
        ref_close_to_close_returns = ref_closes.pct_change().fillna(0)
        df_close_to_close_returns = df_closes.pct_change().fillna(0)
        abs_return_differences = (ref_close_to_close_returns - df_close_to_close_returns).abs()
        tolerance = get_config()["adjustments"]["verification_parameters"]["minute_outlier"]["outlier_price_limit"]
        significant_discrepancies = not pd.np.allclose(abs_return_differences, 0, atol=2*tolerance)
        if significant_discrepancies:
            self.verification_output["Adjusted_close_ratios_test"] = False

    def verify_data(self, dh):
        """ This function creates the flow for data validations only. """
        self.verification_output = {}
        self.aggregated_minute_data = MinuteDataAggregator()(dh)
        self.identify_outlier_for_price(self.aggregated_minute_data.get_data())
        self.verify_non_negative_values(dh.get_data())
        self.verify_hi_greater_low(dh.get_data())
        self.verify_close_ratios_minute_adjusted_vs_daily(dh.get_data())

    def report_verification_results(self):
        verif_dict = defaultdict(dict)
        """ This function returns status objects with the outcome and result of the verification tests. """

        if "Negative_price_values" in self.verification_output:
            error_msg = "Negative prices identified"
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "NegativePrices",
                                                error_msg, "")
        if "Price_outliers_dt" in self.verification_output:
            warn_msg = "Prices are greater than outlier_price_limit."
            self.add_information_to_report_dict(verif_dict, REPORT_WARNING_LABEL, "PriceOutliers",
                                                warn_msg, len(self.verification_output["Price_outliers_dt"]))

        if "Hi_greater_Lo" in self.verification_output:
            error_msg = "High greater than low test failed"
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "HiGreaterLo",
                                                error_msg, "")
        if "Adjusted_close_ratios_test" in self.verification_output:
            warn_msg = "Close to Close return of tickdata is too different from Yahoo one"
            self.add_information_to_report_dict(verif_dict, REPORT_WARNING_LABEL, "CloseToCloseDifference",
                                                warn_msg, "")
        return verif_dict


class VerifyMinuteOutlierForVolume(DataVerificators):
    def __init__(self, reference_data_holder, do_operation_over_data_flag=True):
        DataVerificators.__init__(self, "->Volume outlier removal", do_operation_over_data_flag)
        self.reference_data_holder = reference_data_holder
        self.aggregated_minute_data = None
        self.verification_output = {}

    def identify_outlier_for_vol(self, dataframe):
        """ Identifies the dates in which the volume difference with reference_data
        is more or less than the median +- outlier_threshold_stds using a 3mo period as lookback window.

        Returns
        -------
           A pd.tseries with the dates for the discrepancies.
        """
        """
        # TODO: Identify better ways to verify for adjustments in volume. Right now will just provide warning
        Because after adjustment the standard deviation of the rolling window is different than before adjustment,
        there can be outliers after outliers were adjusted for. This is because when the std is smaller-since there
        are less outliers- it is easier to be qualified as outlier, and something can now be considered an outlier
        even if it wasn't before
        """
        data_df = dataframe[VolumeLabel]
        differences_df = (data_df / self.reference_data_holder.get_data()[VolumeLabel].reindex(data_df.index) - 1.0)
        differences_df = differences_df.dropna()

        stdev_diff = differences_df.rolling(window=60,center=False).std().fillna(method="bfill")
        mean_diff = differences_df.rolling(window=60,center=False).mean().fillna(method="bfill")

        outlier_threshold_stds = get_config()["adjustments"]["verification_parameters"]["minute_outlier"] \
            ["outlier_threshold_stds"]
        outliers_low = differences_df[differences_df < (mean_diff - (outlier_threshold_stds * stdev_diff))].dropna()
        outliers_hi = differences_df[differences_df > (mean_diff + (outlier_threshold_stds * stdev_diff))].dropna()

        # TODO: Check for outliers_low latter
        self.verification_output["Vol_outliers_dt"] = (1 + mean_diff[outliers_hi.index]) * \
            self.reference_data_holder.get_data()[VolumeLabel][outliers_hi.index]

    def __correct_minute_data_to_daily_reference_volume(self, original_data, agg_original_data, ticker):
        fixed_data = original_data.copy()
        outliers_volume_dates = self.verification_output["Vol_outliers_dt"]
        threshold_vol_reference = get_config()["adjustments"]["verification_parameters"]["minute_outlier"] \
            ["threshold_vol_reference"]
        minute_data_volume = original_data[[VolumeLabel]]
        minute_data_volume["Date_only"] = pd.DatetimeIndex(minute_data_volume.index.date)
        for dt in outliers_volume_dates.index:
            minute_data = minute_data_volume.loc[minute_data_volume['Date_only'] == dt][VolumeLabel]
            R = minute_data.sum()
            if len(minute_data[minute_data != 0]) != 1:
                point_contribution = minute_data / R
                outlier_minute_points = minute_data[point_contribution > threshold_vol_reference]
                # Check if all points exceeding threshold_vol_reference represent 100% of the traded volume
                if pd.np.abs(minute_data[point_contribution <= threshold_vol_reference].sum()) < 1E-8:
                    volume_without_outliers = minute_data
                else:
                    outlier_minute_points = self.__solve_linear_system(outlier_minute_points, R, threshold_vol_reference)
                    volume_without_outliers = outlier_minute_points.reindex(minute_data.index).combine_first(minute_data)
                    volume_without_outliers = pd.np.maximum(volume_without_outliers, 1.0)
            else:
                volume_without_outliers = minute_data
            total_volume = volume_without_outliers.sum()
            target_volume = outliers_volume_dates[dt]
            target_diff = target_volume - total_volume
            volume_without_outliers = pd.np.rint(volume_without_outliers * (1.0 + 1.0 / total_volume * target_diff))
            fixed_data.loc[volume_without_outliers.index[0]:volume_without_outliers.index[-1],
                           VolumeLabel] = volume_without_outliers
            # self.log_report_message(ticker, "VOLUME_ADJUSTMENT", dt, [VolumeLabel], [R, volume_without_outliers.sum()])
        return fixed_data

    def __solve_linear_system(self, outlier_minute_points, R, threshold_vol_reference):
        x = outlier_minute_points.values
        n = len(x)
        matrix_inv = (threshold_vol_reference/(1-n*threshold_vol_reference))*pd.np.ones((n,n)) +pd.np.identity(n)
        adjustments = pd.Series(pd.np.dot(matrix_inv, (x - threshold_vol_reference * R * pd.np.ones(n))),
                                index = outlier_minute_points.index)
        outlier_minute_points -= adjustments
        return outlier_minute_points

    def _make_adjustment(self, dataholder):
        if "Volume_below_zero" in self.verification_output:
            raise ValueError("Negative volumes identified on the provided data {0}"
                             .format(dataholder.get_data_lineage()))

        if "Vol_outliers_dt" in self.verification_output and \
           len(self.verification_output["Vol_outliers_dt"]) != 0:
            corrected_data = self.__correct_minute_data_to_daily_reference_volume(dataholder.get_data(),
                                                                                  self.aggregated_minute_data,
                                                                                  dataholder.get_ticker())
            return corrected_data
        return dataholder.get_data()

    def verify_compatibility(self, dh):
        return dh.is_data_adjusted() == self.reference_data_holder.is_data_adjusted()

    def get_flags(self, dh):
        return dh.is_daily_data(), dh.is_data_adjusted()

    def verify_volume_non_negative(self, dataframe):
        if (dataframe[VolumeLabel] < 0).any():
            self.verification_output["Volume_below_zero"] = True

    def verify_data(self, dh):
        """ This function creates the flow for data validations only. """
        self.verification_output = {}
        self.verify_volume_non_negative(dh.get_data())
        aggregated_minute_data = MinuteDataAggregator()(dh)
        self.aggregated_minute_data = aggregated_minute_data.get_data()
        self.identify_outlier_for_vol(self.aggregated_minute_data)

    def report_verification_results(self):
        """ This function returns status objects with the outcome and result of the verification tests.

        TODO: This code needs to be refactored to return status objects for each test no matter the results.

        """
        verif_dict = defaultdict(dict)
        if "Volume_below_zero" in self.verification_output:
            error_msg = "Records for volume are negative"
            self.add_information_to_report_dict(verif_dict, REPORT_ERROR_LABEL, "NegativeVolumes",
                                                error_msg, "")

        if "Vol_outliers_dt" in self.verification_output:
            """
            #TODO: Identify better ways to verify for adjustments in volume. Right now will just provide warning
            Because after adjustment the standard deviation of the rolling window is different than before adjustment,
            there can be outliers after outliers were adjusted for. This is because when the std is smaller-since there
            are less outliers- it is easier to be qualified as outlier, and something can now be considered an outlier
            even if it wasn't before
            """

            warn_msg = "Dates volume different from the median +- outlier_threshold_stds"
            self.add_information_to_report_dict(verif_dict, REPORT_WARNING_LABEL, "VolOutliers",
                                                warn_msg, len(self.verification_output["Vol_outliers_dt"]))
        return verif_dict
