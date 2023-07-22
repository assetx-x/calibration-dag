import pandas as pd

from operator import ge, eq
from utils import translate_ticker_name
from pipelines.prices_ingestion.equity_data_holders import S3YahooEquityDataHolder
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHolder, S3YahooCorporateActionsHolder
from etl_workflow_steps import BaseETLStep, BaseETLStage, EquitiesETLTaskParameters, StatusType
from pipelines.prices_ingestion.data_retrieval_workflow_steps import DATA_YAHOO_PRICE_LABEL, DATA_YAHOO_CORPORATE_ACTION_LABEL, \
    DATA_TICKDATA_PRICE_HANDLER_LABEL, DATA_TICKDATA_ONEDAY_PRICE_LABEL, TICKDATA_CORPORATE_ACTION_LABEL
from pipelines.prices_ingestion.config import DateLabel, OpenLabel, HighLabel, LowLabel, CloseLabel, VolumeLabel, \
    EquityPriceLabels, TickDataEquityPriceLabels, get_config
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_previous_business_day, get_earliest_possible_data_key, \
    compare_start_date_of_data, verify_against_date, check_df2_subset_of_df1_v2, create_timeline, \
    verify_expected_time_line, compare_end_date_of_data, get_start_or_end_date, verify_no_missing_ca_events

ADJ_CLOSE_LABEL = "Adj Close"

DATA_YAHOO_CORPORATE_ACTION_PREVIOUS_DATE_LABEL = \
    get_config()["etl_equities_process_labels"]["DATA_YAHOO_CORPORATE_ACTION_PREVIOUS_DATE_LABEL"]
DATA_YAHOO_CORPORATE_ACTION_FIRST_DATE_LABEL = \
    get_config()["etl_equities_process_labels"]["DATA_YAHOO_CORPORATE_ACTION_FIRST_DATE_LABEL"]
DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL = \
    get_config()["etl_equities_process_labels"]["DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL"]

TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL = \
    get_config()["etl_equities_process_labels"]["TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL"]
TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL = \
    get_config()["etl_equities_process_labels"]["TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL"]

QA_DATA_TO_CHECK = get_config()["etl_equities_results_labels"]["QA_DATA_TO_CHECK"]
QA_REFERENCE_DATA = get_config()["etl_equities_results_labels"]["QA_REFERENCE_DATA"]
QA_ERROR_LABEL = get_config()["etl_equities_results_labels"]["QA_REFERENCE_DATA"]

REPORT_ERROR_LABEL = get_config()["etl_equities_results_labels"]["REPORT_ERROR_LABEL"]
REPORT_WARNING_LABEL = get_config()["etl_equities_results_labels"]["REPORT_WARNING_LABEL"]
REPORT_DESCRIPTION_LABEL = get_config()["etl_equities_results_labels"]["REPORT_DESCRIPTION_LABEL"]
REPORT_DETAILS_LABEL = get_config()["etl_equities_results_labels"]["REPORT_DETAILS_LABEL"]


class QABaseTask(BaseETLStep):
    def __init__(self, disable_test=False, report_failure_as_warning=False):
        BaseETLStep.__init__(self, disable_test, report_failure_as_warning)
        self.task_params = None

    def add_step_details(self, category, key, description, details):
        if category not in self.step_status:
            self.step_status[category] = {}

        self.step_status[category][key] = {}
        self.step_status[category][key][REPORT_DESCRIPTION_LABEL] = description
        self.step_status[category][key][REPORT_DETAILS_LABEL] = details

    def determine_step_status(self):
        if REPORT_ERROR_LABEL in self.step_status:
            result = StatusType.Fail
        elif REPORT_WARNING_LABEL in self.step_status:
            result = StatusType.Warn
        else:
            result = StatusType.Success
        return result

    def do_step_action(self, **kwargs):
        pass


class PreviousDatesYahooDataRetriever(BaseETLStep):
    REQUIRES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_LABEL]
    PROVIDES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_PREVIOUS_DATE_LABEL, DATA_YAHOO_CORPORATE_ACTION_FIRST_DATE_LABEL,
                       DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL]

    def __init__(self, report_failure_as_warning=True):
        BaseETLStep.__init__(self, report_failure_as_warning=report_failure_as_warning)
        self.yahoo_ca_data_previous = None
        self.yahoo_ca_data_first = None
        self.yahoo_price_data_previous_day = None

    def do_step_action(self, **kwargs):
        # for now re-use the functionality of equity data holder as-is
        ticker = translate_ticker_name(self.task_params.ticker, "unified", "yahoo")
        run_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt
        previous_business_dt = get_previous_business_day(run_date)
        self.yahoo_ca_data_previous = S3YahooCorporateActionsHolder(ticker, run_date=previous_business_dt,
                                                                    as_of_date=as_of_date)

        start_date = self.task_params.start_dt
        corporate_action_holder = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        self.yahoo_price_data_previous_day = S3YahooEquityDataHolder(ticker, corporate_action_holder, start_date,
                                                                     previous_business_dt, as_of_date)

        bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
        data_location_prefix = get_config()["raw_data_pull"]["Yahoo"]["PriceAndCA"]["ca_data_location_prefix"]
        first_possible_date = pd.Timestamp(get_config()["adjustments"]["first_verification_date_on_database"])
        first_ca_date = get_earliest_possible_data_key(bucket_id, data_location_prefix, ticker, first_possible_date,
                                                       run_date, as_of_date)

        self.yahoo_ca_data_first = S3YahooCorporateActionsHolder(ticker, run_date=first_ca_date,
                                                                 as_of_date=as_of_date)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {DATA_YAHOO_CORPORATE_ACTION_PREVIOUS_DATE_LABEL: self.yahoo_ca_data_previous,
                DATA_YAHOO_CORPORATE_ACTION_FIRST_DATE_LABEL: self.yahoo_ca_data_first,
                DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL: self.yahoo_price_data_previous_day}


class PreviousDatesTickdataCADataRetriever(BaseETLStep):
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_LABEL]
    PROVIDES_FIELDS = [TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL, TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.tickdata_ca_data_previous = None
        self.tickdata_ca_data_first = None

    def do_step_action(self, **kwargs):
        status = StatusType.Success
        # for now re-use the functionality of equity data holder as-is
        ticker = translate_ticker_name(self.task_params.ticker, "unified", "tickdata")
        run_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt
        start_dt = self.task_params.start_dt
        end_dt = self.task_params.end_dt

        current_ca = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]

        previous_business_dt = get_previous_business_day(run_date)
        try:
            self.tickdata_ca_data_previous = S3TickDataCorporateActionsHolder(ticker, run_date=previous_business_dt,
                                                                              as_of_date=as_of_date, start_dt=start_dt,
                                                                              end_dt=run_date)
        except RuntimeError:
            self.tickdata_ca_data_previous = current_ca
            status = StatusType.Warn

        bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
        data_location_prefix = get_config()["raw_data_pull"]["TickData"]["CA"]["data_location_prefix"]
        first_possible_date = pd.Timestamp(get_config()["adjustments"]["first_verification_date_on_database"])
        first_ca_date = get_earliest_possible_data_key(bucket_id, data_location_prefix, ticker, first_possible_date,
                                                       run_date, as_of_date)

        self.tickdata_ca_data_first = S3TickDataCorporateActionsHolder(ticker, run_date=first_ca_date,
                                                                       as_of_date=as_of_date, start_dt=start_dt,
                                                                       end_dt=run_date)
        return status

    def _get_additional_step_results(self):
        return {TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL: self.tickdata_ca_data_previous,
                TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL: self.tickdata_ca_data_first}



class YahooCorporateActionsPreviousDayStartDateComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_PREVIOUS_DATE_LABEL, DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        current_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        previous_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_PREVIOUS_DATE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = previous_ca.get_data_lineage()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]
        result = compare_start_date_of_data(df=current_ca.get_ca_data(),
                                            ref=previous_ca.get_ca_data(),
                                            first_date_floor=first_date_floor)
        return StatusType.Success if result else StatusType.Fail


class TickDataCorporateActionsPreviousDayStartDateComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL, TICKDATA_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        current_ca = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]
        previous_ca = kwargs[TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = previous_ca.get_data_lineage()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]

        dividends_result = compare_start_date_of_data(df=current_ca.get_ca_information("DIVIDEND"),
                                                      ref=previous_ca.get_ca_information("DIVIDEND"),
                                                      first_date_floor=first_date_floor,
                                                      df_date_column="EX_DATE",
                                                      ref_date_column="EX_DATE")

        splits_result = compare_start_date_of_data(df=current_ca.get_ca_information("SPLIT"),
                                                   ref=previous_ca.get_ca_information("SPLIT"),
                                                   first_date_floor=first_date_floor,
                                                   df_date_column="EFFECTIVE DATE",
                                                   ref_date_column="EFFECTIVE DATE")

        spinoffs_result = compare_start_date_of_data(df=current_ca.get_ca_information("SPINOFF"),
                                                     ref=previous_ca.get_ca_information("SPINOFF"),
                                                     first_date_floor=first_date_floor,
                                                     df_date_column="EFFECTIVE DATE",
                                                     ref_date_column="EFFECTIVE DATE")

        changes_result = all([compare_start_date_of_data(df=current_ca.get_ca_information(column),
                                                         ref=previous_ca.get_ca_information(column),
                                                         first_date_floor=first_date_floor,
                                                         df_date_column="DATE",
                                                         ref_date_column="DATE")
                              for column in ["CHANGE IN LISTING", "CUSIP CHANGE", "NAME CHANGE", "TICKER SYMBOL CHANGE"]])

        return StatusType.Success if all([dividends_result, splits_result, spinoffs_result, changes_result])\
            else StatusType.Fail


class YahooCorporateActionsPreviousDayContentComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_PREVIOUS_DATE_LABEL, DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        # TODO: Add logging for WARN and NOT Tested Status
        current_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        previous_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_PREVIOUS_DATE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = previous_ca.get_data_lineage()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]
        result = check_df2_subset_of_df1_v2(df2=previous_ca.get_ca_data(),
                                            df1=current_ca.get_ca_data(),
                                            first_date_floor=first_date_floor,
                                            filter_cirteria=["DIVIDEND", "SPLIT"],
                                            filter_column="Action",
                                            numeric_column="Value")
        return StatusType.Success if result else StatusType.Fail


class TickDataCorporateActionsPreviousDayContentComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL, TICKDATA_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        # TODO: Add logging for WARN and NOT Tested Status
        current_ca_holder = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]
        previous_ca_holder = kwargs[TICKDATA_CORPORATE_ACTION_PREVIOUS_DATE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca_holder.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = previous_ca_holder.get_data_lineage()
        current_ca = current_ca_holder.get_ca_data_normalized()
        previous_ca = previous_ca_holder.get_ca_data_normalized()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]

        if first_date_floor:
            first_date_floor = pd.Timestamp(first_date_floor).date()
            # Select only CA after first_date_floor (but include HEADER!!)
            mask = (current_ca.index.get_level_values(1).date >= first_date_floor) | \
                   (current_ca.index.get_level_values(0).isin(['HEADER']))
            current_ca = current_ca[mask]

            # Select only CA after first_date_floor (but include HEADER!!)
            mask = (previous_ca.index.get_level_values(1).date >= first_date_floor) | \
                   (previous_ca.index.get_level_values(0).isin(['HEADER']))
            previous_ca = previous_ca[mask]

        result = verify_no_missing_ca_events(ca_data_recent=current_ca, ca_data_old=previous_ca)
        return StatusType.Success if result else StatusType.Fail


class YahooCorporateActionsFirstDayStartDateComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_FIRST_DATE_LABEL, DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        current_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        first_day_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_FIRST_DATE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = first_day_ca.get_data_lineage()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]
        result = compare_start_date_of_data(df=current_ca.get_ca_data(),
                                            ref=first_day_ca.get_ca_data(),
                                            first_date_floor=first_date_floor)
        return StatusType.Success if result else StatusType.Fail


class TickDataCorporateActionsFirstDayStartDateComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL, TICKDATA_CORPORATE_ACTION_LABEL]

    def __init__(self, disable_test=False, report_failure_as_warning=True):
        super(TickDataCorporateActionsFirstDayStartDateComparisonQA, self).\
            __init__(disable_test, report_failure_as_warning)

    def do_step_action(self,**kwargs):
        current_ca = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]
        first_day_ca =kwargs[TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL]

        self.step_status[QA_DATA_TO_CHECK] = current_ca.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = first_day_ca.get_data_lineage()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]

        # ALI FIX THIS PART
        dividends_result = compare_start_date_of_data(df=current_ca.get_ca_information("DIVIDEND"),
                                                      ref=first_day_ca.get_ca_information("DIVIDEND"),
                                                      first_date_floor=first_date_floor,
                                                      df_date_column="EX_DATE",
                                                      ref_date_column="EX_DATE")

        splits_result = compare_start_date_of_data(df=current_ca.get_ca_information("SPLIT"),
                                                   ref=first_day_ca.get_ca_information("SPLIT"),
                                                   first_date_floor=first_date_floor, df_date_column="EFFECTIVE DATE",
                                                   ref_date_column="EFFECTIVE DATE")

        spinoffs_result = compare_start_date_of_data(df=current_ca.get_ca_information("SPINOFF"),
                                                     ref=first_day_ca.get_ca_information("SPINOFF"),
                                                     first_date_floor=first_date_floor,
                                                     df_date_column="EFFECTIVE DATE", ref_date_column="EFFECTIVE DATE")

        changes_result = all([compare_start_date_of_data(df=current_ca.get_ca_information(column),
                                                         ref=first_day_ca.get_ca_information(column),
                                                         first_date_floor=first_date_floor,
                                                         df_date_column="DATE", ref_date_column="DATE")
                              for column in
                              ["CHANGE IN LISTING", "CUSIP CHANGE", "NAME CHANGE", "TICKER SYMBOL CHANGE"]])

        result = all([dividends_result, splits_result, spinoffs_result, changes_result])
        return StatusType.Success if result else StatusType.Fail


class YahooCorporateActionsFirstDayContentComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_FIRST_DATE_LABEL, DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        # TODO: Adding the logging for WARN and NOT Tested Status
        current_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        first_day_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_FIRST_DATE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = first_day_ca.get_data_lineage()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]
        result = check_df2_subset_of_df1_v2(df2=first_day_ca.get_ca_data(),
                                            df1=current_ca.get_ca_data(),
                                            first_date_floor=first_date_floor,
                                            filter_cirteria=["DIVIDEND", "SPLIT"],
                                            filter_column="Action",
                                            numeric_column="Value")
        return StatusType.Success if result else StatusType.Fail


class TickDataCorporateActionsFirstDayContentComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL, TICKDATA_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        current_ca_holder = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]
        first_day_ca_holder = kwargs[TICKDATA_CORPORATE_ACTION_FIRST_DATE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca_holder.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = first_day_ca_holder.get_data_lineage()
        current_ca = current_ca_holder.get_ca_data_normalized()
        first_day_ca = first_day_ca_holder.get_ca_data_normalized()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]

        if first_date_floor:
            first_date_floor = pd.Timestamp(first_date_floor).date()
            # Select only CA after first_date_floor (but include HEADER!!)
            mask = (current_ca.index.get_level_values(1).date >= first_date_floor) | \
                   (current_ca.index.get_level_values(0).isin(['HEADER']))
            current_ca = current_ca[mask]
            # Select only CA after first_date_floor (but include HEADER!!)
            mask = (first_day_ca.index.get_level_values(1).date >= first_date_floor) | \
                   (first_day_ca.index.get_level_values(0).isin(['HEADER']))
            first_day_ca = first_day_ca[mask]

        result = verify_no_missing_ca_events(ca_data_recent=current_ca, ca_data_old=first_day_ca)
        return StatusType.Success if result else StatusType.Fail


class YahooCorporateActionsRunDateQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        current_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca.get_data_lineage()
        run_date = self.task_params.run_dt
        comparison_operator = ge
        warning_comparison_operator = eq

        result, details = verify_against_date(current_ca.get_ca_data(), run_date,
                                              comparison_operator=comparison_operator,
                                              warning_comparison_operator=warning_comparison_operator)

        if result != 1:
            message = " CA end date Vs run_dt comparison yield invalid results"
            category = REPORT_ERROR_LABEL if result == -1 else REPORT_WARNING_LABEL
            self.add_step_details(category, "CARunDate", message, details)
        return self.determine_step_status()


class TickDataCorporateActionsRunDateQA(QABaseTask):
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_LABEL]

    def do_step_action(self, *args, **kwargs):
        run_date = pd.Timestamp(self.task_params.run_dt).date()
        current_ca = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_ca.get_data_lineage()
        footer_date = pd.Timestamp(current_ca.get_ca_information("FOOTER").index[0]).date()
        return StatusType.Success if footer_date >= run_date else StatusType.Fail


class YahooDailyPriceDataSanityCheckQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_PRICE_LABEL]

    def do_step_action(self, **kwargs):
        current_price = kwargs[DATA_YAHOO_PRICE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_price.get_data_lineage()
        self.__do_sanity_check(current_price.get_data())
        return self.determine_step_status()

    def __do_sanity_check(self, df):
        df_format_check = (self.__check_df_format(df), "DataFormat", "data is not dataframe")
        df_column_names_check = (self.__check_df_column_names(df, EquityPriceLabels), "DataColumnNames",
                                 "data has columns other than {0}. Has {1}".format(
                                 TickDataEquityPriceLabels, df.columns))
        df_non_negative = (self.__check_non_negative_columns(df), "NegativeValues",
                           "data has negative values on at least one column")
        df_no_duplicates = (self.__verify_no_duplicates(df, [DateLabel]), "DuplicatedData",
                            "data has repeated timestamps for dates")
        df_high_low_check = (self.__verify_high_low_labels(df), "HighLowTest", "for some dates concepts are larger "
                                                                               "than hi or smaller than low")
        minimum_price = 0.0
        df_min_price_value_check = (self.__verify_price_values_gt_minimum(df, minimum=minimum_price),
                                    "MinimumPriceCheck", "There are dates where prices are not greater than {0}"
                                    .format(minimum_price))

        all_tests = [df_format_check, df_column_names_check, df_non_negative, df_no_duplicates, df_high_low_check,
                     df_min_price_value_check]

        for test in all_tests:
            if not test[0]:
                self.add_step_details(REPORT_ERROR_LABEL, test[1], test[2], "")

        result = pd.np.all(map(lambda x: x[0], all_tests))
        return result

    @staticmethod
    def __check_df_format(df):
        return isinstance(df, pd.DataFrame)

    @staticmethod
    def __check_df_column_names(df, cols_lables):
        return False if set(df.columns).symmetric_difference(set(cols_lables)) else True

    @staticmethod
    def __check_non_negative_columns(df):
        return (df.min() >= 0).all()

    @staticmethod
    def __verify_no_duplicates(df, columns):
        return not any(df.reset_index().groupby(columns).size() > 1)

    @staticmethod
    def __verify_high_low_labels(df):
        desired_columns = [OpenLabel, HighLabel, LowLabel, CloseLabel]
        high_label_check = (df[desired_columns].max(axis=1) == df[HighLabel]).all()
        low_label_check = (df[desired_columns].min(axis=1) == df[LowLabel]).all()
        return high_label_check and low_label_check

    @staticmethod
    def __verify_price_values_gt_minimum(df, minimum=0.0):
        desired_columns = [OpenLabel, HighLabel, LowLabel, CloseLabel]
        min_prices_violated = (df[desired_columns].min() <= minimum).any()
        return not min_prices_violated


class YahooDailyPriceVerifyTimeLine(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_PRICE_LABEL]

    def do_step_action(self, **kwargs):
        current_price = kwargs[DATA_YAHOO_PRICE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_price.get_data_lineage()
        price_data = current_price.get_data()
        start_date = get_start_or_end_date(price_data, "start_date")
        end_date = get_start_or_end_date(price_data, "end_date")
        time_index, date_timeline = create_timeline(start_date, end_date, calculate_full_timeline=False)
        timeline_difference = verify_expected_time_line(price_data, pd.Series(date_timeline))

        if len(timeline_difference) > 0:
            self.add_step_details(REPORT_ERROR_LABEL, "TimelineVerification", "There are holes on yahoo price data",
                                  timeline_difference)
        return self.determine_step_status()


class YahooDailyPricesDataPreviousDayComparisonQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL, DATA_YAHOO_PRICE_LABEL]

    def do_step_action(self, **kwargs):
        current_price = kwargs[DATA_YAHOO_PRICE_LABEL]
        previous_price = kwargs[DATA_YAHOO_PRICE_PREVIOUS_DATE_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_price.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = previous_price.get_data_lineage()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]
        price_labels = ["Open", "Close", "Low", "High"]
        result_price = check_df2_subset_of_df1_v2(previous_price.get_data()[price_labels],
                                                  current_price.get_data()[price_labels],
                                                  first_date_floor=first_date_floor)
        result_volume = check_df2_subset_of_df1_v2(previous_price.get_data()[[VolumeLabel]],
                                                   current_price.get_data()[[VolumeLabel]],
                                                   first_date_floor=first_date_floor)
        start_date_comparison = compare_start_date_of_data(current_price.get_data()[[VolumeLabel]],
                                                           previous_price.get_data()[price_labels],
                                                           first_date_floor=first_date_floor)
        if not result_price:
            self.add_step_details(REPORT_ERROR_LABEL, "PriceDifferences", "Previous day data has different values than "
                                  "current data", "")
        if not result_volume:
            self.add_step_details(REPORT_WARNING_LABEL, "VolumeDifferences", "Previous day data has different volume "
                                  "values than current data", "")
        if not start_date_comparison:
            self.add_step_details(REPORT_ERROR_LABEL, "StartDateDifference",
                                  "Previous day data has different start date than current data", "")
        return self.determine_step_status()


class YahooPriceAndCorporateActionsDateComparison(QABaseTask):
    REQUIRES_FIELDS = [DATA_YAHOO_PRICE_LABEL, DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def do_step_action(self, **kwargs):
        current_price = kwargs[DATA_YAHOO_PRICE_LABEL]
        current_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        self.step_status[QA_DATA_TO_CHECK] = current_price.get_data_lineage()
        self.step_status[QA_REFERENCE_DATA] = current_ca.get_data_lineage()
        first_date_floor = get_config()["adjustments"]["first_relevant_date_for_all_tests"]
        start_date_comparison = compare_start_date_of_data(current_price.get_data(), current_ca.get_ca_data(),
                                                           first_date_floor)
        end_date_comparison, details_end_dt = compare_end_date_of_data(current_price.get_data(),
                                                                       current_ca.get_ca_data())
        details_start_dt = {"first_date_floor": first_date_floor}
        if not start_date_comparison:
            self.add_step_details(REPORT_ERROR_LABEL, "CA_Price_start_dt_comp",
                                  "The start date of CA and Yahoo Price data don't match", details_start_dt)
        if end_date_comparison.value >= StatusType.Warn.value:
            category = REPORT_ERROR_LABEL if end_date_comparison is StatusType.Fail else REPORT_WARNING_LABEL
            self.add_step_details(category, "CA_Price_end_dt_comp", "The end date of CA and Yahoo Price data"
                                  " don't match", details_end_dt)
        return self.determine_step_status()


class TickDataSanityCheckQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_TICKDATA_ONEDAY_PRICE_LABEL]

    def do_step_action(self, **kwargs):
        current_price = kwargs[DATA_TICKDATA_ONEDAY_PRICE_LABEL].get_data_holder()
        self.step_status[QA_DATA_TO_CHECK] = current_price.get_data_lineage()
        self.__do_sanity_check(current_price.get_data())
        return self.determine_step_status()

    def __do_sanity_check(self, df):
        df_format_check = (self.__check_df_format(df), "DataFormat", "data is not dataframe")
        df_column_names_check = (self.__check_df_column_names(df, EquityPriceLabels), "DataColumnNames",
                                 "data has columns other than {0}. Has {1}".format(EquityPriceLabels, df.columns))
        df_non_negative = (self.__check_non_negative_columns(df), "NegativeValues",
                           "data has negative values on at least one column")
        df_no_duplicates = (self.__verify_no_duplicates(df, [DateLabel]), "DuplicatedData",
                            "data has repeated timestamps for dates")
        df_high_low_check = (self.__verify_high_low_labels(df), "HighLowTest",
                             "for some dates concepts are larger than hi or smaller than low")
        minimum_price = 0.0
        df_min_price_value_check = (self.__verify_price_values_gt_minimum(df, minimum=minimum_price),
                                    "MinimumPriceCheck", "There are dates where prices are not greater than {0}"
                                    .format(minimum_price))
        all_tests = [df_format_check, df_column_names_check, df_non_negative, df_no_duplicates,
                     df_high_low_check, df_min_price_value_check]
        for test in all_tests:
            if not test[0]:
                self.add_step_details(REPORT_ERROR_LABEL, test[1], test[2], "")
        result = pd.np.all(map(lambda x: x[0], all_tests))
        return result

    @staticmethod
    def __check_df_format(df):
        return isinstance(df, pd.DataFrame)

    @staticmethod
    def __check_df_column_names(df, cols_lables):
        return False if set(df.columns).symmetric_difference(set(cols_lables)) else True

    @staticmethod
    def __check_non_negative_columns(df):
        df = df[EquityPriceLabels]
        return (df.min() >= 0).all()

    @staticmethod
    def __verify_no_duplicates(df, columns):
        return not any(df.reset_index().groupby(columns).size() > 1)

    @staticmethod
    def __verify_high_low_labels(df):
        desired_columns = [OpenLabel, HighLabel, LowLabel, CloseLabel]
        high_label_check = (df[desired_columns].max(axis=1) == df[HighLabel]).all()
        low_label_check = (df[desired_columns].min(axis=1) == df[LowLabel]).all()
        return high_label_check and low_label_check

    @staticmethod
    def __verify_price_values_gt_minimum(df, minimum=0.0):
        desired_columns = [OpenLabel, HighLabel, LowLabel, CloseLabel]
        min_prices_violated = (df[desired_columns].min() <= minimum).any()
        return not min_prices_violated


class TickdataFullHistorySanityCheckQA(QABaseTask):
    REQUIRES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL]

    def do_step_action(self, **kwargs):
        full_raw_minute_data = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()
        self.step_status[QA_DATA_TO_CHECK] = full_raw_minute_data.get_data_lineage()
        self.__do_sanity_check(full_raw_minute_data.get_data())

        return self.determine_step_status()

    def __do_sanity_check(self, df):
        df_format_check = (self.__check_df_format(df), "DataFormat", "data is not dataframe")
        df_column_names_check = (self.__check_df_column_names(df, EquityPriceLabels), "DataColumnNames",
                                 "data has columns other than {0}. Has {1}".format(
                                 TickDataEquityPriceLabels, df.columns))
        df_non_negative = (self.__check_non_negative_columns(df), "NegativeValues",
                           "data has negative values on at least one column")
        df_no_duplicates = (self.__verify_no_duplicates(df, [DateLabel]), "DuplicatedData",
                            "data has repeated timestamps for dates")
        df_high_low_check = (self.__verify_high_low_labels(df), "HighLowTest",
                             "for some dates concepts are larger than hi or smaller than low")
        minimum_price = 0.0
        df_min_price_value_check = (self.__verify_price_values_gt_minimum(df, minimum=minimum_price),
                                    "MinimumPriceCheck", "There are dates where prices are not greater than {0}"
                                    .format(minimum_price))
        all_tests = [df_format_check, df_column_names_check, df_non_negative, df_no_duplicates, df_high_low_check,
                     df_min_price_value_check]

        for test in all_tests:
            if not test[0][0]:
                self.add_step_details(REPORT_ERROR_LABEL, test[1], test[2], test[0][1])

        result = pd.np.all(map(lambda x: x[0][0], all_tests))
        return result

    # TODO: probably these function should be consolidated with YahooDailyPriceDataSanityCheckQA. Could create a
    # parent class for both; if oneDayMinuteData also does the same, definitely let's consolidate it
    @staticmethod
    def __check_df_format(df):
        return isinstance(df, pd.DataFrame), ""

    @staticmethod
    def __check_df_column_names(df, cols_lables):
        difference = set(df.columns).symmetric_difference(set(cols_lables))
        result = False if difference else True
        return result, str(list(difference))

    @staticmethod
    def __check_non_negative_columns(df):
        check = df.min() >= 0
        msg = "Problem columns are {0}".format(df.columns[~check].tolist())
        return check.all(), msg

    @staticmethod
    def __verify_no_duplicates(df, columns):
        duplicates_violated = any(df.reset_index().groupby(columns).size() > 1)
        return not duplicates_violated, ""

    @staticmethod
    def __verify_high_low_labels(df):
        desired_columns = [OpenLabel, HighLabel, LowLabel, CloseLabel]
        high_label_check = (df[desired_columns].max(axis=1) == df[HighLabel]).all()
        low_label_check = (df[desired_columns].min(axis=1) == df[LowLabel]).all()
        return (high_label_check and low_label_check), ""

    @staticmethod
    def __verify_price_values_gt_minimum(df, minimum=0.0):
        desired_columns = [OpenLabel, HighLabel, LowLabel, CloseLabel]
        min_prices_violated = (df[desired_columns].min() <= minimum).any()
        return not min_prices_violated, ""


def create_corporate_actions_qa_substage():
    additional_data_ca_puller = PreviousDatesTickdataCADataRetriever()
    ca_qa_prev_day_comp = TickDataCorporateActionsPreviousDayStartDateComparisonQA()
    ca_qa_first_day_comp = TickDataCorporateActionsFirstDayStartDateComparisonQA()
    ca_qa_run_dt_comp = TickDataCorporateActionsRunDateQA()
    ca_qa_prev_day_details_comp = TickDataCorporateActionsPreviousDayContentComparisonQA(report_failure_as_warning=True)
    ca_qa_first_day_details_comp = TickDataCorporateActionsFirstDayContentComparisonQA(report_failure_as_warning=True)
    ca_qa_stage = BaseETLStage("CorpActionsQA", "Sub-Stage", additional_data_ca_puller, ca_qa_prev_day_comp,
                               ca_qa_first_day_comp,
                               ca_qa_run_dt_comp,
                               ca_qa_prev_day_details_comp,
                               ca_qa_first_day_details_comp)
    return ca_qa_stage


def create_yahoo_price_qa_substage():
    price_qa_sanity_check = YahooDailyPriceDataSanityCheckQA(report_failure_as_warning=True)
    price_qa_prev_day_details_comp = YahooDailyPricesDataPreviousDayComparisonQA(report_failure_as_warning=True)
    price_qa_verify_timeline = YahooDailyPriceVerifyTimeLine(report_failure_as_warning=True)
    price_qa_compare_with_ca = YahooPriceAndCorporateActionsDateComparison(report_failure_as_warning=True)
    price_qa_stage = BaseETLStage("PriceQA", "Sub-Stage", price_qa_sanity_check, price_qa_prev_day_details_comp,
                                  price_qa_verify_timeline, price_qa_compare_with_ca)
    return price_qa_stage


def create_full_tickdata_qa_substage():
    tickdata_basic_sanity_check = TickdataFullHistorySanityCheckQA()
    redshift_full_qa_stage = BaseETLStage("RedshiftRawTickData", "Sub-Stage", tickdata_basic_sanity_check)
    return redshift_full_qa_stage


def create_additional_data_pull_substage():
    additional_ca_retriever = PreviousDatesYahooDataRetriever(report_failure_as_warning=True)
    data_pull_stage = BaseETLStage("AdditionalDataPull", "Sub-Stage", additional_ca_retriever)
    return data_pull_stage


def create_tickdata_one_day_price_substage():
    price_qa_sanity_check = TickDataSanityCheckQA()
    tickdata_price_qa_stage = BaseETLStage("TickDataQA", "Sub-Stage", price_qa_sanity_check)
    return tickdata_price_qa_stage


def create_tickdata_full_history_price_substage():
    price_qa_sanity_check = TickdataFullHistorySanityCheckQA()
    tickdata_price_qa_stage = BaseETLStage("TickdataFullHistorySanityCheckQA", "Sub-Stage", price_qa_sanity_check)
    return tickdata_price_qa_stage


def create_qa_stage(full_adjustment=False):
    # data_pull = create_additional_data_pull_substage()
    ca_qa = create_corporate_actions_qa_substage()
    # price_qa = create_yahoo_price_qa_substage()
    tickdata_one_day_qa = create_tickdata_one_day_price_substage()
    full_qa = BaseETLStage("QA", "Stage", ca_qa, tickdata_one_day_qa)

    if full_adjustment:
        full_raw_tickdata = create_full_tickdata_qa_substage()
        full_qa.add(full_raw_tickdata)
    return full_qa


def create_qa_stage_partial(task_name):
    task = eval(task_name+"()")
    qa_stage = BaseETLStage(task_name, "Sub-Stage", task)
    return qa_stage


if __name__ == "__main__":
    import os
    import pandas as pd
    import _pickle as pickle
    from etl_workflow_steps import compose_main_flow_and_engine_for_task

    # ticker = "AAPL"  # general example
    # as_of_dt = pd.Timestamp("2017-07-09")
    # run_dt = pd.Timestamp("2017-07-07")
    # start_dt = pd.Timestamp("2000-01-03")
    # end_dt = pd.Timestamp("2017-07-07")

    # ticker = "NWL"  # has CHANGE IN LISTING on 12/11/2018 for the first time
    # as_of_dt = pd.Timestamp("2018-12-13")
    # run_dt = pd.Timestamp("2018-12-11")
    # start_dt = pd.Timestamp("2018-12-10")
    # end_dt = pd.Timestamp("2018-12-11")

    # ticker = "TARO"  # "BBX" # has DIVIDEND on 12/10/2018 for the first time
    # as_of_dt = pd.Timestamp("2018-12-12")
    # run_dt = pd.Timestamp("2018-12-10")
    # start_dt = pd.Timestamp("2018-12-07")
    # end_dt = pd.Timestamp("2018-12-10")

    ticker = "IRET"  # has both CUSIP CHANGE and SPLIT on 12/28/2018 for the first time
    as_of_dt = pd.Timestamp("2018-12-30")
    run_dt = pd.Timestamp("2018-12-28")
    start_dt = pd.Timestamp("2018-12-26")
    end_dt = pd.Timestamp("2018-12-28")

    # ticker = "ERX"  # EGHT - missing cusip changes (first/last CA file); ERX - flipped same day dividend records
    # as_of_dt = pd.Timestamp("2019-02-10")
    # run_dt = pd.Timestamp("2019-02-08")
    # start_dt = pd.Timestamp("2018-12-26")
    # end_dt = pd.Timestamp("2019-02-08")

    # ticker = "ESIO"
    # as_of_dt = pd.Timestamp("2019-02-13")
    # run_dt = pd.Timestamp("2019-02-11")
    # start_dt = pd.Timestamp("2018-12-26")
    # end_dt = pd.Timestamp("2019-02-11")

    # ticker = "SPXN"  # "SPXE", "EMSH", "MRGR" # has two DIVIDENDs on 12/26/2018
    # as_of_dt = pd.Timestamp("2018-12-30")
    # run_dt = pd.Timestamp("2018-12-28")
    # start_dt = pd.Timestamp("2018-12-26")
    # end_dt = pd.Timestamp("2018-12-28")

    path_dcm_raw_data = r"C:\DCM\temp"
    full_path_dcm_raw = os.path.join(path_dcm_raw_data, '%s_data.pickle' % ticker)

    if not os.path.exists(full_path_dcm_raw):
        from data_retrieval_workflow_steps import create_data_retrieval_stage

        task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=as_of_dt,
                                                run_dt=run_dt, start_dt=start_dt,
                                                end_dt=end_dt)
        data_retrieval_stage = create_data_retrieval_stage(True)
        engine, main_flow = compose_main_flow_and_engine_for_task("Data_Retrieval_Sample", task_params,
                                                                  data_retrieval_stage)
        engine.run(main_flow)
        data_retrieval_results = engine.storage.fetch_all()
        #pickle.dump(data_retrieval_results, open(full_path_dcm_raw, "wb"))
    else:
        data_retrieval_results = pickle.load(open(full_path_dcm_raw, "rb"))

    # create engine and task
    task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=as_of_dt, run_dt=run_dt,
                                            start_dt=start_dt, end_dt=end_dt)

    # ca_qa = create_qa_stage()
    # ca_qa = create_qa_stage(True)
    # qa = create_qa_stage_partial("TickdataFullHistorySanityCheckQA")
    qa = create_corporate_actions_qa_substage()
    engine, main_flow = compose_main_flow_and_engine_for_task("Data_QA_Sample", task_params, qa,
                                                              store=data_retrieval_results)
    # run flow and get results
    engine.run(main_flow)
    results = engine.storage.fetch_all()
    step_results = filter(None, results["step_results"])
    for k in step_results:
        if isinstance(k, list):
            for l in k:
                print(l["status_type"])
        else:
            print(k["status_type"])
    print('Done!!!')
