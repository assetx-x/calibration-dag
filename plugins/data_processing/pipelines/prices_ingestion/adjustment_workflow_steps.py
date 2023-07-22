import gc
import pandas as pd
from io import StringIO
from etl_workflow_steps import BaseETLStep, BaseETLStage, EquitiesETLTaskParameters, StatusType
from pipelines.prices_ingestion.equity_data_holders import RedshiftRawTickDataDataHolder
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHolder, S3TickDataCorporateActionsHandler
from pipelines.prices_ingestion.equity_data_operations import VerifyTimeline, CompleteMinutes, VerifyMinuteOutlierForPrice, \
    VerifyMinuteOutlierForVolume, S3YahooPriceAdjuster, SplitAndDividendEquityAdjuster, \
    VerifyTimelineForTickDataPricesAndCA
from pipelines.prices_ingestion.etl_workflow_aux_functions import store_data_in_s3, build_s3_url
from pipelines.prices_ingestion.data_retrieval_workflow_steps import DATA_YAHOO_PRICE_LABEL, DATA_TICKDATA_PRICE_HANDLER_LABEL,\
    DATA_YAHOO_CORPORATE_ACTION_LABEL, TICKDATA_CORPORATE_ACTION_LABEL
from utils import translate_ticker_name
from pipelines.prices_ingestion.config import get_config, DateLabel, OpenLabel, HighLabel, LowLabel, CloseLabel, \
    VolumeLabel, EquityPriceLabels


LOCATION_S3_ADJUSTED_DATA = get_config()["etl_equities_process_labels"]["LOCATION_S3_ADJUSTED_DATA"]
LOCATION_S3_ADJUSTED_DAILY_DATA = get_config()["etl_equities_process_labels"]["LOCATION_S3_ADJUSTED_DAILY_DATA"]
S3_ADJ_FACTORS_LOCATION_LABEL = get_config()["etl_equities_process_labels"]["S3_ADJ_FACTORS_LOCATION_LABEL"]

S3_LOCATION_LABEL = get_config()["etl_equities_results_labels"]["S3_LOCATION_LABEL"]


REPORT_ERROR_LABEL = get_config()["etl_equities_results_labels"]["REPORT_ERROR_LABEL"]
REPORT_WARNING_LABEL = get_config()["etl_equities_results_labels"]["REPORT_WARNING_LABEL"]


def _add_symbol_asof_and_companyid_columns(ca_data_normalized, adjusted_data):
    # Getting ticker symbol change history
    ticker_history = S3TickDataCorporateActionsHandler.build_ticker_name_change_history(ca_data_normalized)

    adjusted_data["symbol_asof"] = pd.np.NaN
    # NG - To get reviewed this change for empty dataframe here
    if not adjusted_data.empty:
        adjusted_data_index_norm = adjusted_data.index.normalize().tz_localize(None)
         # Selecting tickers' symbols used only in a timeline of the adjusted prices
        utc_start = adjusted_data_index_norm[0] # first entry
        ticker_history = ticker_history.loc[(ticker_history['as_of_end'] >= utc_start) | \
                                            ticker_history['as_of_end'].isnull()].reset_index(drop=True)

        for index, value in ticker_history.iterrows():
            mask = (adjusted_data_index_norm >= value['as_of_start']) & \
                ((adjusted_data_index_norm <= value['as_of_end']) | pd.isnull(value['as_of_end']))
            adjusted_data.loc[mask, "symbol_asof"] = value['Symbol']
        del adjusted_data_index_norm, mask; gc.collect()
    adjusted_data["dcm_security_id"] = ticker_history["CompanyID"][0]
    return adjusted_data


class AdjustmentStepBaseClass(BaseETLStep):
    REQUIRES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL]

    def do_step_action(self, **kwargs):
        pass

    def __init__(self, verification_only=False):
        self.verification_only = verification_only
        step_suffix = "correction" if not verification_only else "verification"
        BaseETLStep.__init__(self, name="{0}.{1}".format(self.__class__.__name__, step_suffix))

    def get_result_status(self, operation_result, **kwargs):
        status = StatusType.Success

        if not self.verification_only:
            kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].set_data_holder(operation_result)
        else:
            if operation_result:
                self.step_status.update(operation_result)

                if REPORT_ERROR_LABEL in operation_result:
                    status = StatusType.Fail
                elif REPORT_WARNING_LABEL in operation_result:
                    status = StatusType.Warn
        return status


class TickdataTimelineAdjustment(AdjustmentStepBaseClass):
    REQUIRES_FIELDS = ["retrieved_from", DATA_TICKDATA_PRICE_HANDLER_LABEL, TICKDATA_CORPORATE_ACTION_LABEL]

    def __init__(self, full_timeline, verification_only=False):
        AdjustmentStepBaseClass.__init__(self, verification_only)
        self.full_timeline = full_timeline

    def do_step_action(self, **kwargs):
        start_dt = self.task_params.start_dt
        end_dt = self.task_params.end_dt

        cutoff_dt = pd.Timestamp(get_config()["adjustments"]["verification_parameters"]["timeline_verification"]\
                                 ["cut_off_date_for_missing_data"]).date()
        if not self.full_timeline:
            start_dt = kwargs["retrieved_from"]
            print("Crop from %s" % start_dt)

        equity_data = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()
        ca_data = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]
        data_to_operate = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()

        timeline_checker = VerifyTimelineForTickDataPricesAndCA(equity_data, ca_data, cutoff_dt,
                                                                start_dt, end_dt, not self.verification_only)
        operation_result = timeline_checker(data_to_operate)
        status = self.get_result_status(operation_result, **kwargs)
        return status


class VerifyEquityDataTimelineAdjustment(AdjustmentStepBaseClass):
    REQUIRES_FIELDS = ["retrieved_from", DATA_YAHOO_PRICE_LABEL, DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def __init__(self, full_timeline, verification_only=False):
        AdjustmentStepBaseClass.__init__(self, verification_only)
        self.full_timeline = full_timeline

    def do_step_action(self, **kwargs):
        start_dt = self.task_params.start_dt
        end_dt = self.task_params.end_dt

        cutoff_dt = pd.Timestamp(get_config()["adjustments"]["verification_parameters"]["timeline_verification"]
                                 ["cut_off_date_for_missing_data"]).date()

        if not self.full_timeline:
            start_dt = kwargs["retrieved_from"]
            print("Crop from %s" % start_dt)

        yahoo_data = kwargs[DATA_YAHOO_PRICE_LABEL]
        yahoo_ca = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        data_to_operate = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()

        timeline_checker = VerifyTimeline(yahoo_data, yahoo_ca, cutoff_dt, start_dt, end_dt, not self.verification_only)
        operation_result = timeline_checker(data_to_operate)
        status = self.get_result_status(operation_result, **kwargs)
        return status


class CompleteEquityDataMinutesAdjustment(AdjustmentStepBaseClass):
    REQUIRES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL, TICKDATA_CORPORATE_ACTION_LABEL]

    def __init__(self, verification_only=False):
        AdjustmentStepBaseClass.__init__(self, verification_only)

    def do_step_action(self, **kwargs):
        start_dt = self.task_params.start_dt
        end_dt = self.task_params.end_dt

        data_to_operate = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()
        corp_actions = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]

        first_ca_date = pd.Timestamp(corp_actions.get_ca_information("HEADER").index[0])
        last_ca_date = pd.Timestamp(corp_actions.get_ca_information("FOOTER").index[0])
        start_dt = max(start_dt, first_ca_date)
        end_dt = min(end_dt, last_ca_date)

        timeline_completer = CompleteMinutes(not self.verification_only, start_date=start_dt, end_date=end_dt)
        operation_result = timeline_completer(data_to_operate)
        status = self.get_result_status(operation_result, **kwargs)
        return status


class OutlierRemovalForPriceAdjustment(AdjustmentStepBaseClass):
    REQUIRES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL, DATA_YAHOO_PRICE_LABEL]

    def __init__(self, verification_only=False):
        AdjustmentStepBaseClass.__init__(self, verification_only)

    def do_step_action(self, **kwargs):
        data_to_operate = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()
        yahoo_data = kwargs[DATA_YAHOO_PRICE_LABEL]
        price_outlier_removal = VerifyMinuteOutlierForPrice(yahoo_data, not self.verification_only)
        operation_result = price_outlier_removal(data_to_operate)
        status = self.get_result_status(operation_result, **kwargs)
        return status


class OutlierRemovalForVolumeAdjustment(AdjustmentStepBaseClass):
    REQUIRES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL, DATA_YAHOO_PRICE_LABEL]

    def __init__(self, verification_only=False):
        AdjustmentStepBaseClass.__init__(self, verification_only)

    def do_step_action(self, **kwargs):
        data_to_operate = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()
        yahoo_data = kwargs[DATA_YAHOO_PRICE_LABEL]
        volume_outlier_removal = VerifyMinuteOutlierForVolume(yahoo_data, not self.verification_only)
        operation_result = volume_outlier_removal(data_to_operate)
        status = self.get_result_status(operation_result, **kwargs)
        return status


class AdjustDataForCorporateActions(AdjustmentStepBaseClass):
    REQUIRES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL, DATA_YAHOO_PRICE_LABEL]

    def __init__(self):
        AdjustmentStepBaseClass.__init__(self)

    def do_step_action(self, **kwargs):
        data_to_operate = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()
        yahoo_data = kwargs[DATA_YAHOO_PRICE_LABEL]
        adjuster = S3YahooPriceAdjuster(yahoo_data)
        operation_result = adjuster(data_to_operate)
        status = self.get_result_status(operation_result, **kwargs)
        return status


class AdjustDataForTickDataCorporateActions(AdjustmentStepBaseClass):
    REQUIRES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL, TICKDATA_CORPORATE_ACTION_LABEL]

    def __init__(self):
        AdjustmentStepBaseClass.__init__(self)

    def do_step_action(self, **kwargs):
        data_to_operate = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder()
        ca_data = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]
        adjuster = SplitAndDividendEquityAdjuster(ca_data.get_full_adjustment_factor(),
                                                  ca_data.get_split_adjustment_factor())
        operation_result = adjuster(data_to_operate)
        status = self.get_result_status(operation_result, **kwargs)
        return status


class FullAdjustmentVerifier(AdjustmentStepBaseClass):
    REQUIRES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL]

    def __init__(self):
        AdjustmentStepBaseClass.__init__(self)

    def do_step_action(self, **kwargs):
        # Re-pull raw data
        ticker = self.task_params.ticker
        run_date = self.task_params.run_dt
        start_dt = self.task_params.start_dt
        end_dt = self.task_params.end_dt
        as_of_date = self.task_params.as_of_dt
        raw_data = RedshiftRawTickDataDataHolder(ticker=translate_ticker_name(ticker, "unified", "tickdata"),
                                                 end_date=end_dt, as_of_date=as_of_date).get_data()

        # Get first day
        adjusted_data = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder().get_data()
        first_day = min(adjusted_data.index).normalize()

        # Re-pull CA holder
        start_dt = first_day
        ca_data = S3TickDataCorporateActionsHolder(ticker, run_date=run_date, as_of_date=as_of_date, start_dt=start_dt,
                                                   end_dt=end_dt).get_ca_data_normalized()
        # Look only since first_day (to account for e.g. change in listings or revivals)
        ca_data = ca_data[ca_data.index.get_level_values(1).tz_localize('UTC') >= first_day]
        ca_data = ca_data.sort_index()
        ca_data = ca_data.loc[(["DIVIDEND", "SPLIT", "SPINOFF"], slice(None)), :]
        num_ca = len(ca_data)
        if num_ca:
            # There are relevant corporate actions requiring adjustment
            if (num_ca == 1) & (ca_data.index.get_level_values(1)[0].date() == first_day.date()):
                # This is a corner case where there is 1 corporate action that coincides with the first_day in which
                # the adjusted and unadjusted prices should be the same
                return StatusType.Success
            else:
                price_threshold = get_config()["adjustments"]["price_adjustment_verification_threshold"]
                adjusted_data = adjusted_data.sort_index().groupby(adjusted_data.index.date)\
                    .agg({VolumeLabel:"sum", OpenLabel:"first", CloseLabel:"last", HighLabel:"max", LowLabel:"min"}) \
                    [[OpenLabel, CloseLabel, HighLabel, LowLabel]]
                raw_data = raw_data.sort_index().groupby(raw_data.index.date)\
                    .agg({VolumeLabel:"sum", OpenLabel:"first", CloseLabel:"last", HighLabel:"max", LowLabel:"min"}) \
                    [[OpenLabel, CloseLabel, HighLabel, LowLabel]]
                min_discrepancy = abs(raw_data.loc[first_day.date()] - adjusted_data.loc[first_day.date()]).min()
                if min_discrepancy < price_threshold:
                    # Price is unadjusted
                    raise ValueError("Despite the existence of corporate actions in date range price was not properly "
                                     "adjusted for {0}".format(ticker))
        return StatusType.Success


class DepositAdjustedDataIntoS3(BaseETLStep):
    PROVIDES_FIELDS = [LOCATION_S3_ADJUSTED_DATA]
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_LABEL, DATA_TICKDATA_PRICE_HANDLER_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.data_s3_key = None

    def _get_additional_step_results(self):
        return {LOCATION_S3_ADJUSTED_DATA: self.data_s3_key}

    def do_step_action(self, **kwargs):
        ticker = self.task_params.ticker
        run_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt

        # Getting final data
        ca_data_normalized = kwargs[TICKDATA_CORPORATE_ACTION_LABEL].get_ca_data_normalized()
        final_adjusted_data = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder().get_data()
        final_adjusted_data = final_adjusted_data[EquityPriceLabels]
        final_adjusted_data = final_adjusted_data.loc[final_adjusted_data.first_valid_index():, :].copy(deep=True)

        # Adding additional columns
        final_adjusted_data["as_of_start_date"] = str(pd.Timestamp.now("UTC").to_pydatetime()).rsplit(".", 1)[0]
        final_adjusted_data["as_of_end_date"] = pd.np.NaN
        final_adjusted_data["Ticker"] = ticker
        final_adjusted_data = _add_symbol_asof_and_companyid_columns(ca_data_normalized, final_adjusted_data)
        final_adjusted_data = final_adjusted_data.reset_index().rename(columns={DateLabel: "cob_date"})

        csv_buffer = StringIO()
        final_adjusted_data.to_csv(csv_buffer, index=False)
        data_s3_key = store_data_in_s3(csv_buffer, get_config()["raw_data_pull"]["base_data_bucket"],
                                       get_config()["adjustments"]["data_prefix"],
                                       ticker, run_date)
        self.data_s3_key = data_s3_key
        self.step_status[S3_LOCATION_LABEL] = build_s3_url(*data_s3_key)
        return StatusType.Success


class DepositDailyAdjustedDataIntoS3(BaseETLStep):
    PROVIDES_FIELDS = [LOCATION_S3_ADJUSTED_DAILY_DATA]
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_LABEL, DATA_TICKDATA_PRICE_HANDLER_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.data_s3_key = None

    def _get_additional_step_results(self):
        return {LOCATION_S3_ADJUSTED_DAILY_DATA: self.data_s3_key}

    def do_step_action(self, **kwargs):
        ticker = self.task_params.ticker
        run_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt

        # Getting final data
        ca_data_normalized = kwargs[TICKDATA_CORPORATE_ACTION_LABEL].get_ca_data_normalized()
        final_adjusted_data = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder().get_data()
        final_adjusted_data = final_adjusted_data[EquityPriceLabels]
        final_adjusted_data = final_adjusted_data.loc[final_adjusted_data.first_valid_index():, :].copy(deep=True)

        final_adjusted_data.index = final_adjusted_data.index.tz_convert("US/Eastern")
        aggfunc = {VolumeLabel: "sum", OpenLabel: "first", CloseLabel: "last", HighLabel: "max", LowLabel: "min"}
        final_adjusted_data = final_adjusted_data.groupby(final_adjusted_data.index.normalize()).agg(aggfunc)
        final_adjusted_data.index = final_adjusted_data.index + pd.Timedelta("16:00:00")
        final_adjusted_data.index = final_adjusted_data.index.tz_convert("UTC").tz_localize(None)
        final_adjusted_data.index.name = DateLabel

        # Adding additional columns
        final_adjusted_data["Ticker"] = ticker
        final_adjusted_data["as_of_start_date"] = str(pd.Timestamp.now("UTC").to_pydatetime()).rsplit(".", 1)[0]
        final_adjusted_data["as_of_end_date"] = pd.np.NaN
        final_adjusted_data = _add_symbol_asof_and_companyid_columns(ca_data_normalized, final_adjusted_data)
        final_adjusted_data = final_adjusted_data.reset_index().rename(columns={DateLabel: "cob_date"})

        # This re-organization is to match table definition in Redshift and to be able to copy the file properly
        final_adjusted_data = final_adjusted_data[["Ticker", "cob_date", OpenLabel, CloseLabel, HighLabel, LowLabel,
                                                   VolumeLabel, "as_of_start_date", "as_of_end_date",
                                                   "symbol_asof", "dcm_security_id"]]

        csv_buffer = StringIO()
        final_adjusted_data.to_csv(csv_buffer, index=False)
        data_s3_key = store_data_in_s3(csv_buffer, get_config()["raw_data_pull"]["base_data_bucket"],
                                       get_config()["adjustments"]["daily_data_prefix"],
                                       ticker, run_date)
        self.data_s3_key = data_s3_key
        self.step_status[S3_LOCATION_LABEL] = build_s3_url(*data_s3_key)
        return StatusType.Success


class DepositAdjustmentFactorsIntoS3(BaseETLStep):
    REQUIRES_FIELDS = [TICKDATA_CORPORATE_ACTION_LABEL, DATA_TICKDATA_PRICE_HANDLER_LABEL]
    PROVIDES_FIELDS = [S3_ADJ_FACTORS_LOCATION_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.data_s3_key = None

    def do_step_action(self, **kwargs):
        ca_data = kwargs[TICKDATA_CORPORATE_ACTION_LABEL]
        factors_df = self._create_adjustment_factors_df(ca_data)

        final_adjusted_data = kwargs[DATA_TICKDATA_PRICE_HANDLER_LABEL].get_data_holder().get_data()
        required_dates = final_adjusted_data.index.normalize().unique()
        factors_df = factors_df.loc[pd.np.in1d(pd.DatetimeIndex(factors_df["cob"]).normalize(), required_dates), :]

        bucket, key = self._store_adjustment_factors_in_s3(factors_df)
        data_s3_key = (bucket, key)
        self.step_status[S3_ADJ_FACTORS_LOCATION_LABEL] = build_s3_url(bucket, key)
        self.data_s3_key = data_s3_key
        return StatusType.Success

    def _create_adjustment_factors_df(self, ca_data):
        ticker = self.task_params.ticker
        run_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt

        ca_data_normalized = ca_data.get_ca_data_normalized()
        split_adjustment_factors = ca_data.get_split_adjustment_factor()
        full_adjustment_factors = ca_data.get_full_adjustment_factor()
        factors_df = pd.concat([split_adjustment_factors, full_adjustment_factors], axis=1)
        factors_df.columns = ["split_factor", "split_div_factor"]

        # Must have time set to 4 pm NY time, converted to UTC
        factors_df.index = (factors_df.index + pd.Timedelta('16 hours')).tz_localize('US/Eastern').tz_convert("UTC")
        factors_df.index.name = "cob"

        # Adding additional columns
        factors_df["as_of_start"] = str(pd.Timestamp.now("UTC").to_pydatetime()).rsplit(".", 1)[0]
        factors_df["as_of_end"] = pd.np.NaN
        factors_df["ticker"] = ticker
        factors_df = _add_symbol_asof_and_companyid_columns(ca_data_normalized, factors_df)
        factors_df.columns = factors_df.columns.str.lower()
        factors_df.reset_index(inplace=True)

        columns = ['ticker', 'cob', 'split_factor', 'split_div_factor', 'as_of_start', 'as_of_end',
                   'symbol_asof', 'dcm_security_id']
        factors_df = factors_df[columns]
        return factors_df

    def _store_adjustment_factors_in_s3(self, factors_df):
        csv_buffer = StringIO()
        factors_df.to_csv(csv_buffer, index=False)
        return store_data_in_s3(csv_buffer, get_config()["raw_data_pull"]["base_data_bucket"],
                                get_config()["adjustments"]["adjustment_factors"],
                                self.task_params.ticker, self.task_params.run_dt)

    def _get_additional_step_results(self):
        return {S3_ADJ_FACTORS_LOCATION_LABEL: self.data_s3_key}


def create_adjustment_stage_with_yahoo_cross_referencing(full_adjustment=False):
    timeline_checker_correct = VerifyEquityDataTimelineAdjustment(full_adjustment)
    date_normalizer_correct = CompleteEquityDataMinutesAdjustment()
    outlier_removal_price_correct = OutlierRemovalForPriceAdjustment()
    outlier_removal_volume_correct = OutlierRemovalForVolumeAdjustment()

    timeline_checker_verify = VerifyEquityDataTimelineAdjustment(full_adjustment, True)
    date_normalizer_verify = CompleteEquityDataMinutesAdjustment(True)
    outlier_removal_price_verify = OutlierRemovalForPriceAdjustment(True)
    outlier_removal_volume_verify = OutlierRemovalForVolumeAdjustment(True)

    yahoo_adjuster = AdjustDataForCorporateActions()
    adjusted_data_writer = DepositAdjustedDataIntoS3()

    equities_adjustment = BaseETLStage("FullAdjustment", "Sub-Stage", timeline_checker_correct, date_normalizer_correct,
                                       outlier_removal_price_correct)
    if full_adjustment:
        equities_adjustment.add(outlier_removal_volume_correct)
    equities_adjustment.add(timeline_checker_verify, date_normalizer_verify, outlier_removal_price_verify)

    if full_adjustment:
        equities_adjustment.add(outlier_removal_volume_verify, yahoo_adjuster)
    equities_adjustment.add(adjusted_data_writer)
    return equities_adjustment


def create_adjustment_stage(full_adjustment=False):
    timeline_checker_correct = TickdataTimelineAdjustment(full_adjustment)
    timeline_checker_verify = TickdataTimelineAdjustment(full_adjustment, True)

    date_normalizer_correct = CompleteEquityDataMinutesAdjustment()
    date_normalizer_verify = CompleteEquityDataMinutesAdjustment(True)

    ca_adjuster = AdjustDataForTickDataCorporateActions()
    full_adj_verifier = FullAdjustmentVerifier()
    adjusted_data_writer = DepositAdjustedDataIntoS3()
    adjusted_daily_data_writer = DepositDailyAdjustedDataIntoS3()
    adjustment_factors_writer = DepositAdjustmentFactorsIntoS3()

    equities_adjustment = BaseETLStage("FullAdjustment", "Sub-Stage", timeline_checker_correct, date_normalizer_correct)
    equities_adjustment.add(timeline_checker_verify, date_normalizer_verify)
    if full_adjustment:
        equities_adjustment.add(ca_adjuster, full_adj_verifier)
    equities_adjustment.add(adjusted_data_writer, adjusted_daily_data_writer, adjustment_factors_writer)
    return equities_adjustment


def create_adjustment_stage_no_deposit_to_s3(full_adjustment=False):
    timeline_checker_correct = TickdataTimelineAdjustment(full_adjustment)
    timeline_checker_verify = TickdataTimelineAdjustment(full_adjustment, True)

    date_normalizer_correct = CompleteEquityDataMinutesAdjustment()
    date_normalizer_verify = CompleteEquityDataMinutesAdjustment(True)

    ca_adjuster = AdjustDataForTickDataCorporateActions()
    full_adj_verifier = FullAdjustmentVerifier()

    equities_adjustment = BaseETLStage("FullAdjustment", "Sub-Stage", timeline_checker_correct, date_normalizer_correct)
    equities_adjustment.add(timeline_checker_verify, date_normalizer_verify)

    if full_adjustment:
        equities_adjustment.add(ca_adjuster, full_adj_verifier)
    return equities_adjustment


if __name__ == "__main__":
    import os
    import _pickle as pickle
    from etl_workflow_steps import compose_main_flow_and_engine_for_task

    path_dcm_raw_data = r"C:\DCM\temp\adjustment"
    # path_dcm_raw_data = r"C:\DCM\temp"  #\adjustment"

    # create engine and task
    # ticker = 'HSBC'
    # task_params = EquitiesETLTaskParameters(ticker=ticker,
    #                                         as_of_dt=pd.Timestamp("2018-12-28 09:30:00"),
    #                                         run_dt=pd.Timestamp("2018-10-11"),
    #                                         start_dt=pd.Timestamp("2000-01-03"),
    #                                         end_dt=pd.Timestamp("2018-10-11"))

    ticker = 'JPM'
    task_params = EquitiesETLTaskParameters(ticker=ticker,
                                            as_of_dt=pd.Timestamp("2018-12-27 09:30:00"),
                                            run_dt=pd.Timestamp("2018-12-26"),
                                            start_dt=pd.Timestamp("2000-01-03"),
                                            end_dt=pd.Timestamp("2018-12-26"))

    # timeline_correct = VerifyEquityDataTimelineAdjustment()
    # timeline_correct.task_params = task_params
    # result_correct  = timeline_correct.do_step_action(**data_retrieval_results)

    # timeline_verify = VerifyEquityDataTimelineAdjustment(True)
    # timeline_verify.task_params = task_params
    # result_verify  = timeline_verify.do_step_action(**data_retrieval_results)

    # full_minutes_correct = CompleteEquityDataMinutesAdjustment()
    # full_minutes_correct.task_params = task_params
    # result_correct  = full_minutes_correct.do_step_action(**data_retrieval_results)

    # full_minutes_verify = CompleteEquityDataMinutesAdjustment(True)
    # full_minutes_verify.task_params = task_params
    # result_verify  = full_minutes_verify.do_step_action(**data_retrieval_results)

    # price_outlier_correct = OutlierRemovalForPriceAdjustment()
    # price_outlier_correct.task_params = task_params
    # result_correct  = price_outlier_correct.do_step_action(**data_retrieval_results)

    # price_outlier_verify = OutlierRemovalForPriceAdjustment(True)
    # price_outlier_verify.task_params = task_params
    # result_verify  = price_outlier_verify.do_step_action(**data_retrieval_results)

    # volume_outlier_correct = OutlierRemovalForVolumeAdjustment()
    # volume_outlier_correct.task_params = task_params
    # result_correct  = volume_outlier_correct.do_step_action(**data_retrieval_results)

    # volume_outlier_verify = OutlierRemovalForVolumeAdjustment(True)
    # volume_outlier_verify.task_params = task_params
    # result_verify  = volume_outlier_verify.do_step_action(**data_retrieval_results)

    # equity_adjuster = AdjustDataForCorporateActions()
    # equity_adjuster.task_params = task_params
    # result  = equity_adjuster.do_step_action(**data_retrieval_results)

    # s3_data_loc = DepositAdjustedDataIntoS3()
    # s3_data_loc.task_params = task_params
    # result  = s3_data_loc.do_step_action(**data_retrieval_results)

    # Data retrieval stage
    full_path_dcm_raw = os.path.join(path_dcm_raw_data, '%s_data.pickle' % ticker)
    if not os.path.exists(full_path_dcm_raw):
        from data_retrieval_workflow_steps import create_data_retrieval_stage
        data_retrieval_stage = create_data_retrieval_stage(True)
        engine, main_flow = compose_main_flow_and_engine_for_task("Data_Retrieval_Sample", task_params,
                                                                  data_retrieval_stage)
        engine.run(main_flow)
        data_retrieval_results = engine.storage.fetch_all()
        #pickle.dump(data_retrieval_results, open(full_path_dcm_raw, "wb"))
    else:
        data_retrieval_results = pickle.load(open(full_path_dcm_raw, "rb"))

    # Adjustment stage (with/without depositing to S3)
    adjustment_flow = create_adjustment_stage(True)
    # adjustment_flow = create_adjustment_stage_no_deposit_to_s3(True)
    engine, main_flow = compose_main_flow_and_engine_for_task("Data_Adjustment_Sample", task_params, adjustment_flow,
                                                              store=data_retrieval_results)
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
