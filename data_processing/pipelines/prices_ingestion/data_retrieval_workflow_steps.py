import pandas as pd

from commonlib.config import get_config
from utils import translate_ticker_name
from datetime import timedelta, datetime, time
from commonlib.market_timeline import marketTimeline
from pipelines.prices_ingestion.equity_data_holders import S3YahooEquityDataHolder, RedshiftRawTickDataDataHolder
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_redshift_engine, convert_localize_timestamps
from etl_workflow_steps import BaseETLStep, BaseETLStage, EquitiesETLTaskParameters, StatusType
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHolder, S3YahooCorporateActionsHolder

DATA_YAHOO_PRICE_LABEL = get_config()["etl_equities_process_labels"]["DATA_YAHOO_PRICE_LABEL"]
DATA_TICKDATA_PRICE_HANDLER_LABEL = get_config()["etl_equities_process_labels"]["DATA_TICKDATA_PRICE_HANDLER_LABEL"]
DATA_YAHOO_CORPORATE_ACTION_LABEL = get_config()["etl_equities_process_labels"]["DATA_YAHOO_CORPORATE_ACTION_LABEL"]
DATA_LINEAGE_LABEL = get_config()["etl_equities_results_labels"]["DATA_LINEAGE_LABEL"]
DATA_HOLDER_TYPE_LABEL = get_config()["etl_equities_results_labels"]["DATA_HOLDER_TYPE_LABEL"]

DATA_TICKDATA_ONEDAY_PRICE_LABEL = get_config()["etl_equities_process_labels"]["DATA_TICKDATA_ONEDAY_PRICE_LABEL"]
TICKDATA_CORPORATE_ACTION_LABEL = get_config()["etl_equities_process_labels"]["TICKDATA_CORPORATE_ACTION_LABEL"]
TICKDATA_TICKER_HISTORY_LABEL = get_config()["etl_equities_process_labels"]["TICKDATA_TICKER_HISTORY_LABEL"]


class DataHolderHandlerForAdjustment(object):
    """ This is an object to avoid memory bloating; internally this object holds a EquityDataHolder that will be passed
    through the adjustment process. The idea is that each Adjustment Step will return one object of this type, to get
    accumulated on the results field for price data, but internally the actual data holder content will be modified
    as the adjustment goes through. That is, instead of holding N copies of data holders in memory as results objects
    we hold only one of these objects, with it's content getting modified by the flow
    """

    def __init__(self, dh):
        self.data_holder = dh

    def get_data_holder(self):
        return self.data_holder

    def set_data_holder(self, new_data_holder):
        self.data_holder = new_data_holder


class YahooCorporateActionsDataRetriever(BaseETLStep):
    PROVIDES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self, report_failure_as_warning=True)
        self.yahoo_ca_data = None

    def do_step_action(self, **kwargs):
        # for now re-use the functionality of equity data holder as-is
        ticker = self.task_params.ticker
        run_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt
        self.yahoo_ca_data = S3YahooCorporateActionsHolder(ticker=translate_ticker_name(ticker, "unified", "yahoo"),
                                                           run_date=run_date, as_of_date=as_of_date)
        self.step_status[DATA_LINEAGE_LABEL] = self.yahoo_ca_data.get_data_lineage()
        self.step_status[DATA_HOLDER_TYPE_LABEL] = self.yahoo_ca_data.__class__.__name__

        return StatusType.Success

    def _get_additional_step_results(self):
        return {DATA_YAHOO_CORPORATE_ACTION_LABEL: self.yahoo_ca_data}


class TickDataCorporateActionsDataRetriever(BaseETLStep):
    PROVIDES_FIELDS = [TICKDATA_CORPORATE_ACTION_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.tickdata_ca_data = None

    def do_step_action(self, **kwargs):
        ticker = translate_ticker_name(self.task_params.ticker, "unified", "tickdata")
        run_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt
        start_dt = self.task_params.start_dt
        end_dt = self.task_params.end_dt
        self.tickdata_ca_data = S3TickDataCorporateActionsHolder(ticker, run_date=run_date, as_of_date=as_of_date,
                                                                 start_dt=start_dt, end_dt=end_dt)
        self.step_status[DATA_LINEAGE_LABEL] = self.tickdata_ca_data.get_data_lineage()
        self.step_status[DATA_HOLDER_TYPE_LABEL] = self.tickdata_ca_data.__class__.__name__

        return StatusType.Success

    def _get_additional_step_results(self):
        return {TICKDATA_CORPORATE_ACTION_LABEL: self.tickdata_ca_data}


class YahooPriceDataRetriever(BaseETLStep):
    PROVIDES_FIELDS = [DATA_YAHOO_PRICE_LABEL]
    REQUIRES_FIELDS = [DATA_YAHOO_CORPORATE_ACTION_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.yahoo_data = None

    def do_step_action(self, **kwargs):
        # for now re-use the functionality of equity data holder as-is
        ticker = self.task_params.ticker
        end_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt
        ca_holder = kwargs[DATA_YAHOO_CORPORATE_ACTION_LABEL]
        self.yahoo_data = S3YahooEquityDataHolder(translate_ticker_name(ticker, "unified", "yahoo"), ca_holder,
                                                  end_date=end_date, as_of_date=as_of_date)
        self.step_status[DATA_LINEAGE_LABEL] = self.yahoo_data.get_data_lineage()
        self.step_status[DATA_HOLDER_TYPE_LABEL] = self.yahoo_data.__class__.__name__

        return StatusType.Success

    def _get_additional_step_results(self):
        return {DATA_YAHOO_PRICE_LABEL: self.yahoo_data}


class TickDataFullHistoryDataRetriever(BaseETLStep):
    PROVIDES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL]

    def __init__(self, ticker_history=None):
        BaseETLStep.__init__(self)
        self.tickdata_price_data = None

    def do_step_action(self, **kwargs):
        # for now re-use the functionality of equity data holder as-is
        ticker = self.task_params.ticker
        end_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt
        equity_data_holder = RedshiftRawTickDataDataHolder(ticker=translate_ticker_name(ticker, "unified", "tickdata"),
                                                           end_date=end_date, as_of_date=as_of_date)
        self.step_status[DATA_LINEAGE_LABEL] = equity_data_holder.get_data_lineage()
        self.step_status[DATA_HOLDER_TYPE_LABEL] = equity_data_holder.__class__.__name__
        self.tickdata_price_data = DataHolderHandlerForAdjustment(equity_data_holder)

        return StatusType.Success

    def _get_additional_step_results(self):
        return {DATA_TICKDATA_PRICE_HANDLER_LABEL: self.tickdata_price_data}


class TickDataPassThroughDataRetriever(BaseETLStep):
    PROVIDES_FIELDS = [DATA_TICKDATA_ONEDAY_PRICE_LABEL, "retrieved_from"]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.tick_data = None
        self.retrieved_from = None

    def do_step_action(self, **kwargs):
        ticker = self.task_params.ticker
        end_date = self.task_params.run_dt
        as_of_date = self.task_params.as_of_dt
        utc_end = convert_localize_timestamps(pd.Timestamp(end_date))
        engine = get_redshift_engine()

        query = "SELECT cob FROM {table} " \
                "WHERE cob <= '{end_date}' AND ticker = '{ticker}' AND farm_fingerprint = FARM_FINGERPRINT('{ticker}') " \
                "AND as_of_end IS NULL ORDER by cob desc \
                LIMIT 1".format(table=get_config()['redshift']['equity_price_table'],
                                               end_date=utc_end.date(),
                                               ticker=ticker)

        with engine.begin() as connection:
            query_results = [r for r in connection.execute(query)]
            start_date = query_results[0][0] if query_results else None

        if start_date is None:
            start_date = self.task_params.start_dt.date()
        else:
            start_date += timedelta(days=1)
            start_date = datetime.combine(start_date.date(), time(0, 0))

            while start_date.weekday() >= 5:
                start_date += timedelta(1)

        start_date = marketTimeline.get_trading_days(pd.Timestamp(start_date), end_date)[0]

        equity_data_holder = RedshiftRawTickDataDataHolder(
            ticker=translate_ticker_name(ticker, "unified", "tickdata"),
            start_date=start_date,
            end_date=end_date,
            as_of_date=as_of_date
        )

        self.step_status[DATA_LINEAGE_LABEL] = equity_data_holder.get_data_lineage()
        self.step_status[DATA_HOLDER_TYPE_LABEL] = equity_data_holder.__class__.__name__
        self.tick_data = DataHolderHandlerForAdjustment(equity_data_holder)
        self.retrieved_from = self.step_status["retrieved_from"] = start_date

        return StatusType.Success

    def _get_additional_step_results(self):
        return {
            DATA_TICKDATA_ONEDAY_PRICE_LABEL: self.tick_data,
            "retrieved_from": self.retrieved_from
        }


class ReRouteOneDayTickDataToPriceHandler(BaseETLStep):
    REQUIRES_FIELDS = [DATA_TICKDATA_ONEDAY_PRICE_LABEL]
    PROVIDES_FIELDS = [DATA_TICKDATA_PRICE_HANDLER_LABEL]

    def __init__(self):
        BaseETLStep.__init__(self)
        self.tick_data = None

    def do_step_action(self, **kwargs):
        equity_data_holder = kwargs[DATA_TICKDATA_ONEDAY_PRICE_LABEL].get_data_holder()
        self.tick_data = DataHolderHandlerForAdjustment(equity_data_holder)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {DATA_TICKDATA_PRICE_HANDLER_LABEL: self.tick_data}


def create_data_retrieval_stage(full_adjustment=False):
    # yahoo_ca_retriever = YahooCorporateActionsDataRetriever()
    # yahoo_price_retriever = YahooPriceDataRetriever()
    tickdata_ca_data_retriever = TickDataCorporateActionsDataRetriever()
    tickdata_on_eday_price_retriever = TickDataPassThroughDataRetriever()
    # data_retrieval = BaseETLStage("DataRetrieval", "Stage", yahoo_ca_retriever, yahoo_price_retriever,
    #                              tickdata_on_eday_price_retriever, tickdata_ca_data_retriever)
    data_retrieval = BaseETLStage("DataRetrieval", "Stage", tickdata_on_eday_price_retriever, tickdata_ca_data_retriever)
    if full_adjustment:
        tickdata_price_retriever = TickDataFullHistoryDataRetriever()
    else:
        tickdata_price_retriever = ReRouteOneDayTickDataToPriceHandler()
    data_retrieval.add(tickdata_price_retriever)
    return data_retrieval


if __name__ == "__main__":
    from etl_workflow_steps import compose_main_flow_and_engine_for_task

    # ticker = "AAPL"
    # as_of_dt = pd.Timestamp("2017-07-07")
    # run_dt = pd.Timestamp("2017-07-06")
    # start_dt = pd.Timestamp("2000-01-03")
    # end_dt = pd.Timestamp("2017-07-06")

    # ticker = 'AMN'
    # as_of_dt = pd.Timestamp("2018-12-11")
    # run_dt = pd.Timestamp("2018-06-25")
    # start_dt = pd.Timestamp("2011-10-27")
    # end_dt = pd.Timestamp("2018-06-25")

    # ticker = 'HSBC'
    # as_of_dt = pd.Timestamp("2018-12-12 09:00:00")
    # run_dt = pd.Timestamp("2018-12-11")
    # start_dt = pd.Timestamp("2011-10-27")
    # end_dt = pd.Timestamp("2018-12-11")

    ticker = "TARO"
    as_of_dt = pd.Timestamp("2018-12-11 09:00:00")
    run_dt = pd.Timestamp("2018-12-10")
    start_dt = pd.Timestamp("2018-12-07")
    end_dt = pd.Timestamp("2018-12-10")

    # ticker = "AEO"
    # as_of_dt = pd.Timestamp.now("UTC")
    # run_dt = pd.Timestamp("2018-06-25")
    # start_dt = pd.Timestamp("2005-10-27")
    # end_dt = pd.Timestamp("2009-06-25")

    task_params = EquitiesETLTaskParameters(ticker=ticker, as_of_dt=as_of_dt, run_dt=run_dt,
                                            start_dt=start_dt, end_dt=end_dt)

    # yahoo_price_retriever = YahooPriceDataRetriever()
    # yahoo_price_retriever.task_params = task_params
    # status = yahoo_price_retriever.do_step_action()

    # yahoo_ca_retriever = YahooCorporateActionsDataRetriever()
    # yahoo_ca_retriever.task_params = task_params
    # status = yahoo_ca_retriever.do_step_action()

    # tickdata_price_retriever = TickDataFullHistoryDataRetriever()
    # tickdata_price_retriever.task_params = task_params
    # status = tickdata_price_retriever.do_step_action()

    # tickdata_retriever = TickDataPassThroughDataRetriever()
    # tickdata_retriever.task_params = task_params
    # status = tickdata_retriever.do_step_action()

    # tickdata_ca_retriever = TickDataCorporateActionsDataRetriever()
    # tickdata_ca_retriever.task_params = task_params
    # status = tickdata_ca_retriever.do_step_action()

    data_retrieval_stage = create_data_retrieval_stage(True)
    task_engine, main_flow = compose_main_flow_and_engine_for_task("Data_Retrieval_Sample", task_params,
                                                                   data_retrieval_stage)
    # run flow and get results
    task_engine.run(main_flow)
    results = task_engine.storage.fetch_all()
    step_results = filter(None, results["step_results"])

    for k in step_results:
        print(k["status_type"])

    print(results)
