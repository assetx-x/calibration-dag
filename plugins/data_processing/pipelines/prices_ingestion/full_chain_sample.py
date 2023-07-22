from pipelines.prices_ingestion.data_pull_workflow_steps import create_data_pull_stage, TickDataPriceDataPullSpark, setup_mock_s3
from pipelines.prices_ingestion.data_retrieval_workflow_steps import create_data_retrieval_stage
from pipelines.prices_ingestion.qa_workflow_steps import create_qa_stage
from pipelines.prices_ingestion.adjustment_workflow_steps import create_adjustment_stage
from etl_workflow_steps import EquitiesETLTaskParameters, compose_main_flow_and_engine_for_task
import pandas as pd

from taskflow.patterns import linear_flow
# from pyspark import SparkContext


class SimplifiedPriceIngestionStage(object):
    def __init__(self, stage_parameters, custom_ticker_list=None):
        if not isinstance(stage_parameters, EquitiesETLTaskParameters):
            raise ValueError()
        self.stage_parameters = stage_parameters
        self.ticker_list = custom_ticker_list or self.infer_tickers_list()
        self.step_results = None

    def run(self):
        self.step_results = self._collect_step_results()
        self._store_step_results()

    def _collect_step_results(self):
        raise NotImplementedError()

    def _infer_tickers_list(self):
        raise NotImplementedError()

    def _store_step_results(self):
        print(self.step_results)

    def _get_universe_file_content(self):
        pass

    def _get_overal_status_per_ticker(self):
        pass


class TickDataStage(SimplifiedPriceIngestionStage):
    def _infer_tickers_list(self):
        return None

    def _collect_step_results(self):
        data_puller = TickDataPriceDataPullSpark(self.ticker_list)
        full_flow = linear_flow.Flow("FullDataProcess").add(data_puller)
        engine, main_flow = compose_main_flow_and_engine_for_task("FullProcess", task_params, full_flow)
        engine.run(main_flow)
        results = engine.storage.fetch_all()
        return results["individual_tickdata_price_load_stepresults"]

def full_adjustment_chain_for_one_ticker(task_params, full_adjustment_flag):
    #import wingdbstub
    #m = setup_mock_s3()
    data_pull = create_data_pull_stage()
    data_retrieval = create_data_retrieval_stage(full_adjustment_flag)
    data_qa = create_qa_stage(full_adjustment_flag)
    data_adjustment = create_adjustment_stage(full_adjustment_flag)
    full_flow = linear_flow.Flow("FullDataProcess").add(data_pull, data_retrieval, data_qa, data_adjustment)

    engine, main_flow = compose_main_flow_and_engine_for_task("FullProcess", task_params, full_flow)
    ##run flow and get results
    engine.run(main_flow)
    results = engine.storage.fetch_all()
    step_results = filter(None, results["step_results"])
    print("Finishing {0}".format(task_params.ticker))
    #m.stop()
    return step_results

class AdjustmentStage(SimplifiedPriceIngestionStage):
    def _infer_tickers_list(self):
        return self._get_universe_file_content()

    def get_full_adjustment_decisions(self):
        from collections import defaultdict
        return defaultdict(lambda :True)

    def _collect_step_results(self):
        fulladj_flag_map = self.get_full_adjustment_decisions()
        sc = SparkContext("local[{0}]".format(min(len(self.ticker_list), 24)), "adjustment")
        try:
            adjustments_to_run_params = [{"task_params":type(self.stage_parameters)(k, *self.stage_parameters[1:]),
                                          "full_adjustment_flag": fulladj_flag_map[k]} for k in self.ticker_list]
            adjustments_to_run_params_rdd = sc.parallelize(adjustments_to_run_params)
            adjustment_results_rdd = adjustments_to_run_params_rdd.map(lambda x:full_adjustment_chain_for_one_ticker(**x))
            adjustment_results = adjustment_results_rdd.collect()
        finally:
            sc.stop()
        return adjustment_results


if __name__ == "__main__":
    # task_params = EquitiesETLTaskParameters(ticker=None, as_of_dt=pd.Timestamp("2017-07-09"),
    #                                         run_dt=pd.Timestamp("2017-07-07"),
    #                                         start_dt=pd.Timestamp("2000-01-03"), end_dt=pd.Timestamp("2017-07-07"))
    # tick_pull = TickDataStage(task_params, ["AAPL", "JPM", "NFLX"])
    # tick_pull.run()

    # task_params = EquitiesETLTaskParameters(ticker=None, as_of_dt=pd.Timestamp("2018-07-25"),
    #                                         run_dt=pd.Timestamp("2017-10-19"),
    #                                         start_dt=pd.Timestamp("2000-01-03"), end_dt=pd.Timestamp("2017-10-19"))
    # adj_flow = AdjustmentStage(task_params, ["AAPL", "JPM", "NFLX"])
    # adj_flow.run()

    task_params = EquitiesETLTaskParameters(ticker="HSBC", as_of_dt=pd.Timestamp("2018-10-13"),
                                            run_dt=pd.Timestamp("2018-10-11"),
                                            start_dt=pd.Timestamp("2000-01-03"), end_dt=pd.Timestamp("2018-10-11"))
    full_adjustment_chain_for_one_ticker(task_params, True)

