from datetime import datetime

import pytz
from luigi import WrapperTask
from luigi import build as run_luigi_pipeline

from base import DCMTaskParams, ExternalLuigiTask, S3Target
from pipelines.common.price_adjustment_results import PriceAdjustmentResults
from pipelines.common.stage_store import PipelineStageStore
from pipelines.earnings_ingestion import EarningsIngestionPipeline
from pipelines.prices_ingestion import get_config, PricePipelineHelper

config = get_config()


class CalibrationTaskFundamentalStrategy(DCMTaskParams, Task):
    def __init__(self, *args, **kwargs):
        super(CalibrationTaskFundamentalStrategy, self).__init__(*args, **kwargs)
        self.original_config = None
        self.calibration_results_path = None
        self.__correct_config_options_before_calibration()
        self.__restore_config_options_after_calibration()
        calibration_logger.info('****** CalibrationTaskFundamentalStrategy initialized')

    def __correct_config_options_before_calibration(self):
        self.original_config = deepcopy(get_config())
        get_config()["SQLReader"]["persistent_data_caching"] = False
        get_config()["SQLReader"]["force_data_pull"] = True
        get_config()["SQLReader"]["local_cache_base_dir"] = None
        get_config()["SQLReader"]["s3_cache_base_dir"] = get_config()["FundamentalsCalibration"]["s3_cache_base_dir"]
        del(get_config()["FundamentalsCalibration"]["s3_cache_base_dir"])
        get_config()["FundamentalsCalibration"]["output_dir"] = None
        self.calibration_results_path = set_calibration_results_path()

    def __restore_config_options_after_calibration(self):
        set_config(self.original_config)

    @staticmethod
    def __cleanup_temp_files_after_calibration(temp_dir):
        files_to_clean = glob(os.path.join(temp_dir, "*"))
        for filename in files_to_clean:
            os.remove(filename)
        try:
            os.rmdir(temp_dir)
        except BaseException:
            pass

    def run(self):
        calibration_logger.info('****** Starting new run')
        temp_dir = tempfile.mkdtemp()
        try:
            self.__correct_config_options_before_calibration()
            universe_dt = marketTimeline.get_trading_day_using_offset(pd.Timestamp(self.date), -1)
            flow_params = create_pipeline_parameters_for_calibration(pd.Timestamp(self.date), universe_dt)
            create_and_run_full_calibration_pipeline(flow_params, temp_dir, True)
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            raise
        finally:
            self.__cleanup_temp_files_after_calibration(temp_dir)
            self.__restore_config_options_after_calibration()


def set_calibration_results_path():
    file_suffix = get_config()["FundamentalsCalibration"]["file_suffix"]
    filename = "{0}_{1}".format(str(pd.Timestamp.now().date()).replace("-", ""), file_suffix)
    target_location = get_config()["FundamentalsCalibration"]["s3_location"]
    if target_location.startswith("s3"):
        calibration_results_path = join_s3_path(target_location, filename)
    else:
        calibration_results_path = os.path.join(target_location, filename)
    get_config()["FundamentalsCalibration"]["calibration_results_path"] = calibration_results_path
    calibration_logger.info("DEBUG: calibration_results_path =%s", calibration_results_path)
    return calibration_results_path


def _main():
    # historical mode
    DATA_DIR = r"C:\DCM\engine_results\fundamental_strategy"
    #DATA_DIR = r"C:\Users\sergii.lutsanych\Desktop\Fundamental_DATA_DIR"
    calibration_mode = True
    if not calibration_mode:
        # original universe_dt = pd.Timestamp("2017-12-05")
        target_dt = pd.Timestamp.now().tz_localize(None).normalize()
        universe_dt = pd.Timestamp("2018-05-11")
        #universe_dt = marketTimeline.get_trading_day_using_offset(pd.Timestamp.now().tz_localize(None).normalize(), -1)
        flow_parameters = BasicTaskParameters(pd.Timestamp.now(), pd.Timestamp("2004-06-01"), target_dt,
                                              universe_dt)
        #universe_dt = marketTimeline.get_trading_day_using_offset(pd.Timestamp.now().tz_localize(None).normalize(), -1)
        #flow_parameters = BasicTaskParameters(pd.Timestamp.now(), pd.Timestamp("2012-06-01"), pd.Timestamp("2015-01-03"),
        #                                      universe_dt)
        #flow_parameters = BasicTaskParameters(pd.Timestamp.now(), pd.Timestamp("2013-06-01"), pd.Timestamp("2017-12-13"),
        #                                          pd.Timestamp("2017-12-05"))
    else:
        #target_dt = pd.Timestamp.now().tz_localize(None).normalize()
        target_dt = pd.Timestamp("2018-07-16").tz_localize(None).normalize()
        #target_dt = marketTimeline.get_trading_day_using_offset(target_dt, -1)
        universe_dt = marketTimeline.get_trading_day_using_offset(target_dt, -1)
        #target_dt = pd.Timestamp("2014-01-02")
        flow_parameters = create_pipeline_parameters_for_calibration(target_dt, universe_dt)

        get_config()["SQLReader"]["persistent_data_caching"] = False
        get_config()["SQLReader"]["force_data_pull"] = True
        get_config()["SQLReader"]["local_cache_base_dir"] = None
        get_config()["SQLReader"]["s3_cache_base_dir"] = get_config()["SQLReader"]["s3_calibration_temp_dir"]

    create_and_run_full_calibration_pipeline(flow_parameters, DATA_DIR, calibration_mode)

class FundamentalsCalibrationStep(CalibrationTaskFundamentalStrategy):
    def __init__(self, *args, **kwargs):
        super(FundamentalsCalibrationStep, self).__init__(*args, **kwargs)
        self.run_id = PricePipelineHelper.get_run_id()
        self.adjustment_results = None

    def requires(self):
        if not self.adjustment_results:
            self.adjustment_results = PriceAdjustmentResults(date=self.date, run_id=self.run_id)

        yield [
            self.adjustment_results,
            ExternalLuigiTask(external_task=EarningsIngestionPipeline(date=self.date)),
        ]

    def output(self):
        return S3Target(self.calibration_results_path)

    def run(self):
        start_time = datetime.utcnow()
        super(FundamentalsCalibrationStep, self).run()
        end_time = datetime.utcnow()
        if S3Target(self.calibration_results_path).exists():
            stage_id = self.date_ymd + '-fundamentals-calibration'
            with PipelineStageStore(self.run_id, self.date_only(), stage_id) as progress:
                progress.save_progress(start_time, end_time, 'stage')
            self.adjustment_results.record_missing_tickers('fundamentals-missing-tickers')


class FundamentalsCalibrationPipeline(DCMTaskParams, WrapperTask):
    def __init__(self, *args, **kwargs):
        default_date = datetime.now(tz=pytz.timezone("US/Eastern")).replace(tzinfo=None)
        kwargs["date"] = kwargs.get("date", default_date)
        super(FundamentalsCalibrationPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        yield FundamentalsCalibrationStep(date=self.date)


def _main():
    import os
    file_dir = os.path.dirname(os.path.realpath(__file__))
    logging_conf_path = os.path.join(file_dir, "..", "..", "conf", "luigi-logging.conf")

    config["calibration"]["max_missing_tickers_allowed"] = "2.5%"
    run_luigi_pipeline([FundamentalsCalibrationPipeline()], local_scheduler=True, logging_conf_file=logging_conf_path)


if __name__ == '__main__':
    _main()

if __name__ == '__main__':
    _main()
    #cal = CalibrationTaskFundamentalStrategy(date=pd.Timestamp("2018-07-06"))
    #cal.run()
    calibration_logger.info("done")
