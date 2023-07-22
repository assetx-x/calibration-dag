from datetime import datetime

from luigi import WrapperTask
from luigi import build as run_luigi_pipeline

from base import DCMTaskParams, ExternalLuigiTask, S3Target
from pipelines.common.price_adjustment_results import PriceAdjustmentResults
from pipelines.common.stage_store import PipelineStageStore
from pipelines.earnings_ingestion import EarningsIngestionPipeline
from calibrations.views_in_market_trading.market_view_analysis_input_generator import CalibrationTask
from pipelines.prices_ingestion import get_config, PricePipelineHelper

config = get_config()

class CalibrationTask(DCMTaskParams, Task):
    def __init__(self, *args, **kwargs):
        super(CalibrationTask, self).__init__(*args, **kwargs)
        self.original_config = None
        self.calibration_results_path = None
        self.__correct_config_options_before_calibration()
        self.__restore_config_options_after_calibration()
        calibration_logger.info('****** CalibrationTask initialized')

    def __correct_config_options_before_calibration(self):
        self.original_config = deepcopy(get_config())
        get_config()["SQLReader"]["persistent_data_caching"] = False
        get_config()["SQLReader"]["force_data_pull"] = True
        get_config()["SQLReader"]["local_cache_base_dir"] = None
        get_config()["SQLReader"]["s3_cache_base_dir"] = get_config()["SQLReader"]["s3_calibration_temp_dir"]
        get_config()["ViewsInMarketFullCalibration"]["calibration_location"] = \
            get_config()["ViewsInMarketFullCalibration"]["s3_calibration_location"]
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
        #temp_dir = tempfile.mkdtemp()
        temp_dir = r"C:\DCM\temp"
        try:
            self.__correct_config_options_before_calibration()
            universe_dt = marketTimeline.get_trading_day_using_offset(pd.Timestamp(self.date), -1)
            flow_params = create_pipeline_parameters_for_calibration(pd.Timestamp(self.date), universe_dt)
            data_pipeline_file = os.path.join(temp_dir, "market_view_analysis_input_generator_cal.h5")
            calibration_pipeline_file = os.path.join(temp_dir, "market_view_analysis_calibration_cal.h5")
            #create_and_run_data_preparation_pipeline(flow_params, data_pipeline_file, calibration_mode=True)
            create_and_run_data_calibration_pipeline(flow_params, calibration_pipeline_file, data_pipeline_file)
        finally:
            #self.__cleanup_temp_files_after_calibration(temp_dir)
            self.__restore_config_options_after_calibration()



class CalibrationStep(CalibrationTask):
    def __init__(self, *args, **kwargs):
        super(CalibrationStep, self).__init__(*args, **kwargs)
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
        super(CalibrationStep, self).run()
        end_time = datetime.utcnow()
        if S3Target(self.calibration_results_path).exists():
            stage_id = self.date_ymd + '-market-views-calibration'
            with PipelineStageStore(self.run_id, self.date_only(), stage_id) as progress:
                progress.save_progress(start_time, end_time, 'stage')
            self.adjustment_results.record_missing_tickers('market-views-missing-tickers')


class CalibrationPipeline(DCMTaskParams, WrapperTask):
    def __init__(self, *args, **kwargs):
        kwargs["date"] = kwargs.get("date", datetime.today())
        super(CalibrationPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        yield CalibrationStep(date=self.date)


def _main():
    import os
    file_dir = os.path.dirname(os.path.realpath(__file__))
    logging_conf_path = os.path.join(file_dir, "..", "..", "conf", "luigi-logging.conf")

    config["calibration"]["max_missing_tickers_allowed"] = "2.5%"
    run_luigi_pipeline([CalibrationPipeline()], local_scheduler=True, logging_conf_file=logging_conf_path)


if __name__ == '__main__':
    _main()
