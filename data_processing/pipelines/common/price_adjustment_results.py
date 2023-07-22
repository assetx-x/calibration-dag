from datetime import datetime

from luigi import ExternalTask, Parameter

from base import DCMTaskParams
from pipelines.common.stage_store import PipelineStageStore
from pipelines.prices_ingestion import get_config, PricePipelineHelper

config = get_config()


class PriceAdjustmentResults(DCMTaskParams, ExternalTask):
    run_id = Parameter(default=None, significant=False)

    def __init__(self, *args, **kwargs):
        super(PriceAdjustmentResults, self).__init__(*args, **kwargs)
        self.max_failed_allowed = config["calibration.max_missing_tickers_allowed"].strip()
        self.universe_size = 0
        self.num_adjusted = 0
        self.missing_tickers = []

    def complete(self):
        is_complete, self.missing_tickers = self.check_if_complete()
        return is_complete

    def check_if_complete(self):
        adjustment_date = PricePipelineHelper.get_proper_run_date(self.date).date()
        adjustment_stage = adjustment_date.strftime("%Y%m%d-price-ingestion-merge")
        all_tickers = PricePipelineHelper.load_tickers()
        self.universe_size = len(all_tickers)
        with PipelineStageStore("ignored", adjustment_date, adjustment_stage) as progress:
            adjusted_tickers = progress.get_completed_stage_keys()
            adjusted_tickers = filter(lambda x: "pipeline" not in x, adjusted_tickers)
            self.num_adjusted = len(adjusted_tickers)
        missing_tickers = sorted(list(set(all_tickers) - set(adjusted_tickers)))
        return self._validate_missing_threshold(self.universe_size, len(missing_tickers)), missing_tickers

    def _validate_missing_threshold(self, num_tickers, num_missing):
        self.logger.info("Validating number of missing tickers: num tickers = %d, missing = %d, threshold is %s",
                         num_tickers, num_missing, self.max_failed_allowed)
        if isinstance(self.max_failed_allowed, str) and self.max_failed_allowed.endswith('%'):
            allowed_fraction = float(self.max_failed_allowed[:-1]) * .01
            return num_missing <= (num_tickers * allowed_fraction)
        else:
            max_allowed = int(self.max_failed_allowed)
            return num_missing <= max_allowed

    def record_missing_tickers(self, stage_name):
        stage_id = self.date_ymd + '-' + stage_name
        with PipelineStageStore(self.run_id, self.date_only(), stage_id) as progress:
            now = datetime.utcnow()
            progress.save_progress(now, now, self.missing_tickers)
