from luigi import Parameter
from datetime import datetime
from pipelines.common.base_stages import PipelineStageWithResults
from pipelines.common.stage_store import PipelineStageStore
from pipelines.prices_ingestion import PricePipelineHelper, PriceIngestionStageParams
from pipelines.security_universe_generation.security_universe_builder import UpdateSecurityUniverseStage, SecurityUniverseHandler


class SecurityUniverseGenerationPipeline(PriceIngestionStageParams, PipelineStageWithResults):
    run_id = Parameter(default=None, significant=False)

    def __init__(self, *args, **kwargs):
        kwargs["date"] = PricePipelineHelper.get_proper_run_date(kwargs.get("date", datetime.utcnow()))
        kwargs["pull_as_of"] = kwargs.get("pull_as_of", datetime.now())
        kwargs["run_id"] = PricePipelineHelper.get_run_id()
        super(SecurityUniverseGenerationPipeline, self).__init__(*args, **kwargs)

    @property
    def stage_id(self):
        return self.date_ymd + '-security-universe-generation'

    def _check_price_ingestion_status(self):
        external_stage = UpdateSecurityUniverseStage(self.date)
        return external_stage.price_adjustment_pipeline_completed()

    def _run_stage(self):
        start_time = datetime.utcnow()
        if self._check_price_ingestion_status():
            self.logger.info("Running security universe update ...")

            task_params = self._get_task_params('*')
            self._update_securities_universe(task_params)

            end_time = datetime.utcnow()
            with PipelineStageStore(self.run_id, self.date_only(), self.stage_id) as progress:
                progress.save_progress(start_time, end_time, "pipeline")
        else:
            self.logger.info("Prices adjustment pipeline hasn't completed yet, terminating ...")

    def _update_securities_universe(self, task_params):
        chunk_size = 1000  # 1000 (number of symbols to process in 1 batch when pulling CAPIQ)
        pull_data = True  # if True -> pull data (prices, CA, CAPIQ, RPack, ...). If false -> read data from cache
        path_local_cache = r'c:\DCM\Tasks\7_Extension_of_universe'  # path on local disk where cached data is stored

        drop_no_rp_comp_id = False  # if True -> drop records with no rp company id (e.g. ETFs)
        drop_etfs_not_in_etfg = False  # if True -> drop ETFs that are not chris/etfg/legacy_constituents/ universe
        drop_comp_name_simil_below_thresh = False  # if True -> drop records with company name similarity below thresh.
        min_thresh_match_str = 80.0  # minimum similarity score threshold [in %] for CAPIQ/TD company names vs RavenPack

        self._sec_univ_handler = SecurityUniverseHandler(task_params, pull_data, min_thresh_match_str,
                                                         drop_comp_name_simil_below_thresh, drop_no_rp_comp_id,
                                                         drop_etfs_not_in_etfg, chunk_size, path_local_cache)
        self._sec_univ_handler.update_security_universe()

    def _check_stage_status(self):
        with PipelineStageStore(run_id=self.run_id, run_date=self.date, stage_id=self.stage_id) as progress:
            return progress.stage_key_has_been_processed("pipeline")


def _main():
    import os
    from luigi import build as run_luigi_pipeline
    from pipelines.prices_ingestion.config import get_config

    get_config().put("security_master.dcm_config_repo_path", r"d:\dcm\dcm-config")

    file_dir = os.path.dirname(os.path.realpath(__file__))
    logging_conf_path = os.path.join(file_dir, "..", "..", "conf", "luigi-logging.conf")

    run_luigi_pipeline([SecurityUniverseGenerationPipeline()], local_scheduler=True,
                       logging_conf_file=logging_conf_path)


if __name__ == '__main__':
    _main()
