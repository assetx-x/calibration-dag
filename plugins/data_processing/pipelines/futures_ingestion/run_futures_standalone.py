from pipelines.futures_ingestion import FuturesIngestionPipeline
from luigi import build as run_luigi_pipeline

def _main():
    import os
    file_dir = os.path.dirname(os.path.realpath(__file__))
    logging_conf_path = os.path.join(file_dir, "..", "..", "conf", "luigi-logging.conf")

    run_luigi_pipeline([FuturesIngestionPipeline()], local_scheduler=True, logging_conf_file=logging_conf_path)


if __name__ == '__main__':
    _main()
