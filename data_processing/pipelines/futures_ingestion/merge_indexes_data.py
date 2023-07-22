import json

import sqlalchemy as sa
from base import credentials_conf, TaskWithStatus
from pipelines.common.bitemporal_table import BitemporalTable


class MergeAdjustedIndexDataTask(TaskWithStatus):
    def __init__(self, *args, **kwargs):
        super(MergeAdjustedIndexDataTask, self).__init__(*args, **kwargs)
        self.redshift_engine = sa.create_engine("redshift+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["redshift"]["username"],
            credentials_conf["redshift"]["password"],
            credentials_conf["redshift"]["host"],
            credentials_conf["redshift"]["port"],
            credentials_conf["redshift"]["database"]
        ))

    def collect_keys(self):
        minute_files = []
        daily_files = []
        for adjust_task in self.input():
            if adjust_task["stage"].get_statuses()[0]["status_type"] == "Success":
                if "adjustment_data" in adjust_task:
                    minute_files.append(adjust_task["adjustment_data"].path)
                if "adjustment_daily_data" in adjust_task:
                    daily_files.append(adjust_task["adjustment_daily_data"].path)

        if minute_files:
            self._merge_files_into_redshift(minute_files, "index_prices", "manifest", "cob", ["ticker"])

        if daily_files:
            self._merge_files_into_redshift(daily_files, "daily_index_prices", "daily_manifest", "date", ["symbol"])

    def _merge_files_into_redshift(self, files_to_merge, table_key, manifest_key, cob_field, key_fields):
        self._write_manifest(files_to_merge, manifest_key)

        table_name = self.output()[table_key].table
        manifest_path = self.output()[manifest_key].path

        with self.redshift_engine.begin() as connection:
            bitemporal_table = BitemporalTable(connection, table_name, cob_field, key_fields)
            bitemporal_table.merge(manifest_path)

    def _write_manifest(self, files_to_merge, manifest_key):
        manifest_data = {
            "entries": [{"url": s3_location, "mandatory": True} for s3_location in files_to_merge]
        }
        with self.output()[manifest_key].open("w") as manifest_fd:
            manifest_fd.write(json.dumps(manifest_data))
