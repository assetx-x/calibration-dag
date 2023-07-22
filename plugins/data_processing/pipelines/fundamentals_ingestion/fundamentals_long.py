import json

import boto3
import pandas as pd
from luigi import Task

from base import DCMTaskParams, HdfStorageMixin, S3Target

pd.set_option('expand_frame_repr', False)

columns = ['metric', 'ticker', 'date', 'value', 'as_of_start', 'as_of_end', 'raw_value']


class FundamentalsLongTask(DCMTaskParams, HdfStorageMixin, Task):
    def __init__(self, *args, **kwargs):
        super(FundamentalsLongTask, self).__init__(*args, **kwargs)
        self.data_to_update = None
        self.records_to_retry = None
        self.new_records = None
        self.s3_bucket = "dcm-data-test"
        self.s3_dir = "boris/fundamentals_ingestion/%s/copy_to_redshift" % self.date_only()

    def run(self):
        self._load_merged_data()
        self._process_new_records()
        self._process_data_to_update()
        self._save_data_to_s3()
        self.logger.info("Breakpoint")
        self.output()["flag"].open("w").close()

    def _load_merged_data(self):
        self.data_to_update = self._load_df('data_to_update')
        self.records_to_retry = self._load_df('records_to_retry')
        self.new_records = self._load_df('new_records')

    def _process_new_records(self):
        cols = ['metric', 'ticker', 'date', 'value_new', 'as_of_start_new', 'as_of_end_new', 'raw_value_new']
        self.new_records = self.new_records[cols]
        self.new_records.columns = columns

    def _process_data_to_update(self):
        new_cols = ['metric', 'ticker', 'date', 'value_new', 'as_of_start_new', 'as_of_end_new', 'raw_value_new']
        new_records = self.data_to_update[new_cols]
        new_records.columns = columns
        self.new_records = pd.concat([self.new_records, new_records], ignore_index=True)

        upd_cols = ['metric', 'ticker', 'date', 'value_old', 'as_of_start_old', 'as_of_start_new', 'raw_value_old']
        self.data_to_update = self.data_to_update[upd_cols]
        self.data_to_update.columns = columns

    def _save_data_to_s3(self):
        if len(self.data_to_update.index) > 0:
            self._df_to_s3(self.data_to_update, "data_to_update", 10)
        if len(self.new_records.index) > 0:
            self._df_to_s3(self.new_records, "new_records", 20)

    def _df_to_s3(self, df, df_name, num_chunks):
        manifest_entries = []
        df_len = len(df.index)
        chunk_len = df_len / num_chunks
        chunks = [(start, min(start + chunk_len, df_len)) for start in range(0, df_len, chunk_len)]
        for chunk_idx, chunk in enumerate(chunks):
            chunk_url = "s3://%s/%s/%s.csv.%02d" % (self.s3_bucket, self.s3_dir, df_name, chunk_idx)
            self.logger.info("Saving chunk %s (%d:%d)" % (chunk_url, chunk[0], chunk[1]))
            with S3Target(chunk_url).open('w') as chunk_fd:
                df[chunk[0]: chunk[1]].to_csv(chunk_fd, header=False, index=False, encoding='utf-8')
            manifest_entries.append({"url": chunk_url, "mandatory": True})
        return self.create_manifest(df_name, manifest_entries)

    def create_manifest(self, df_name, manifest_entries):
        key = "%s/manifest-%s.json" % (self.s3_dir, df_name)
        manifest_s3_location = "s3://%s/%s" % (self.s3_bucket, key)
        self.logger.info("Creating manifest " + manifest_s3_location)
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=json.dumps({
                "entries": manifest_entries,
            }, indent=2, separators=(',', ': '))
        )
        return manifest_s3_location
