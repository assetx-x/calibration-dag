import json
from StringIO import StringIO

import boto3
import pandas as pd
from sqlalchemy import create_engine

from base import credentials_conf, TaskWithStatus
from pipelines.prices_ingestion.config import get_config
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHolder
from pipelines.prices_ingestion.etl_workflow_aux_functions import build_s3_url, store_data_in_s3


class AdjustmentFactorsProducer(object):
    def __init__(self, ticker, run_date, start_date, end_date):
        self.ticker = ticker
        self.run_date = run_date
        self.start_date = start_date
        self.end_date = end_date

    def create_adjustment_factors(self):
        factors_df = self._load_adjustment_factors()
        bucket, key = self._save_adjustment_factors_to_s3(factors_df)
        return build_s3_url(bucket, key)

    def _load_adjustment_factors(self):
        as_of_date = pd.Timestamp.now(tz="UTC")
        ca = S3TickDataCorporateActionsHolder(self.ticker, run_date=self.run_date, as_of_date=as_of_date,
                                              start_dt=pd.Timestamp(self.start_date),
                                              end_dt=pd.Timestamp(self.end_date))

        # ticker,cob,split_factor,split_div_factor,as_of_start,as_of_end
        factors_df = pd.concat([ca.split_adjustment_factor, ca.full_adjustment_factor], axis=1).reset_index()
        factors_df.columns = ["cob", "split_factor", "split_div_factor"]
        factors_df["cob"] = (factors_df["cob"] + pd.Timedelta('16 hours')).dt.tz_localize(tz='US/Eastern').dt.tz_convert("UTC")
        factors_df.insert(loc=0, column="ticker", value=pd.Series(self.ticker, index=factors_df.index))
        factors_df.loc[:, "as_of_start"] = pd.Series(as_of_date, index=factors_df.index)
        factors_df.loc[:, "as_of_end"] = pd.Series(None, index=factors_df.index, dtype=factors_df['as_of_start'].dtype)
        return factors_df

    def _save_adjustment_factors_to_s3(self, factors_df):
        csv_buffer = StringIO()
        factors_df.to_csv(csv_buffer, index=False)
        bucket_id = get_config()["raw_data_pull"]["base_data_bucket"]
        data_location = get_config()["adjustments"]["adjustment_factors"]
        return store_data_in_s3(csv_buffer, bucket_id, data_location, self.ticker, self.run_date)


class AdjustmentFactorsTask(TaskWithStatus):
    def collect_keys(self):
        file_locations = self._create_adjustment_factors()
        manifest_location = self._create_manifest(file_locations)
        self._load_data_into_redshift(manifest_location)

    def _create_adjustment_factors(self):
        file_locations = []
        for ticker in self.tickers:
            self.logger.info("Creating adjustment factors for %s", ticker)
            af = AdjustmentFactorsProducer(ticker, self.date, self.start_date, self.end_date)
            try:
                af_file_path = af.create_adjustment_factors()
                file_locations.append(af_file_path)
            except Exception as e:
                self.logger.error("Failed to produce adjustment factors for %s, exception is: %s", ticker, e)
        return file_locations

    def _create_manifest(self, manifest_entries):
        s3_bucket = get_config()["raw_data_pull"]["base_data_bucket"]
        data_location = get_config()["adjustments"]["adjustment_factors"]
        manifest_key = "%s/.manifest/manifest.json" % data_location
        manifest_location = build_s3_url(s3_bucket, manifest_key)
        self.logger.info("Creating manifest %s", manifest_location)
        s3 = boto3.client("s3")
        s3.put_object(Bucket=s3_bucket, Key=manifest_key,
                      Body=json.dumps({
                          "entries": [
                              {"url": s3_location, "mandatory": True}
                              for s3_location in manifest_entries
                              if s3_location]
                      }, indent=2, separators=(',', ': ')))
        return manifest_location

    def _get_aws_credentials(self):
        self.logger.info("Acquiring temporary credentials for redshift")
        sts = boto3.client("sts")
        try:
            credentials = sts.assume_role(
                RoleArn="arn:aws:iam::294659765478:role/slave",
                RoleSessionName="AdjustmentImportDaily"
            )
        except Exception:
            self.logger.warning("STS:AssumeRole failed, falling back to normal session token retrieval")
            credentials = sts.get_session_token()
        return credentials

    def _load_data_into_redshift(self, manifest_location):
        credentials = self._get_aws_credentials()
        format_params = {
            "manifest_location": manifest_location,
            "access_key": credentials["Credentials"]["AccessKeyId"],
            "secret_access_key": credentials["Credentials"]["SecretAccessKey"],
            "session_token": credentials["Credentials"]["SessionToken"],
            "adjustment_factors_table": "equity_adjustment_factors"
        }

        engine = create_engine("redshift+psycopg2://%s:%s@%s:%s/%s" % (
            credentials_conf["redshift"]["username"],
            credentials_conf["redshift"]["password"],
            credentials_conf["redshift"]["host"],
            credentials_conf["redshift"]["port"],
            credentials_conf["redshift"]["database"]
        ), echo=True)

        with engine.begin() as connection:
            connection.execute(
                """TRUNCATE TABLE {adjustment_factors_table}""".format(**format_params)
            )
            connection.execute(
                """
                COPY {adjustment_factors_table}
                FROM '{manifest_location}'
                WITH CREDENTIALS AS 'aws_access_key_id={access_key};aws_secret_access_key={secret_access_key};token={session_token}'
                dateformat 'auto'
                timeformat 'auto'
                ignoreheader 1
                acceptinvchars
                blanksasnull
                emptyasnull
                format csv
                encoding UTF8
                manifest
                """.format(**format_params)
            )
