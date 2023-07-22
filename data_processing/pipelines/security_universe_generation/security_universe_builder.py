import os
import re
import time
import string
import logging
import requests
import itertools
import numpy as np
import pandas as pd

from zipfile import ZipFile
from fuzzywuzzy import fuzz
from functools import partial
from io import StringIO, BytesIO
from multiprocessing import Pool
from base import credentials_conf
from difflib import SequenceMatcher

from commonlib.market_timeline import marketTimeline
from pipelines.prices_ingestion.config import get_config
from pipelines.earnings_ingestion.capiq import CapIQClient
from pipelines.common.stage_store import PipelineStageStore
from pipelines.common.pipeline_util import get_postgres_engine
from pipeline_util import read_txt_file, store_txt_file, git_push, git_commit, git_pull
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHandler
from pipelines.security_universe_generation.security_master_from_ca_files import convert_cusip_to_nine_digits
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_redshift_engine, get_s3_client, save_to_s3

# Filter InsecureRequestWarning (due to used verify=False in requests.get())
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Logging settings
log_format = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s"
log_formatter = logging.Formatter(log_format, "%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger('SM')
_logger.setLevel(logging.DEBUG)


class UpdateSecurityUniverseStage(object):

    def __init__(self, run_date):
        """
        This Class has method price_adjustment_pipeline_completed() that checks whether adjustment pipeline is completed
        :param run_date: business date for equity price ingestion pipeline
        """
        self.run_date = run_date

    @staticmethod
    def get_prices_adjustment_stage_id(run_dt):
        return run_dt.strftime("%Y%m%d") + '-price-ingestion-adjust'

    # ====================================================================================
    # Get current DCM Universe status (security names, pending security changes, etc.)
    # ====================================================================================

    def price_adjustment_pipeline_completed(self):
        """
        This method is used to check whether the adjustment pipeline has been completed
        :return:
        """
        _logger.info(">>> Fetching status of prices adjustment pipeline from pipeline_stage_status table")
        stage_id = UpdateSecurityUniverseStage.get_prices_adjustment_stage_id(self.run_date)
        with PipelineStageStore(run_id=1, run_date=self.run_date, stage_id=stage_id) as progress:
            stage_completed = progress.stage_key_has_been_processed(key="pipeline")

        if stage_completed:
            message = "'completed'. Starting security_universe table update"
        else:
            message = "'incomplete'. Wait till prices adjustment pipeline completed"
        _logger.info(">>> Prices adjustment pipeline is %s" % message)
        return stage_completed


class SecurityUniverseHandler(object):

    # Mapper used to convert TickData exchange info to the CAPIQ format
    TD_TO_CIQ_EXCHANGE_MAP = {
        'NEW YORK STOCK EXCHANGE': 'NYSE',
        'NYSE ARCA EXCHANGE': 'ARCA',
        'NASDAQ GLOBAL SELECT MARKET': 'NasdaqGS',
        'NASDAQ GLOBAL MARKET': 'NasdaqGM',
        'NASDAQ CAPITAL MARKET': 'NasdaqCM',
        'BATS EXCHANGE': 'BATS',
        'AMERICAN STOCK EXCHANGE': 'AMEX',
        'NYSE American': 'AMEX',
        'The Investors Exchange LLC - IEX': 'IEX'
    }

    # Convert TickData Exchange to MIC
    TD_EXCHANGE_TO_MIC_MAP = {
        'ARCA': 'ARCX',
        'BATS': 'BATS',
        'NasdaqCM': 'XNCM',  # NASDAQ CAPITAL MARKET
        'NasdaqGS': 'XNGS',  # NASDAQ/NGS (GLOBAL SELECT MARKET)
        'NasdaqGM': 'XNMS',  # NASDAQ/NMS (GLOBAL MARKET)
        'AMEX': 'XASE',
        'NYSE': 'XNYS',
        'IEX': 'IEXG',
    }

    def __init__(self, task_params, pull_data=True, min_thresh_match_str=80.0, drop_comp_name_simil_below_thresh=False,
                 drop_no_rp_comp_id=False, drop_etfs_not_in_etfg=False, chunk_size=1000, path_local_cache="",
                 path_dcm_config_repo=None):
        """
        :param task_params: EquitiesETLTaskParameters (start_dt, as_of_dt, end_dt, run_dt)
        :param pull_data: if True -> run _pull_latest_tickdata_ca(), else -> read tickdata_ca_validated DF from disk
        :param min_thresh_match_str: min threshold for company names similarity (it is used for discarding tickers
                                     from SM due to quality issues)
        :param drop_comp_name_simil_below_thresh: if True -> drop tickers with similarity score < min_thresh_match_str
        :param drop_no_rp_comp_id: if True -> remove records that have no RP company id (e.g. ETFs)
        :param drop_etfs_not_in_etfg: if True -> remove ETF names from SM that are not in chris/etfg/legacy_constituents
        :param chunk_size: number of symbols to process in 1 batch when pulling CAPIQ
        :param path_local_cache: path to store intermediate results
        :param path_dcm_config_repo: path to dcm-config repo on local disk
        """

        # EquitiesETLTaskParameters
        self.start_dt = task_params.start_dt  # pd.Timestamp('2000-01-03')
        self.as_of_dt = task_params.as_of_dt  # pd.Timestamp.now()
        self.end_dt = task_params.end_dt  # marketTimeline.get_trading_day_using_offset(self.as_of_dt, -1)
        self.run_dt = task_params.run_dt  # self.end_dt

        # Parameters used in Security Universe update
        self.pull_data = pull_data
        self.min_thresh_match_str = min_thresh_match_str
        self.drop_comp_name_simil_below_thresh = drop_comp_name_simil_below_thresh
        self.drop_no_rp_comp_id = drop_no_rp_comp_id
        self.drop_etfs_not_in_etfg = drop_etfs_not_in_etfg
        self.chunk_size = chunk_size
        self.path_local_cache = path_local_cache
        self.path_dcm_config_repo = path_dcm_config_repo if path_dcm_config_repo \
            else get_config()["security_master.dcm_config_repo_path"]

    def __call__(self, ticker, auth):
        """
        This is a trick to let multiprocessing pool work when fetching CA from TickData API. The solution was found on:
        https://stackoverflow.com/questions/1816958/cant-pickle-type-instancemethod-when-using-multiprocessing-pool-map/21345273
        :param ticker: ticker name
        :param auth: credentials used to access TickData API (see get_tickdata_auth())
        :return: pass arguments to take_footer_only() method
        """
        return self.take_footer_only(ticker, auth)

    # ====================================================================================
    # Auxiliary functions to interact with TickData API
    # ====================================================================================

    @staticmethod
    def get_tickdata_auth():
        authentication = (get_config()["raw_data_pull"]["TickData"]["Price"]["username"],
                          get_config()["raw_data_pull"]["TickData"]["Price"]["password"])
        return authentication

    @staticmethod
    def create_tickdata_url(start_dt):
        tick_data_url = get_config()["raw_data_pull"]["TickData"]["Price"]["URL"] \
            .format(**{"start": start_dt.strftime("%m/%d/%Y")})
        return tick_data_url

    @staticmethod
    def create_tickdata_ca_url_with_asof(ticker, end_dt, as_of_dt):
        ca_url_template = r"https://tickapi.tickdata.com/events?COLLECTION_TYPE=COLLECTION_TYPE." \
                          r"US_TED&EXTRACT_TYPE=COLLECTOR_SUBTYPE_US_EVENTS&START_DATE={start}" \
                          r"&END_DATE={end}&REQUESTED_DATA={ticker}%5b{as_of}%5d&COMPRESSION_TYPE=NONE"
        ca_url_template = ca_url_template.format(**{"ticker": ticker,
                                                    "start": "01/03/2000",
                                                    "end": end_dt.strftime("%m/%d/%Y"),
                                                    "as_of": as_of_dt.strftime("%Y%m%d")})
        return ca_url_template

    # ====================================================================================
    # Generic auxiliary functions
    # ====================================================================================

    @staticmethod
    def describe_security_master(df):
        """
        This method print statistics on security master dataframe
        :param df: security master dataframe
        :return:
        """
        mask = (df['symbol'].duplicated(keep=False))
        duplicated_names = sorted(df.loc[mask, 'symbol'])

        mask = (df['rp_company_id'].duplicated(keep=False))
        duplicated_rp_comp_id = sorted(df.loc[mask & df['rp_company_id'].notnull(), 'rp_company_id'])

        mask = df.duplicated(subset=['symbol', 'rp_isin', 'rp_company_id'], keep=False)
        duplicates_to_check_name = sorted(df.loc[mask, 'symbol'].unique())

        _logger.info("Shape of SM: {0}".format(df.shape))
        _logger.info("Number of unique symbols in SM: %d" % len(set(df.symbol)))
        _logger.info("Number of symbols with no rp_company_id: %d" % df.loc[df['rp_company_id'].isnull()].shape[0])
        _logger.info("Number of ETFs in SM (CAPIQ Public Fund): %d" % df[df.company_type == 'Public Fund'].shape[0])
        _logger.info("Number of symbols with different isin CAPIQ vs RavenPack: %d" %
                     df.loc[(df['isin'] != df['rp_isin']) & (df['rp_isin'].notnull())].shape[0])
        _logger.info("Number of symbols with different exchanges TickData vs CAPIQ: %d" %
                     df.loc[(df['td_exchange'] != df['exchange'])].shape[0])
        _logger.info("Number of duplicated symbols in SM: %d (%d records). %s .." %
                     (len(set(duplicated_names)), len(duplicated_names), sorted(set(duplicated_names))[0:5]))
        _logger.info("Number of records having same symbol, rp_isin, rp_company_id but not rp_company_name: {0}. "
                     "Symbols: {1} ..".format(len(duplicates_to_check_name), duplicates_to_check_name[0:5]))
        _logger.info("Number of duplicated not null rp_company_id in SM: %d (%d records). %s .." %
                     (len(set(duplicated_rp_comp_id)), len(duplicated_rp_comp_id),
                      sorted(set(duplicated_rp_comp_id))[0:5]))
        _logger.info("Columns with nulls: %s" % list(df.isnull().sum().to_frame('nulls').query("nulls > 0").index))

    @staticmethod
    def get_dcm_universe():
        """
        This method read S3 chris/universe.txt
        :return: list with current dcm universe tickers
        """
        _logger.info(">>> Pulling DCM universe")
        s3_client = get_s3_client()
        body = s3_client.get_object(Bucket=get_config()['bucket'], Key="chris/universe.txt")['Body']
        universe_tickers = body.read().splitlines()
        _logger.info("Number of securities in DCM universe: {0}".format(len(universe_tickers)))
        return universe_tickers

    @staticmethod
    def get_security_universe_table():
        """
        This method read current security universe table from Redshift
        :return: pandas DF with current security_universe table
        """
        _logger.info(">>> Pulling security_universe table")
        security_universe_table = get_config()["security_master"]["security_universe_table"]
        sql_engine = get_redshift_engine()
        security_universe = pd.read_sql("SELECT * FROM %s" % security_universe_table, sql_engine)
        security_universe['dcm_security_id'] = security_universe['dcm_security_id'].astype(int)
        security_universe['ticker'] = security_universe['ticker'].map(lambda x: x.encode('ascii'))
        sql_engine.dispose()

        # Active tickers in security_universe table
        mask = (security_universe['is_active'] == True)
        _logger.info("Number of tickers in security_universe: Active [{0}] / Inactive [{1}]".format(
            security_universe.loc[mask].shape[0], security_universe.loc[~mask].shape[0]))
        return security_universe

    @staticmethod
    def get_pending_security_changes():
        """
        This method read Postgres table with pending security changes. These changes comes from two main sources:
        1) CA events: 'TICKER SYMBOL CHANGE', 'CUSIP CHANGE', 'CHANGE IN LISTING', 'NAME CHANGE'
        2) Company delisted / acquired: this info is not in the CA files but can be detected by using FOOTER information
        :return: pandas DF with pending security changes
        """
        pending_security_changes_table = get_config()["security_master"]["pending_security_changes_table"]
        start = time.time()
        _logger.info(">>> Reading pending security changes table")
        sql_engine = get_postgres_engine()
        sql_command = "SELECT * FROM {table_name}".format(table_name=pending_security_changes_table)
        pending_security_changes = pd.read_sql(sql_command, sql_engine)
        sql_engine.dispose()
        _logger.info("Done in : %.2f s" % (time.time() - start))

        if not pending_security_changes.empty:
            # Ticker is utf decoded -> encode back to ascii
            pending_security_changes['dcm_security_id'] = pending_security_changes['dcm_security_id'].astype(int)
            pending_security_changes['ticker'] = pending_security_changes['ticker'].map(lambda x: x.encode('ascii'))
        return pending_security_changes

    @staticmethod
    def _pull_etf_universe_names(col_names):
        _logger.info(">>> Pulling ETF universe from AWS S3")
        s3_client = get_s3_client()
        obj = s3_client.get_object(Bucket=get_config()['bucket'], Key="chris/etfg/20181031_constituents_export.csv")
        etfg_data = pd.read_csv(StringIO(obj['Body'].read()), header=None, parse_dates=[0], index_col=[0],
                                names=col_names)
        return etfg_data

    @staticmethod
    def save_updated_dcm_universe_to_s3(dcm_universe):
        """
        This method stores updated dcm universe.txt to S3 chris/universe.txt
        :return:
        """
        _logger.info(">>> Saving updated DCM universe.txt to S3")
        body = StringIO("\n".join(dcm_universe))
        return save_to_s3(body, s3_bucket=get_config()['bucket'], s3_key="chris/universe.txt")

    def filter_pending_security_changes(self, pending_security_changes, security_universe):
        """
        This method is used to filter pending security changes by using only the ones that
        have 'cob_date' > 'latest_update' (i.e. these that were not yet processed).
        :param pending_security_changes: pandas DF containing all pending security changes (both new and old)
        :param security_universe: pandas DF with current security_universe
        :return: filtered pandas DF with pending security changes
        """
        if not pending_security_changes.empty:
            # Merge 'latest_update' column from security_universe table into pending_security_changes on
            # 'dcm_security_id'
            pending_sec_changes_filt = pending_security_changes.merge(
                security_universe[['dcm_security_id', 'latest_update']], how='left', on='dcm_security_id', copy=True)

            # Selecting only pending security changes that have cob_date larger than last_update in
            # security_universe table
            mask = (pending_sec_changes_filt['cob_date'] > pending_sec_changes_filt['latest_update'])
            pending_sec_changes_filt = pending_sec_changes_filt.loc[mask]

            # If there are duplicates on dcm_security_id -> take the latest one (largest 'cob_date')
            pending_sec_changes_filt = pending_sec_changes_filt.sort_values(['dcm_security_id', 'cob_date'], ascending=[
                False, True]).drop_duplicates(subset=['dcm_security_id'], keep='last')
            return pending_sec_changes_filt
        return pending_security_changes

    def modify_dcm_universe(self, dcm_universe, tickers_to_drop, tickers_to_rename):
        """
        This method is used to filter original dcm universe.txt by removing tickers that should ne dropped and renaming
        these that had TICKER SYMBOL CHANGE corporate action event
        :param dcm_universe: list of tickers in original dcm universe
        :param tickers_to_drop: list of tickers to be dropped
        :param tickers_to_rename: dictionary with old and new symbols for tickers that were renamed
        :return: updated dcm universe
        """
        dcm_universe_new = list(set(dcm_universe) - set(tickers_to_drop))
        dcm_universe_new = map(lambda x: tickers_to_rename[x] if x in tickers_to_rename else x, dcm_universe_new)
        dcm_universe_new = sorted(set(dcm_universe_new))
        _logger.info("Size of universe.txt: original [%d] / modified [%d]" % (len(dcm_universe), len(dcm_universe_new)))
        return dcm_universe_new

    def update_universe_txt_github(self, tickers_to_drop, tickers_to_rename):
        """
        This function does the two following things:
        1) If there are any tickers to drop from universe -> it clones dcm-config github remote repo, deletes these
        tickers, commit changes and push them back to repo, afterwards deleting local cloned repo.
        2) If there are any tickers that have changed their symbol -> it clones dcm-config github remote repo,
        renames these tickers, commit changes and push them back to repo, afterwards deleting local cloned repo.
        :param tickers_to_drop: tickers that have been delisted / acquired ..
        :param tickers_to_rename: tickers that have changed their underlying symbols
        :return: updated universe.txt
        """
        _logger.info(">>> Updating dcm-config/data/universe/universe.txt on github")

        # Pulling latest changes from remote dcm-config repo
        if os.path.exists(self.path_dcm_config_repo):
            git_pull(self.path_dcm_config_repo)
        else:
            raise RuntimeError("Path %s does not exists" % self.path_dcm_config_repo)

        # Full path to universe.txt file
        filename = "universe.txt"
        universe_txt_file_path = os.path.join(self.path_dcm_config_repo, "data", "universe", filename)

        if os.path.exists(universe_txt_file_path):
            dcm_universe = read_txt_file(os.path.dirname(universe_txt_file_path), filename)
        else:
            raise IOError("File %s does not exists" % universe_txt_file_path)

        dcm_universe_new = []
        if len(tickers_to_drop) or len(tickers_to_rename):
            try:
                # Update dcm_universe.txt by dropping / modifying requested tickers
                dcm_universe_new = self.modify_dcm_universe(dcm_universe, tickers_to_drop, tickers_to_rename)

                # Saving modified universe.txt file to the cloned repo
                store_txt_file(dcm_universe_new, os.path.dirname(universe_txt_file_path), filename)

                # Commit and push the changes
                commit_message = ""
                if len(tickers_to_drop):
                    commit_message += "Removed {0}: {1}".format(len(tickers_to_drop), tickers_to_drop)
                if len(tickers_to_rename):
                    commit_message += " Renamed {0}: {1}".format(len(tickers_to_rename), tickers_to_rename)
                commit_message = commit_message.replace("'", "")  # removing "'" around ticker names: ['AAPL'] -> [AAPL]
                git_commit(commit_message, os.path.abspath(self.path_dcm_config_repo))
                git_push(os.path.abspath(self.path_dcm_config_repo))
            except:
                _logger.error("Could not update universe.txt on github")
        else:
            _logger.info(">>> Nothing to update in dcm-config/data/universe/universe.txt")
        return dcm_universe_new if len(dcm_universe_new) else dcm_universe

    def process_pending_security_changes(self, pending_security_changes):
        """
        This method processes pending security changes.
        1) It reads CA file from S3 [chris/tickdata_ca] for all tickers in pending_security_changes
        2) It checks whether the company has been delisted / acquired by comparing ACTION DATE in the FOOTER with the
        run_dt of the pipeline. If FOOTER date < run_dt -> it means that the company has been delisted / acquired.
        If so, it adds the ticker to tickers_to_drop
        3) It then checks what CA events occured for each ticker in pending security changes on run_dt (e.g. new name,
        new cusip, new exchange, new symbol).

        :param pending_security_changes: pandas DF with pending security changes due to 'TICKER SYMBOL CHANGE',
            'CUSIP CHANGE', 'CHANGE IN LISTING', 'NAME CHANGE' or Company Acquis. / De-listing
        :return: list of tickers_to_drop, tickers_to_rename, tickers_cusip_change
        """
        pending_security_changes_table = get_config()["security_master"]["pending_security_changes_table"]
        _logger.info(">>> Processing {} table".format(pending_security_changes_table))

        tickers_to_drop = []
        tickers_to_rename = {}
        tickers_cusip_change = []
        tickers_listing_change = []
        tickers_comp_name_change = []

        if not pending_security_changes.empty:
            for i, ticker in enumerate(pending_security_changes['ticker']):
                _logger.debug('#%d. Processing %s' % (i, ticker))

                # Get CA raw file from S3 and parse it
                ca_data_normalized, _ = S3TickDataCorporateActionsHandler._get_tickdata_ca(ticker, self.start_dt,
                                                                                           self.end_dt, self.as_of_dt)
                df_temp = ca_data_normalized.reset_index().copy()
                footer_date = df_temp.loc[df_temp['ACTION'] == 'FOOTER', 'ACTION DATE'].iloc[0]

                # De-activate tickers in security_universe that were delisted / acquired
                if footer_date < self.run_dt:  # check if security's FOOTER < run_date
                    _logger.info("Ticker '{0}' to be removed from universe.txt: FOOTER '{1}' < run_dt '{2}'".format(
                        ticker, footer_date.date(), self.run_dt.date()))
                    tickers_to_drop.append(ticker)
                    continue

                # Get last update of the security
                mask = (pending_security_changes['ticker'] == ticker)
                latest_update = pending_security_changes.loc[mask, 'latest_update'].iloc[0]

                # Mask to search events that affect Security Master and have occured at run_dt
                ca_columns_affecting_sm = ['TICKER SYMBOL CHANGE', 'CUSIP CHANGE', 'CHANGE IN LISTING', 'NAME CHANGE']
                mask = (df_temp['ACTION'].isin(ca_columns_affecting_sm)) & (df_temp['ACTION DATE'] > latest_update)
                df_temp = df_temp.loc[mask]

                # Check if security have any CA on run_date that could affect Security Master
                if not df_temp.empty:
                    for action in df_temp['ACTION']:
                        if action == 'TICKER SYMBOL CHANGE':
                            # TODO: add check that new symbol has no punctuation - if it does it should be added to
                            #  ticker_mapping table
                            action_dt = df_temp.loc[df_temp['ACTION'] == action, 'ACTION DATE'].iloc[0]
                            old_symbol = df_temp.loc[df_temp['ACTION'] == action, 'OLD VALUE'].iloc[0]
                            new_symbol = df_temp.loc[df_temp['ACTION'] == action, 'NEW VALUE'].iloc[0]
                            tickers_to_rename[old_symbol] = new_symbol
                            _logger.info("Ticker '{0}'. Old symbol '{1}' changed to '{2}' due to {3} on '{4}'".format(
                                ticker, old_symbol, new_symbol, action, action_dt.date()))

                        if action == 'NAME CHANGE':
                            action_dt = df_temp.loc[df_temp['ACTION'] == action, 'ACTION DATE'].iloc[0]
                            old_company_name = df_temp.loc[df_temp['ACTION'] == action, 'OLD VALUE'].iloc[0]
                            new_company_name = df_temp.loc[df_temp['ACTION'] == action, 'NEW VALUE'].iloc[0]
                            tickers_comp_name_change.append(ticker)
                            _logger.info("Ticker '{0}'. Old company name '{1}' changed to '{2}' due "
                                         "to {3} on '{4}'".format(ticker, old_company_name, new_company_name,
                                                                  action, action_dt.date()))

                        if action == 'CUSIP CHANGE':
                            action_dt = df_temp.loc[df_temp['ACTION'] == action, 'ACTION DATE'].iloc[0]
                            old_cusip = df_temp.loc[df_temp['ACTION'] == action, 'OLD VALUE'].iloc[0]
                            new_cusip = convert_cusip_to_nine_digits(df_temp.loc[df_temp['ACTION'] == action,
                                                                                 'NEW VALUE'].iloc[0])
                            tickers_cusip_change.append(ticker)
                            _logger.info("Ticker '{0}'. Old CUSIP '{1}' changed to '{2}' due to {3} on '{4}'".format(
                                ticker, old_cusip, new_cusip, action, action_dt.date()))

                        if action == 'CHANGE IN LISTING':
                            action_dt = df_temp.loc[df_temp['ACTION'] == action, 'ACTION DATE'].iloc[0]
                            old_exchange = df_temp.loc[df_temp['ACTION'] == action, 'OLD VALUE'].iloc[0]
                            new_exchange = df_temp.loc[df_temp['ACTION'] == action, 'NEW VALUE'].iloc[0]
                            tickers_listing_change.append(ticker)
                            _logger.info("Ticker '{0}'. Old Exchange '{1}' changed to '{2}' due "
                                         "to {3} on '{4}'".format(ticker, old_exchange, new_exchange,
                                                                  action, action_dt.date()))

        return tickers_to_drop, tickers_to_rename, tickers_comp_name_change, tickers_cusip_change, tickers_listing_change

    def get_tickers_to_add_activ_deactiv_in_sec_univ(self, dcm_universe, security_universe, tickers_to_rename):
        """
        This method is used to prepare list of tickers that should be activated/deactivated in security_universe as
        well as those that need to be added (due to manually added names to universe.txt or because of the ticker
        symbol change).
        :param dcm_universe: list of tickers in updated universe.txt (according to pending_security_changes)
        :param security_universe: pandas DF with current security universe from Redshift
        :param tickers_to_rename:
        :return: list of tickers_add_to_sec_univ; dict with dcm_security_id of tickers to be activated/deactivated
        """
        security_universe_table = get_config()["security_master"]["security_universe_table"]
        _logger.info(">>> Preparing tickers to be added/activated/deactivated in {0} table".format(
            security_universe_table))

        # Mask below is used to find active / inactive tickers
        active_tickers_mask = (security_universe['is_active'] == True)

        # Filter from the new dcm universe names that are modified due to ticker symbol change -> these names should
        # not be added as a new records to the security_universe but updated
        dcm_universe_excl_tick_with_rename = sorted(set(dcm_universe).difference(set(tickers_to_rename.values())))

        # ====================================================================================================
        # Tickers that are in universe.txt but not in security universe table
        # ====================================================================================================
        # Difference between updated universe.txt and current security_universe table
        tickers_add_to_sec_univ = sorted(set(dcm_universe_excl_tick_with_rename).difference(
            set(security_universe['ticker'])))
        _logger.info("{0} tickers to be added to {1} table: {2}".format(
            len(tickers_add_to_sec_univ), security_universe_table, tickers_add_to_sec_univ))

        # ====================================================================================================
        # Tickers that are in universe.txt and in security universe table but inactive (to be activated)
        # ====================================================================================================
        tickers_to_activ_in_sec_univ = sorted(set(security_universe.loc[~active_tickers_mask, 'ticker']).intersection(
            set(dcm_universe_excl_tick_with_rename)))

        # Take dcm_security_id for these tickers: it is safer to operate with dcm_security_id rather than tickers since
        # there can be a situation when there are two equal tickers with different dcm_security_id (e.g. one has
        # is_active = True, another is_active = False).
        mask = (~active_tickers_mask) & (security_universe['ticker'].isin(tickers_to_activ_in_sec_univ))
        tickers_to_activ_in_sec_univ = dict(security_universe.loc[mask, ['ticker', 'dcm_security_id']].values)
        _logger.info("{0} tickers to be activated in {1} table: {2}".format(
            len(tickers_to_activ_in_sec_univ), security_universe_table, tickers_to_activ_in_sec_univ))

        # ====================================================================================================
        # Tickers that are in security universe table [active] but not in universe.txt (to be de-activated)
        # ====================================================================================================
        tickers_to_deactiv_in_sec_univ = sorted(set(security_universe.loc[active_tickers_mask, 'ticker']).difference(
            set(dcm_universe)))

        # Exclude from the list of tickers to be de-activated these that had a ticker symbol change CA
        tickers_to_deactiv_in_sec_univ = sorted(set(tickers_to_deactiv_in_sec_univ).difference(
            set(tickers_to_rename.keys())))

        # Take dcm_security_id for these tickers: it is safer to operate with dcm_security_id rather than tickers since
        # there can be a situation when there are two equal tickers with different dcm_security_id (e.g. one has
        # is_active = True, another is_active = False).
        mask = (active_tickers_mask) & (security_universe['ticker'].isin(tickers_to_deactiv_in_sec_univ))
        tickers_to_deactiv_in_sec_univ = dict(security_universe.loc[mask, ['ticker', 'dcm_security_id']].values)
        _logger.info("{0} tickers to be de-activated in {1} table: {2}".format(
            len(tickers_to_deactiv_in_sec_univ), security_universe_table, sorted(tickers_to_deactiv_in_sec_univ.keys())))

        return tickers_add_to_sec_univ, tickers_to_activ_in_sec_univ, tickers_to_deactiv_in_sec_univ

    def activ_deactiv_tickers_in_security_universe(self, tickers_to_activ_in_sec_univ, tickers_to_deactiv_in_sec_univ):
        """
        This method does activate / deactivate tickers in security_universe table on Redshift
        :param tickers_to_activ_in_sec_univ: tickers that are in universe.txt and in security univ. table but inactive
        :param tickers_to_deactiv_in_sec_univ: tickers that are in security univ. table [active] but not in universe.txt
        :return:
        """
        security_universe_table = get_config()["security_master"]["security_universe_table"]
        update_time = pd.Timestamp.now('UTC').tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')  # "latest_update" column

        queries_to_upd_sec_univ = []
        if len(tickers_to_deactiv_in_sec_univ):
            _logger.info(">>> Activating/deactivating tickers in {0} table".format(security_universe_table))
            for ticker, dcm_security_id in tickers_to_deactiv_in_sec_univ.items():
                # Query to de-activate ticker in security_universe
                query = "UPDATE {table_name} SET is_active={is_active}, latest_update='{time}' " \
                        "WHERE dcm_security_id={dcm_security_id};".format(
                    table_name=security_universe_table, is_active=False, time=update_time,
                    dcm_security_id=dcm_security_id)
                queries_to_upd_sec_univ.append(query)

        if len(tickers_to_activ_in_sec_univ):
            for ticker, dcm_security_id in tickers_to_activ_in_sec_univ.items():
                # Query to activate ticker in security_universe
                query = "UPDATE {table_name} SET is_active={is_active}, latest_update='{time}' " \
                        "WHERE dcm_security_id={dcm_security_id};".format(
                    table_name=security_universe_table, is_active=True, time=update_time,
                    dcm_security_id=dcm_security_id)
                queries_to_upd_sec_univ.append(query)

        if len(queries_to_upd_sec_univ):  # updating security_universe table [Redshift]
            queries_to_upd_sec_univ = '\n'.join(queries_to_upd_sec_univ)  # concat all queries into singe query
            sa_engine = get_redshift_engine()
            with sa_engine.begin() as connection:
                connection.execute(queries_to_upd_sec_univ)
            sa_engine.dispose()
            _logger.info("The {0} table is updated on '{1}'. Activated: {2}. De-activated: {3}.".format(
                security_universe_table, self.run_dt.date(), sorted(tickers_to_activ_in_sec_univ.keys()),
                sorted(tickers_to_deactiv_in_sec_univ.keys())))

    # ====================================================================================
    # Pull TickData Universe (based on Equity Prices)
    # ====================================================================================

    def _pull_latest_tickdata_universe(self, filename):
        """
        This method send query to TickData API that returns ZIP file with prices for all TickData universe on run_dt.
        Based on this information one constructs TickData universe.
        :param filename: name of file where to store pulled TickData universe
        :return: TickData universe on run_dt
        """
        _logger.info(">>> Pulling tickdata universe as of run_dt='%s'" % str(self.run_dt.date()))
        prices_url = self.create_tickdata_url(self.run_dt)
        data_request = requests.get(prices_url, auth=self.get_tickdata_auth(), verify=False)
        prices_data = ZipFile(BytesIO(data_request.content))

        # Taking names of tickers from names of zip files
        tickdata_universe = sorted([k.encode().split('_')[0] for k in prices_data.namelist()])
        _logger.info("Number of tickers in tickdata universe on %s: %d "
                     "[ca not validated]" % (str(self.run_dt.date()), len(tickdata_universe)))

        # Saving names to disk
        if self.pull_data and os.path.exists(self.path_local_cache):
            with open(os.path.join(self.path_local_cache, filename), 'w') as f:
                f.write('\n'.join(tickdata_universe))

        return tickdata_universe

    def get_latest_tickdata_universe(self):
        """
        This method get list of tickers in TickData universe on run_dt. Note: if self.pull_data = True ->
        _pull_latest_tickdata_universe() will query TickData API and construct tickdata universe on run_dt.
        If False -> read TickData universe from disk
        :return: list of tickers in TickData universe on run_dt
        """
        filename = 'td_universe_ca_not_validated.csv'
        start = time.time()
        if not self.pull_data and os.path.exists(os.path.join(self.path_local_cache, filename)):
            _logger.info(">>> Reading TickData universe from disk as of run_dt='%s'" % str(self.run_dt.date()))
            tickdata_universe = pd.read_csv(os.path.join(self.path_local_cache, filename),
                                            header=None).values.ravel().tolist()
        else:
            tickdata_universe = self._pull_latest_tickdata_universe(filename)
        _logger.info("Done in : %.2f s" % (time.time() - start))
        return tickdata_universe

    # ====================================================================================
    # Pull TickData Corporate Actions
    # ====================================================================================

    def take_footer_only(self, ticker, auth):
        """
        This method collects TickData CA events data for given list of tickers. It reads only FOOTER information
        which then is used to verify that all tickers are valid (e.g. company is not delisted, acquired). This method
        is called from _pull_latest_tickdata_ca() using multi-processing Pool (so to speed-up rate of requests to
        TickData API).
        :param ticker: ticker name
        :param auth: credentials used to access TickData API (see get_tickdata_auth())
        :return: FOOTER record from CA file for given ticker, run_dt, as_of_dt
        """
        _logger.info(ticker)
        url = self.create_tickdata_ca_url_with_asof(ticker, self.run_dt, self.as_of_dt)
        try:
            data_request = requests.get(url, auth=auth)
        except requests.exceptions.RequestException as e:
            _logger.error("Ticker: %s. Error: %s" % (ticker, e))
            time.sleep(3)  # sleep 3 seconds and retry
            data_request = requests.get(url, auth=auth)

        _logger.debug(ticker)
        if data_request.ok:
            content = data_request.content
            if content != '':  # to exclude cases with no data in ca file
                try:
                    footer_record = content.decode('utf-8').split('\n')[-2].split('\r')[0]  # FOOTER
                    return footer_record.startswith('FOOTER') and footer_record
                except IndexError:
                    pass
        return

    def _pull_latest_tickdata_ca(self, tickdata_universe, filename, pool_size):
        """
        This method collects TickData CA FOOTER data for given list of tickers on run_dt.
        :param tickdata_universe: list of tickers in TickData universe on run_dt
        :param filename: name of file where to store pulled TickData CA FOOTER information
        :param pool_size: size of multiprocessing pool when pulling CA data from TickData (max. 12)
        :return: pandas DF containing FOOTER inform. from TickData CA files for tickers in tickdata_universe on run_dt
        """
        _logger.info(">>> Pulling TickData CA information as of run_dt='%s'" % str(self.run_dt.date()))
        p = Pool(pool_size)
        tickdata_ca_validated = p.map(partial(self, auth=self.get_tickdata_auth()), tickdata_universe)
        p.terminate()
        p.join()

        # Filtering out None records (if any)
        tickdata_ca_validated = list(filter(lambda x: x is not None, tickdata_ca_validated))
        _logger.info("Number of tickers in TickData universe on %s: %d "
                     "[ca validated]" % (str(self.run_dt.date()), len(tickdata_ca_validated)))

        # Creating DF out of FOOTER CA data
        footer_cols = ['metadata', 'td_security_id', 'td_end_dt', 'td_symbol', 'td_company_name', 'cusip', 'td_exchange']
        tickdata_ca_validated = pd.DataFrame([(k.split(',')) for k in tickdata_ca_validated], columns=footer_cols) \
            .drop('metadata', axis=1).sort_values('td_symbol').reset_index(drop=True)

        # Saving CA data [FOOTER only] for TickData universe (!! does not contain tickers with no or empty CA files)
        if self.pull_data and os.path.exists(self.path_local_cache):
            tickdata_ca_validated.to_csv(os.path.join(self.path_local_cache, filename), index=False)
        return tickdata_ca_validated

    def get_latest_tickdata_ca(self, tickdata_universe):
        """
        This method collects TickData CA FOOTER data for given list of tickers on run_dt. This information is then used
        to verify that all tickers are valid and not e.g. delisted, acquired ...
        :param tickdata_universe: list of tickers in TickData universe on run_dt
        :return: pandas DF with TickData CA FOOTER data for given tickdata_universe tickers on run_dt.
        """
        filename = 'td_ca_footer_info_validated.csv'

        # Size of multiprocessing pool when pulling CA data from TickData (max. 12, conservative max -> 10)
        # 1..10 -> 1, 11..20 -> 2, 21..30 -> 3, etc. Max. 10
        pool_size = min((int(np.ceil(len(tickdata_universe) / 10.))), 10)

        start = time.time()
        if not self.pull_data and os.path.exists(os.path.join(self.path_local_cache, filename)):
            _logger.info(">>> Reading TickData CA information from disk as of run_dt='%s'" % str(self.run_dt.date()))
            tickdata_ca_validated = pd.read_csv(os.path.join(self.path_local_cache, filename))
        else:
            tickdata_ca_validated = self._pull_latest_tickdata_ca(tickdata_universe, filename, pool_size)
        _logger.info("Done in : %.2f s" % (time.time() - start))
        return tickdata_ca_validated

    # ====================================================================================
    # Filtering TickData Universe names
    # ====================================================================================

    def filter_tickdata_universe(self, tickdata_ca_validated):
        """
        This method filters TickData universe by removing symbols that contain '_PR', '_W', '_F'.
        :param tickdata_ca_validated: pandas DF with TickData CA FOOTER data for tickdata_universe tickers on run_dt.
        :return: filtered pandas DF with TickData CA FOOTER data
        """
        start = time.time()
        _logger.info(">>> Filtering TickData CA info for tickers containing '_PR', '_W', '_F'")
        filtered_tickdata_ca = tickdata_ca_validated.copy()

        # Filtering symbols that contains the following
        symbols_to_filter = ['_W', '_F', '_PR']
        for symbol in symbols_to_filter:
            filtered_tickdata_ca = filtered_tickdata_ca.loc[~filtered_tickdata_ca['td_symbol'].str.contains(symbol)]
            filtered_tickdata_ca.reset_index(drop=True, inplace=True)
        _logger.info('Tickers filtered: %d' % (tickdata_ca_validated.shape[0] - filtered_tickdata_ca.shape[0]))
        _logger.info("Done in : %.2f s" % (time.time() - start))

        # Saving info about filtered TickData universe
        if self.pull_data and os.path.exists(self.path_local_cache):
            filename = 'td_ca_footer_info_validated_filtered.csv'
            filtered_tickdata_ca.to_csv(os.path.join(self.path_local_cache, filename), index=False)
        return filtered_tickdata_ca

    # ====================================================================================
    # Pull CAPIQ data
    # ====================================================================================

    def _pull_capiq_data(self, td_ca_data_valid_filtered, filename):
        """
        This method pulls CAPIQ data for given list of tickers.
        :param td_ca_data_valid_filtered:
        :param filename: name of file where to store pulled CAPIQ information
        :return: pandas DF with CAPIQ data
        """

        def _apply_template_nd(template, ciq_ticker, metric):
            key = "{ciq_ticker};{metric}".format(ciq_ticker=ciq_ticker, metric=metric)
            formula = template.format(ciq_ticker=ciq_ticker, metric=metric)
            return key, formula

        def _ciq_nd(ciq_ticker, metric):
            return _apply_template_nd("""=CIQ("{ciq_ticker}", "{metric}")""", ciq_ticker, metric)

        def _merge_two_dicts(x, y):
            z = x.copy()  # start with x's keys and values
            z.update(y)  # modifies z with y's keys and values & returns None
            return z

        def _postprocess_response(response):
            tree = {}
            for key, val in response.items():
                t = tree
                prev = None
                for part in key.split(';'):
                    if prev is not None:
                        t = t.setdefault(prev, {})
                    prev = part
                else:
                    t.setdefault(prev, val)
            return tree

        def _chunker(seq, size):
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        # Columns of interest from CAPIQ
        capiq_cols = ["IQ_COMPANY_ID", "IQ_COMPANY_NAME", "IQ_EXCHANGE", "IQ_INDUSTRY", "IQ_INDUSTRY_SECTOR",
                      "IQ_ISIN", "IQ_TRADING_CURRENCY", "IQ_COMPANY_TYPE", "IQ_SECURITY_NAME",
                      "IQ_COUNTRY_NAME", "IQ_TRADING_ITEM_CIQID", "IQ_SECURITY_ITEM_CIQID"]

        # Create CapIQClient
        client = CapIQClient(credentials_conf["capiq"]["username"], credentials_conf["capiq"]["password"])
        client.login()

        capiq_data = {}
        list_of_requests = []

        # ====================================================================
        # Retrieve CAPIQ data for TickData validated and filtered universe
        # ====================================================================

        _logger.info(">>> Retrieving CAPIQ data for %d tickers" % (td_ca_data_valid_filtered.shape[0]))
        for i, group in enumerate(_chunker(list(td_ca_data_valid_filtered['td_symbol']), self.chunk_size)):
            for ticker in group:
                ticker = ticker.replace('_', '.') if '_' in ticker else ticker
                for col in capiq_cols:
                    list_of_requests.append(_ciq_nd(ticker, col))
            start = time.time()
            capiq_response = client.request(dict(list_of_requests))
            _logger.info("Processed chunk #%d [size=%d symbols]. Done in : %.2f s" % ((i + 1), len(group),
                                                                                      (time.time() - start)))
            capiq_data = _merge_two_dicts(capiq_data, _postprocess_response(capiq_response))
            list_of_requests = []
        capiq_data_before_exch_adj = capiq_data.copy()  # this is used in logs below

        # ====================================================================
        # Handling situation when CAPIQ exchange differs from TickData
        # ====================================================================

        # Convert TickData exchange info to the CAPIQ format
        td_ca_data_valid_filtered['td_exchange'] = td_ca_data_valid_filtered['td_exchange'].map(
            lambda x: self.TD_TO_CIQ_EXCHANGE_MAP[x] if x in self.TD_TO_CIQ_EXCHANGE_MAP else x)

        # Set is needed since there might be duplicates (e.g. there are 2 'AMEX' values in the mapper)
        exchange_mapper_values = sorted(set(self.TD_TO_CIQ_EXCHANGE_MAP.values()))

        tickers_wrong_exchange = {}
        for ciq_symbol, ciq_record in capiq_data.items():
            if ciq_record['IQ_EXCHANGE'] not in exchange_mapper_values:
                td_exchange = td_ca_data_valid_filtered.loc[td_ca_data_valid_filtered['td_symbol'] == \
                                                            ciq_symbol.replace('.', '_'), 'td_exchange'].iloc[0]
                ticker = ':'.join([td_exchange, ciq_symbol])
                ticker = ticker.replace('_', '.') if '_' in ticker else ticker
                tickers_wrong_exchange[ticker] = ciq_symbol
                for col in capiq_cols:
                    list_of_requests.append(_ciq_nd(ticker, col))

        if bool(list_of_requests):
            start = time.time()
            _logger.info("Processing requests for tickers with wrong exchange [size=%d symbols]"
                         % (len(tickers_wrong_exchange)))
            capiq_response = client.request(dict(list_of_requests))
            _logger.info("Done in : %.2f s" % (time.time() - start))
            capiq_response = _postprocess_response(capiq_response)
            for symbol in capiq_response.keys():
                if symbol in tickers_wrong_exchange:
                    capiq_response[tickers_wrong_exchange[symbol]] = capiq_response.pop(symbol)
                else:
                    _logger.warning('{} not in tickers_wrong_exchange'.format(symbol))
            capiq_data = _merge_two_dicts(capiq_data, capiq_response)
            list_of_requests = []

        # ====================================================================
        # Handling situation when no ISIN (retry request with diff. exchange
        # ====================================================================

        tickers_no_isin = {}  # dict for mapping back generated names with all exchanges to original symbols
        for ciq_symbol, ciq_record in capiq_data.items():
            if ciq_record['IQ_ISIN'] is None or ciq_record['IQ_ISIN'] == '(Invalid Identifier)':
                for element in itertools.product(*[exchange_mapper_values, [ciq_symbol]]):
                    ticker = ':'.join(element)
                    tickers_no_isin[ticker] = ciq_symbol
                    for col in capiq_cols:
                        list_of_requests.append(_ciq_nd(ticker, col))

        if bool(list_of_requests):
            start = time.time()
            _logger.info("Processing requests for symbols with no ISIN [size=%d symbols]"
                         % (len(set(tickers_no_isin.values()))))
            capiq_response = client.request(dict(list_of_requests))
            _logger.info("Done in : %.2f s" % (time.time() - start))
            capiq_response = _postprocess_response(capiq_response)

            symbols_successful_lookup = []
            # Avoiding RuntimeError: dictionary changed size during iteration
            capiq_response_copy = capiq_response.copy()
            for ciq_symbol, ciq_record in capiq_response_copy.items():
                if ciq_record['IQ_ISIN'] != '(Invalid Identifier)' and ciq_record['IQ_ISIN'] is not None:
                    original_symbol = tickers_no_isin[ciq_symbol]
                    td_exchange = td_ca_data_valid_filtered.loc[td_ca_data_valid_filtered['td_symbol'] ==
                                                                original_symbol.replace('.', '_'), 'td_exchange'].iloc[0]
                    _logger.debug('Response info [{0}]: {1} [{2}]. After fix exch. data: {3} [{4}]. Before fix exch: '
                                  '{5} [{6}]'.format(original_symbol, ciq_symbol, ciq_record['IQ_ISIN'],
                                                     td_exchange, capiq_data[original_symbol]['IQ_ISIN'],
                                                     capiq_data_before_exch_adj[original_symbol]['IQ_EXCHANGE'],
                                                     capiq_data_before_exch_adj[original_symbol]['IQ_ISIN']))
                    capiq_data[original_symbol] = capiq_response.pop(ciq_symbol)
                    symbols_successful_lookup.append(original_symbol)

            if bool(symbols_successful_lookup):
                _logger.info("ISIN fixed for %d symbols" % len(symbols_successful_lookup))
                _logger.info("Symbols with no ISIN remaining: %d" % (len(set(tickers_no_isin.values())) -
                                                                     len(symbols_successful_lookup)))
        client.logout()

        # Saving CAPIQ results
        capiq_data = pd.DataFrame(capiq_data).T.reset_index().rename(columns={'index': 'SYMBOL'}).sort_values('SYMBOL')
        capiq_data.columns = capiq_data.columns.map(lambda x: x.split('IQ_')[1].lower() if 'IQ_' in x else x.lower())

        # Replacing '(Invalid Identifier)' with pd.np.nan
        for col in capiq_data.columns:
            capiq_data[col] = capiq_data[col].apply(lambda x: pd.np.nan if x == '(Invalid Identifier)' or x is None else x)
        rename_dict = {
            'company_id': 'ciq_company_id',
            'company_name': 'ciq_company_name',
            'security_item_ciqid': 'ciqid_security_item',
            'trading_item_ciqid': 'ciqid_trading_item'
        }
        capiq_data.rename(columns=rename_dict, inplace=True)

        # Save to local disk
        if self.pull_data and os.path.exists(self.path_local_cache):
            capiq_data.to_csv(os.path.join(self.path_local_cache, filename), header=True, index=False, encoding='utf-8')
        return capiq_data

    def get_capiq_data(self, td_ca_data_valid_filtered):
        """
        This method pulls CAPIQ data for given list of tickers.
        :param td_ca_data_valid_filtered: list of tickers for which CAPIQ data to be pulled
        :return: pandas DF with CAPIQ data
        """
        filename = 'capiq_data.csv'
        start = time.time()
        if not self.pull_data and os.path.exists(os.path.join(self.path_local_cache, filename)):
            _logger.info(">>> Reading CAPIQ data from disk as of '%s'" % str(self.run_dt.date()))
            capiq_data = pd.read_csv(os.path.join(self.path_local_cache, filename))
        else:
            capiq_data = self._pull_capiq_data(td_ca_data_valid_filtered, filename)
        _logger.info("Done in : %.2f s" % (time.time() - start))
        return capiq_data

    # ====================================================================================
    # Postprocess and merge TickData and CAPIQ data
    # ====================================================================================

    def postprocess_tickdata_and_capiq_data(self, tickdata_data, capiq_data):
        """
        This method is used to postprocess tickdata and capiq data. It adds some missing data (e.g. capiq ISINs),
        but also brings capiq and tickdata information to same outlook (by normalizing symbol, converting cusip
        to 9 digits, etc.)
        :param tickdata_data: pandas DF with tickdata information
        :param capiq_data: pandas DF with capiq information
        :return: post-processed tickdata_data, capiq_data pandas DFs
        """
        def _add_missing_capiq_isin(df):
            lookup_nan_data = {
                'ITG': {'isin': 'I_US46145F1057', 'ciq_company_name': 'Investment Technology Group, Inc.',
                        'ciq_company_id': 'IQ336687', 'ciqid_trading_item': 'IQT2622362'},
                'NXEO': {'isin': 'I_US65342H1023', 'ciq_company_name': 'Nexeo Solutions, Inc.',
                         'ciq_company_id': 'IQ263587669', 'ciqid_trading_item': 'IQT269729687'},
                'REN': {'isin': 'I_US76116A3068', 'ciq_company_name': 'Resolute Energy Corporation',
                        'ciq_company_id': 'IQ34970103', 'ciqid_trading_item': 'IQT69036339'},
                'SPA': {'isin': 'I_US8472351084', 'ciq_company_name': 'Sparton Corporation',
                        'ciq_company_id': 'IQ304383', 'ciqid_trading_item': 'IQT2651878'},
                'SXE': {'isin': 'I_US84130C1009', 'ciq_company_name': 'Southcross Energy Partners, L.P.',
                        'ciq_company_id': 'IQ204710830', 'ciqid_trading_item': pd.np.nan},
                'TLP': {'isin': 'I_US89376V1008', 'ciq_company_name': 'Transmontaigne Partners L.P.',
                        'ciq_company_id': 'IQ20927496', 'ciqid_trading_item': 'IQT22472345'}
            }
            mask = df['isin'].isnull()
            for key in ['isin', 'ciq_company_name', 'ciq_company_id', 'ciqid_trading_item']:
                df.loc[mask, key] = df.loc[mask, 'symbol'].map(
                    lambda x: lookup_nan_data[x][key] if x in lookup_nan_data else pd.np.nan)
            return df

        start = time.time()
        _logger.info(">>> Post-processing TickData and CAPIQ data")

        # TickData
        tickdata_data['symbol'] = tickdata_data['td_symbol'].apply(lambda x: re.sub('[^0-9a-zA-Z]+', '', x))  # ('_' -> '')
        tickdata_data['cusip'] = tickdata_data['cusip'].apply(convert_cusip_to_nine_digits)

        # CAPIQ
        capiq_data['ciq_symbol'] = capiq_data['symbol'].copy()  # creating column with original symbol from CAPIQ
        capiq_data['symbol'] = capiq_data['symbol'].apply(lambda x: re.sub('[^0-9a-zA-Z]+', '', x))

        # Set ISINs taken from external source to the records with no CAPIQ ISIN (selected names)
        capiq_data = _add_missing_capiq_isin(capiq_data)
        capiq_data['ciq_isin'] = capiq_data['isin'].copy()  # creating column with original ISIN from CAPIQ
        capiq_data['isin'] = capiq_data['isin'].str.split('_').str[1]

        _logger.info("Shape of TickData / CAPIQ: {0} {1}".format(tickdata_data.shape, capiq_data.shape))
        _logger.info("Done in : %.2f s" % (time.time() - start))
        return tickdata_data, capiq_data

    def merge_tickdata_and_capiq_data(self, tickdata_data, capiq_data):
        """
        This method merges tickdata_data and capiq_data on ['symbol'].
        :param tickdata_data: pandas DF with TickData information (after postprocess_tickdata_and_capiq_data())
        :param capiq_data: pandas DF with CAPIQ data
        :return: merged pandas DF containing both TickData and CAPIQ information
        """
        start = time.time()
        _logger.info(">>> Merging TickData and CAPIQ data")
        tickdata_capiq = tickdata_data.merge(capiq_data, how='left', on='symbol', copy=True)

        # Re-organizing columns
        cols_of_interest = ['symbol', 'td_symbol', 'ciq_symbol', 'cusip', 'isin', 'ciq_isin', 'td_exchange', 'exchange',
                            'trading_currency', 'security_name', 'td_company_name', 'ciq_company_name', 'company_type',
                            'industry', 'industry_sector', 'country_name', 'td_security_id', 'ciq_company_id',
                            'ciqid_security_item', 'ciqid_trading_item', 'td_end_dt']
        tickdata_capiq = tickdata_capiq[cols_of_interest]

        # Symbols with no ISIN
        symbols_no_isin = sorted(tickdata_capiq.loc[tickdata_capiq['isin'].isnull()]['ciq_symbol'])

        # Statistics on created DF
        _logger.info("Shape of merged table: {0}".format(tickdata_capiq.shape))
        _logger.info("Symbols with no isin: {0}: {1} ..".format(len(symbols_no_isin), symbols_no_isin[0:5]))
        _logger.info("Done in : %.2f s" % (time.time() - start))

        tickdata_capiq = tickdata_capiq.sort_values('symbol').reset_index(drop=True)
        if self.pull_data and os.path.exists(self.path_local_cache):
            filename = 'tickdata_capiq.csv'
            tickdata_capiq.to_csv(os.path.join(self.path_local_cache, filename), header=True,
                                  index=False, encoding='utf-8')
        return tickdata_capiq

    # ====================================================================================
    # Retrieve RavenPack data from ravenpack_equities table
    # ====================================================================================

    def _retrieve_ravenpack_data(self, filename):
        """
        This method queries Redshift table "ravenpack_equities" and returns DF with rp_symbol, rp_isin, rp_company_id,
        rp_company_name, rp_end_dt.
        :param filename: name of file where to store retrieved RavenPack data
        :return: pandas DF with rp_symbol, rp_isin, rp_company_id, rp_company_name, rp_end_dt
        """
        _logger.info(">>> Retrieving data from ravenpack_equities table [Redshift]")

        query = """select l.timestamp_utc, l.company, l.entity_name, l.isin, l.rp_entity_id 
        from ravenpack_equities l
        JOIN
        (select rp_entity_id, company, MAX(timestamp_utc) as max_timestamp_utc
        from ravenpack_equities
        group by rp_entity_id, company) r
        ON
        l.rp_entity_id = r.rp_entity_id AND
        l.company = r.company AND
        l.timestamp_utc = r.max_timestamp_utc
        """
        engine = get_redshift_engine()
        rvpack_data = pd.read_sql_query(query, engine)
        engine.dispose()

        rvpack_data.rename(columns={'company': 'rp_symbol', 'entity_name': 'rp_company_name', 'isin': 'rp_isin',
                                    'rp_entity_id': 'rp_company_id', 'timestamp_utc': 'rp_end_dt'}, inplace=True)
        rvpack_data['symbol'] = rvpack_data['rp_symbol'].apply(lambda x: x.split('/')[1])  # "US/AAPL" -> "AAPL"
        rvpack_data['symbol'] = rvpack_data['symbol'].apply(lambda x: re.sub('[^0-9a-zA-Z]+', '', x))
        rvpack_data['isin'] = rvpack_data['rp_isin'].copy()  # this column is needed when merging to SM at later stage
        rvpack_data['rp_company_id'] = rvpack_data['rp_company_id'].map(lambda x: x.encode('ascii'))
        _logger.info("Original number of symbols in RavenPack data: {0}".format(rvpack_data.shape[0]))
        _logger.info("Starting cleaning of RavenPack data ..")

        # Filtering symbols that contain any numbers
        mask = rvpack_data.symbol.str.contains("\d", regex=True)
        _logger.info("Removing records where symbol contains numbers: %d" % len(list(filter(lambda x: x, mask))))
        rvpack_data = rvpack_data[~mask]

        # Removing records that have no symbol
        mask = ((rvpack_data['symbol'].isnull()) | (rvpack_data['symbol'] == ''))
        _logger.info("Removing records with no symbol: %d" % len(list(filter(lambda x: x, mask))))
        rvpack_data = rvpack_data[~mask]

        # Organizing columns
        columns = ['symbol', 'rp_symbol', 'isin', 'rp_isin', 'rp_company_id', 'rp_company_name', 'rp_end_dt']
        rvpack_data = rvpack_data[columns].sort_values(['symbol', 'rp_company_id', 'rp_end_dt']).reset_index(drop=True)
        _logger.info("Final number of RavenPack symbols after cleaning: %d" % (rvpack_data.shape[0]))

        # Symbols with no ravenpack ISIN
        symbols_no_isin = sorted(rvpack_data.loc[rvpack_data['isin'].isnull()]['rp_symbol'])
        _logger.info("Number of RavenPack symbols with no isin: %d" % len(symbols_no_isin))

        # Saving Retrieved RavenPack data to the local disk
        if self.pull_data and os.path.exists(self.path_local_cache):
            rvpack_data.to_csv(os.path.join(self.path_local_cache, filename), header=True, index=False, encoding='utf-8')
        return rvpack_data

    def get_ravenpack_data(self):
        """
        This method is used to retrieve RavenPack data such as rp_symbol, rp_isin, rp_company_id, rp_company_name,
        rp_end_dt from Redshift table "ravenpack_equities" on run_dt
        :return: pandas DF with RavenPack data (rp_symbol, rp_isin, rp_company_id, rp_company_name, rp_end_dt)
        """
        filename = 'ravenpack_data.csv'
        start = time.time()
        if not self.pull_data and os.path.exists(os.path.join(self.path_local_cache, filename)):
            _logger.info(">>> Reading RavenPack data from disk as of run_dt='%s'" % str(self.run_dt.date()))
            rvpack_data = pd.read_csv(os.path.join(self.path_local_cache, filename))
        else:
            rvpack_data = self._retrieve_ravenpack_data(filename)
        _logger.info("Done in : %.2f s" % (time.time() - start))
        return rvpack_data

    # ====================================================================================
    # Merge RavenPack and TickData_CAPIQ tables
    # ====================================================================================

    def merge_tickdatacapiq_with_rvpack(self, tickdata_capiq, rvpack_data):
        """
        This method merges tickdata-capiq DF with the RavenPack DF on ['symbol', 'isin']
        :param tickdata_capiq: merged tickdata and capiq DF
        :param rvpack_data: pandas DF with RavenPack data
        :return: merged tickdata-capiq DF with the RavenPack DF
        """
        start = time.time()
        _logger.info(">>> Merging TickData_CAPIQ and RavenPack data")

        security_master = tickdata_capiq.merge(rvpack_data, how='left', on=['symbol', 'isin'], copy=True)
        columns = [
            'symbol', 'td_symbol', 'ciq_symbol', 'rp_symbol', 'cusip', 'isin', 'ciq_isin', 'rp_isin', 'td_exchange',
            'exchange', 'trading_currency', 'security_name', 'td_company_name', 'ciq_company_name',
            'rp_company_name', 'company_type', 'industry', 'industry_sector', 'country_name', 'td_security_id',
            'ciq_company_id', 'rp_company_id', 'ciqid_security_item', 'ciqid_trading_item', 'td_end_dt', 'rp_end_dt']

        security_master = security_master[columns].sort_values(['symbol', 'rp_company_id', 'rp_end_dt'])

        # Filter symbols with same symbol, rp_isin, rp_company_name, rp_company_id by taking record with latest
        # 'rp_end_dt'. Note: records should be sorted by 'symbol', 'rp_company_id', 'rp_end_dt' (as above)!!
        mask = security_master.duplicated(subset=['symbol', 'rp_isin', 'rp_company_name', 'rp_company_id'], keep=False)
        _logger.info("Filtering records with same symbol, rp_isin, rp_company_name, rp_company_id but not rp_end_dt "
                     "(keep latest rp_end_dt): %d" % security_master.loc[mask, 'symbol'].unique().size)
        security_master.drop_duplicates(subset=['symbol', 'rp_isin', 'rp_company_name', 'rp_company_id'],
                                        keep='last', inplace=True)

        # Filtering all records whose ciq_isin is null (those records have no data from CAPIQ)
        mask = (security_master['ciq_isin'].isnull())

        _logger.info("Removing records whose CAPIQ isin is null: %d" % len(list(filter(lambda x: x, mask))))
        security_master = security_master.loc[~mask].sort_values('symbol').reset_index(drop=True)

        # Symbols with no Raven Pack company_id / isin (thus be looked-up by cusip + company name in RavenPack Website)
        # Note: we exclude all Funds since we don't have cusip/isin for them (searching only for Companies)
        mask = (security_master['company_type'] != 'Public Fund') & (security_master['rp_isin'].isnull())
        symbols_no_rp_company_id = sorted(security_master.loc[mask, 'symbol'].values)

        # Mapping TickData Exchange into CAPIQ format
        security_master['td_exchange'] = security_master['td_exchange'].map(
            lambda x: self.TD_TO_CIQ_EXCHANGE_MAP[x] if x in self.TD_TO_CIQ_EXCHANGE_MAP else x)
        security_master.drop(['ciq_isin', 'td_end_dt'], axis=1, inplace=True)

        # Statistics on created table
        _logger.info("Final shape of merged table: {0}".format(security_master.shape))
        _logger.info("Symbols with no RavenPack company_id / isin (excl. ETFs): {0}".format(len(symbols_no_rp_company_id)))
        _logger.info("Done in : %.2f s" % (time.time() - start))
        return security_master

    # ====================================================================================
    # Pull ETF universe names (based on S3 chris/etfg/legacy_constituents/)
    # ====================================================================================

    def get_etf_universe_names(self):
        """
        This method is used to fetch ETF universe names from S3 [see chris/etfg/20181031_constituents_export.csv]
        :return: list of ETF universe [chris/etfg]
        """
        col_names = ['Date', 'etp_ticker', 'constitutent_ticker', 'constituent_name', 'constituent_weight',
                     'constituent_market_value', 'constituent_cusip', 'constituent_isin', 'constituent_figi',
                     'constituent_sedol', 'constituent_exchange', 'constituent_iso_country', 'total_shares_held',
                     'asset_class', 'security_type']

        filename = '20181031_constituents_export.csv'
        if not self.pull_data and os.path.exists(os.path.join(self.path_local_cache, 'ETF_universe', filename)):
            _logger.info(">>> Reading ETF universe from local disk")
            etfg_data = pd.read_csv(os.path.join(self.path_local_cache, 'ETF_universe', filename), header=None,
                                    parse_dates=[0], index_col=[0], names=col_names)
        else:
            etfg_data = self._pull_etf_universe_names(col_names)

        etfg_data.columns = etfg_data.columns.str.lower()
        etf_names = sorted(etfg_data['etp_ticker'].unique())
        _logger.info("Number of unique names in ETF universe: %d" % len(etf_names))
        return etf_names

    # ====================================================================================
    # Validate and Save Security Master
    # ====================================================================================

    @staticmethod
    def seq_match_similarity(a, b):
        # This method computes similarity score between company names
        return SequenceMatcher(None, a, b).ratio() * 100

    @staticmethod
    def clean_company_name(comp_name):
        """
        This method cleans company name: it encodes name to ascii, remove punctuations, maps some company shorthands
        :param comp_name: original company name (str)
        :return: cleaned company name (str)
        """
        try:
            comp_name = comp_name.decode('ascii', errors='ignore').encode('ascii')
        except AttributeError:
            # AttributeError: 'str' object has no attribute 'decode'
            comp_name = re.sub(r'[^\x00-\x7F]+', '', comp_name)
        except UnicodeEncodeError as e:
            comp_name = re.sub(r'[^\x00-\x7F]+', '', comp_name).encode('ascii', 'ignore')  # drop non-ASCII

        comp_name = comp_name.lower()
        comp_name = comp_name.replace(' corp.', 'corporation')
        comp_name = comp_name.replace(' ltd.', 'limited')
        comp_name = comp_name.replace(' co.', 'company')

        if comp_name.endswith(' corp'):  # e.g. ALCOA CORP, ATLANTIC AMERICAN CORP
            comp_name = comp_name.replace(' corp', ' corporation')
        if comp_name.endswith(' ltd'):  # e.g. ABB LTD
            comp_name = comp_name.replace(' ltd', ' limited')
        if comp_name.endswith(' co'):
            comp_name = comp_name.replace(' co', ' company')

        try:
            comp_name = comp_name.translate(string.punctuation)
        except TypeError as e:
            _logger.error("Problem of removing non-alphanumeric from {0}. Message: {1}".format(comp_name, e))

        return comp_name

    @staticmethod
    def compute_company_name_similarity(df):
        """
        This method computes company names similarity [0...100%] between TickData, RavenPack and CAPIQ. It is computed
        using two approaches: fuzz.ratio and SequenceMatcher. The function computes 2 scores (fuzz and sequence matcher)
        for 2 pairs: TD vs RVPack and CAPIQ vs RVPack -> then it takes maximum out of these 4 scores. This vector with
        maximum similarity scores is added to the security master dataframe as "max_sr" column.
        :param df: pandas DF with collected information about securities
        :return: pandas DF with collected information about securities + "max_sr" column.
        """
        cols_of_interest = ['symbol', 'isin', 'rp_company_id', 'td_company_name',
                            'ciq_company_name', 'rp_company_name', 'rp_end_dt']
        df_temp = df[cols_of_interest].copy()

        for col in ['td_company_name', 'ciq_company_name', 'rp_company_name']:
            df_temp[col + '_clean'] = df_temp[col].apply(lambda x: SecurityUniverseHandler.clean_company_name(x))

        # Compute company names similarity scores
        df_temp['cpq_name_vs_rp_name_sr1'] = df_temp.apply(lambda x: fuzz.ratio(x['ciq_company_name_clean'],
                                                                                x['rp_company_name_clean']), axis=1)
        df_temp['cpq_name_vs_rp_name_sr2'] = df_temp.apply(lambda x: SecurityUniverseHandler.seq_match_similarity(
            x['ciq_company_name_clean'], x['rp_company_name_clean']), axis=1)
        df_temp['td_name_vs_rp_name_sr1'] = df_temp.apply(lambda x: fuzz.ratio(x['td_company_name_clean'],
                                                                               x['rp_company_name_clean']), axis=1)
        df_temp['td_name_vs_rp_name_sr2'] = df_temp.apply(lambda x: SecurityUniverseHandler.seq_match_similarity(
            x['td_company_name_clean'], x['rp_company_name_clean']), axis=1)

        # Compute min & max company names similarity score (CAPIQ vs RavenPack vs TickData)
        cols_agg = ['cpq_name_vs_rp_name_sr1', 'cpq_name_vs_rp_name_sr2', 'td_name_vs_rp_name_sr1',
                    'td_name_vs_rp_name_sr2']
        df_temp['min_sr'] = df_temp[cols_agg].apply(min, axis=1)
        df_temp['max_sr'] = df_temp[cols_agg].apply(max, axis=1)

        df_temp = df_temp[cols_of_interest + ['max_sr']].reset_index(drop=True)
        return df_temp

    def filter_security_master(self, security_master):
        """
        This function filters security master updates. Note: if pull_data = True -> get_etf_universe_names() will
        read AWS S3 to get chris/etfg/legacy_constituents/ ETFs
        :param security_master: pandas DF with collected information about securities
        :return: filtered security_master
        """
        _logger.info(">>> Filtering Security Master")
        _logger.info("Shape of SM before cleaning: {0}".format(security_master.shape))

        if self.drop_no_rp_comp_id:
            # Remove records that have no RP company id (e.g. ETFs)
            mask = security_master['rp_company_id'].notnull()
            security_master = security_master.loc[mask].sort_values('symbol').reset_index(drop=True)
            _logger.info("Removing %d names with no rp_company_id" % (len(list(filter(lambda x: not x, mask)))))

        if self.drop_etfs_not_in_etfg:
            # Reading ETF universe (for verifying ETFs in Security Master)
            etf_names = self.get_etf_universe_names()

            # Removing all ETF names from SM that are not in ETF universe (see chris/etfg/legacy_constituents/)
            mask = (security_master['company_type'] == 'Public Fund')
            security_master_etfs = list(security_master.loc[mask, 'symbol'])
            sm_etfs_not_in_etg = sorted(set(security_master_etfs).difference(set(etf_names)))
            _logger.info("Removing {0} ETFs from SM that are not in chris/etfg/legacy_constituents/ universe. "
                         "Symbols: {1} ..".format(len(sm_etfs_not_in_etg), sm_etfs_not_in_etg[0:5]))
            security_master = security_master.loc[~security_master['symbol'].isin(sm_etfs_not_in_etg)] \
                .sort_values('symbol').reset_index(drop=True)

        _logger.info("Shape of SM after cleaning: {0}".format(security_master.shape))
        return security_master

    def check_company_names_consistency(self, security_master):
        """
        This function checks company names consistency between TickData, RavenPack and CAPIQ. The similarity [0...100%]
        is computed using two approaches: fuzz.ratio and SequenceMatcher. There are couple of steps:

        1) The function computes 2 scores (fuzz and sequence matcher) for 2 pairs: TD vs RVPack and CAPIQ vs RVPack ->
        then it takes maximum out of these 4 scores and compares with the min_thresh_match_str. If the score is below
        the threshold and drop_comp_name_simil_below_thresh is True -> the company will be excluded from the security
        master update dataframe due to quality issue. Manual check for these tickers is then needed.
        2) After manual verification of tickers with the company name issues, one can add these verified names to
        tickers_verified_by_hand list. Similarity ratio for all tickers in this list is set to 100%.

        :param security_master: pandas DF with collected information about securities
        :return: pandas DF with changes to be added to security universe table
        """
        _logger.info(">>> Checking consistency of company names")

        # Computing max similarity score for company names: TD vs RP, CAPIQ vs RP (only companies that are in RavenPack)
        df_with_rp = security_master.loc[security_master['rp_company_id'].notnull()]
        if df_with_rp.empty:
            return security_master
        company_name_df = self.compute_company_name_similarity(df_with_rp)

        # Adding column to SM
        security_master = security_master.merge(company_name_df[['symbol', 'isin', 'max_sr']], how='left',
                                                on=['symbol', 'isin'])
        _logger.info("Shape of SM before cleaning: {0}".format(security_master.shape))

        # For duplicated records with same 'symbol', 'isin' -> take the record with highest max_sr and then
        # the most recent (i.e. rp_end_dt is max)
        _logger.info("Removing {0} duplicated records with same 'symbol', 'isin' (keeping the latest)".format(
            security_master.loc[security_master.duplicated(subset=['symbol', 'isin'])].shape[0]))
        security_master = security_master.sort_values(['symbol', 'max_sr', 'rp_end_dt'], ascending=[
            True, False, False]).groupby(['symbol', 'isin']).head(1)

        # Tickers below are verified by hands (i.e. company names TD == RP == CAPIQ)
        tickers_verified_by_hand = [
            'ACM', 'AIMC', 'ALLT', 'APHB', 'ARCW', 'ARMK', 'ASCMA', 'ATLC', 'AUMN', 'BEAT', 'BGI',
            'BGY', 'BKCC', 'BOE', 'BPRN', 'BTN', 'BTO', 'CAPE', 'CIDM', 'CLRO', 'CNDT', 'COP', 'CSL',
            'DCO', 'DLHC', 'DNI', 'DSW', 'ECF', 'ECOL', 'EDEN', 'EFL', 'EFU', 'EHI', 'ENS', 'EPR',
            'EQR', 'FAS', 'FBNC', 'FBP', 'FCTR', 'FEN', 'FEU', 'FMO', 'FOXA', 'FRC', 'FTEC', 'GBX',
            'GDL', 'GEOS', 'GLW', 'GPM', 'HCI', 'HELE', 'HRTX', 'IBM', 'IDCC', 'INPX', 'INVE', 'IRCP',
            'IRDM', 'IRM', 'JBGS', 'JE', 'JMM', 'JPC', 'KCAP', 'KFY', 'KODK', 'LZB', 'MHE', 'MINT',
            'MJCO', 'MMAC', 'MRBK', 'MSA', 'MUX', 'NAC', 'NAN', 'NAZ', 'NEN', 'NHF', 'NIE', 'NKX',
            'NMY', 'NNC', 'NOBL', 'NOM', 'NPV', 'NQP', 'NRK', 'NSP', 'NSSC', 'NTC', 'NTIP', 'NTP',
            'NUO', 'NVG', 'NWLI', 'NXJ', 'NZF', 'OC', 'OFG', 'OHAI', 'PAI', 'PCMI', 'PDVW', 'PFL',
            'PFN', 'PLCE', 'PRPH', 'PSX', 'PW', 'QQQX', 'RAVE', 'RCS', 'RH', 'RIF', 'ROIC', 'RTIX',
            'SAR', 'SBFG', 'SJW', 'SNA', 'SOHO', 'SP', 'SPE', 'SPTN', 'SPXX', 'SRE', 'SYBT', 'SYNA',
            'TRMB', 'TRST', 'TRU', 'TTS', 'UFCS', 'USFD', 'USLM', 'UTF', 'VBF', 'VCEL', 'VIRC', 'VKQ',
            'VLRS', 'VLT', 'VVR', 'WAB', 'WLTW', 'WTW', 'WUBA', 'WWD', 'XOMA', 'XONE', 'XYF', 'ZION'
        ]
        mask = (security_master['symbol'].isin(tickers_verified_by_hand))
        security_master.loc[mask, 'max_sr'] = 100.0  # set similarity score to 100% for manually checked names

        if self.drop_comp_name_simil_below_thresh:
            # Remove records that did not pass company name verification (with threshold). Note: majority of these
            # companies had ticker symbol change CA but Raven Pack did not sync with it
            mask = (security_master['max_sr'] < self.min_thresh_match_str)
            security_master = security_master.loc[~mask].copy()
            _logger.info("Removing {0} symbols from SM [company_names similarity < {1:0.2f}%]".format(
                len(list(filter(lambda x: x, mask))), self.min_thresh_match_str))

        _logger.info("Shape of SM after cleaning: {0}".format(security_master.shape))
        _logger.info("==== Final stats on SM ====")
        self.describe_security_master(security_master)
        if self.pull_data and os.path.exists(self.path_local_cache):
            filename = "security_master.csv"
            security_master.to_csv(os.path.join(self.path_local_cache, filename), header=True,
                                   index=False, encoding='utf-8')
        return security_master

    def prepare_security_universe_update(self, security_master):
        """
        This function prepares an update to the security universe table
        :param security_master: pandas DF with collected information about securities
        :return: pandas DF with changes to be added to security universe table
        """
        columns = [
            'symbol', 'cusip', 'isin', 'td_exchange', 'trading_currency', 'ciq_company_name', 'security_name',
            'td_security_id', 'ciq_company_id', 'ciqid_trading_item', 'rp_company_id'
        ]
        security_universe_upd = security_master[columns].copy()
        security_universe_upd.insert(loc=0, column='dcm_security_id', value=security_universe_upd['td_security_id'])
        security_universe_upd['is_active'] = True
        security_universe_upd['latest_update'] = pd.Timestamp.now('UTC').tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
        security_universe_upd['td_exchange'] = security_universe_upd['td_exchange'].map(self.TD_EXCHANGE_TO_MIC_MAP)

        cols_rename = {
            'symbol': 'ticker', 'td_exchange': 'exchange', 'ciq_company_name': 'company_name',
            'ciqid_trading_item': 'ciq_trading_item_id', 'rp_company_id': 'rp_entity_id'
        }
        security_universe_upd.rename(columns=cols_rename, inplace=True)

        # IDs should be integers
        for col in ['dcm_security_id', 'td_security_id']:
            security_universe_upd[col] = security_universe_upd[col].astype(int)

        if self.pull_data and os.path.exists(self.path_local_cache):
            filename = "security_universe_update.csv"
            security_universe_upd.to_csv(os.path.join(self.path_local_cache, filename), header=True,
                                         index=False, encoding='utf-8')
        return security_universe_upd

    def modify_security_universe(self, security_universe_update, tickers_add_to_sec_univ, tickers_modify_in_sec_univ,
                                 tickers_to_rename):
        """
        This method is used to introduce changes to the security universe table.
        :param security_universe_update: pandas DF with the prepared updates (added or modified tickers)
        :param tickers_add_to_sec_univ: list of tickers to add to security_universe table (these were added
                                        manually to the universe.txt)
        :param tickers_modify_in_sec_univ: list of tickers to modify in security_universe table (these were modified
                                           due to any CA events that affect company info
                                           (e.g. symbol/company/listing/cusip change)
        :param tickers_to_rename: dict with old company symbol as key and the new symbol as a value (per security_id)
        :return:
        """
        security_universe_table = get_config()["security_master"]["security_universe_table"]
        queries_to_upd_sec_univ = []

        # Convert bytes to string
        # Fixes: Value has type BYTES which cannot be inserted into column rp_entity_id, which has type STRING
        #  also: Some values in the column are NaN
        col = 'rp_entity_id'
        security_universe_update[col] = security_universe_update[col].apply(
            lambda x: x.decode("utf-8") if isinstance(x, bytes) else ''
        )

        if len(tickers_add_to_sec_univ):
            query_header = "INSERT INTO {table_name} VALUES".format(table_name=security_universe_table)
            query_body = []
            mask = (security_universe_update['ticker'].isin(tickers_add_to_sec_univ))
            for _, record in security_universe_update.loc[mask].iterrows():
                query_body.append(str(tuple(record)))
            query_body = ',\n'.join(query_body) + ';'
            query = '\n'.join([query_header, query_body])
            queries_to_upd_sec_univ.append(query)

        if len(tickers_modify_in_sec_univ):
            # Get new symbols for tickers in tickers_modify_in_sec_univ that had ticker symbol change event by looking
            # up into tickers_to_rename. Note that in the security_universe_update DF ticker name is a new symbol, but
            # tickers_modify_in_sec_univ contains old symbols (we need to map everything to new for the boolean mask
            # below)
            tickers_modify_in_sec_univ = [tickers_to_rename[ticker] if ticker in tickers_to_rename else ticker
                                          for ticker in tickers_modify_in_sec_univ]

            # Select records from security_universe_update DF that are prepared for tickers_modify_in_sec_univ
            mask = (security_universe_update['ticker'].isin(tickers_modify_in_sec_univ))
            cols_to_modify = [col for col in security_universe_update.columns if col not in ['dcm_security_id', 'rp_entity_id']]

            query_header = "UPDATE {table_name}".format(table_name=security_universe_table)
            for _, record in security_universe_update.loc[mask].iterrows():
                dcm_security_id = record['dcm_security_id']
                query_footer = "WHERE dcm_security_id={dcm_security_id};".format(dcm_security_id=dcm_security_id)
                query_body = ""
                flag_first_change = True
                for col in cols_to_modify:
                    if flag_first_change:
                        query_body += "SET {0}='{1}'".format(col, record[col])
                        flag_first_change = False
                    else:
                        if col == 'td_security_id':  # is integer
                            query_body += ", {0}={1}".format(col, record[col])
                        else:
                            query_body += ", {0}='{1}'".format(col, record[col])
                query = '\n'.join([query_header, query_body, query_footer])
                queries_to_upd_sec_univ.append(query)

        # Updating security_universe table [Redshift]
        if len(queries_to_upd_sec_univ):
            queries_to_upd_sec_univ = '\n'.join(queries_to_upd_sec_univ)  # concat all queries into singe query
            sa_engine = get_redshift_engine()
            with sa_engine.begin() as connection:
                connection.execute(queries_to_upd_sec_univ)
            sa_engine.dispose()
            _logger.info("The {0} table is updated: {1}".format(
                security_universe_table, sorted(set(tickers_add_to_sec_univ).union(set(tickers_modify_in_sec_univ)))))

    def update_adjustment_stage_status(self, tickers_add_to_sec_univ, tickers_to_rename):
        """
        This method removes adjustment stage status record with stage_key 'pipeline' on run_dt in the
        pipeline_stage_status table if there were any changes in universe.txt:
        1) Tickers were added by hands
        2) Tickers were modified due to ticker symbol change CA event
        After removing this record (if the case) - the equity adjustment pipeline will restart (following 10' wake-up
        periods) and will see that there are new symbols in universe -> thus will run adjustment for these names.
        :param tickers_add_to_sec_univ: list of tickers to add to security_universe table (these were added manually
        to the universe.txt)
        :param tickers_to_rename: dict with old company symbol as key and the new symbol as a value (per security_id)
        :return:
        """

        # New names in dcm universe.txt due to adding by hands or due to ticker symbol change
        new_names_in_dcm_universe = sorted(set(tickers_add_to_sec_univ).union(set(tickers_to_rename.values())))

        if len(new_names_in_dcm_universe):
            _logger.info(">>> Removing adjustment stage status record from pipeline_stage_status table. %d new name(s) "
                         "added to universe.txt: %s." % (len(new_names_in_dcm_universe), new_names_in_dcm_universe))

            stage_id = UpdateSecurityUniverseStage.get_prices_adjustment_stage_id(self.run_dt)
            with PipelineStageStore(run_id=1, run_date=self.run_dt, stage_id=stage_id) as progress:
                progress.reset_stage_status(key="pipeline")

    def update_security_universe(self):
        """
        This is the main method that runs security_universe update
        """
        start = time.time()

        # Get the most recent security universe table
        security_universe = self.get_security_universe_table()

        # Get pending security changes and modify security_universe table and universe.txt file [on github]
        pending_security_changes = self.get_pending_security_changes()

        # Filter pending security changes by selecting only these that have cob_date > last_update date in
        # security_universe
        pending_security_changes_filtered = self.filter_pending_security_changes(pending_security_changes,
                                                                                 security_universe)
        # Process pending security changes
        tickers_to_drop, tickers_to_rename, tickers_comp_name_change, tickers_cusip_change, \
            tickers_listing_change = self.process_pending_security_changes(pending_security_changes_filtered)

        # Updating universe.txt file on github [datacapitalmanagement/dcm-config/data/universe/universe.txt] with
        # the changes caused by ticker de-listing/acquisition or symbol change.
        dcm_universe = self.update_universe_txt_github(tickers_to_drop, tickers_to_rename)

        # Get tickers to be added, activated or de-activated to security universe table.
        # This is achieved by checking the difference between latest universe.txt and security_universe table
        tickers_add_to_sec_univ, tickers_to_activ_in_sec_univ, tickers_to_deactiv_in_sec_univ = \
            self.get_tickers_to_add_activ_deactiv_in_sec_univ(dcm_universe, security_universe, tickers_to_rename)

        # Activate/de-activate tickers in security_universe table [if any]
        self.activ_deactiv_tickers_in_security_universe(tickers_to_activ_in_sec_univ, tickers_to_deactiv_in_sec_univ)

        # Tickers to update in security_universe table [due to company name/cusip/exchange change]
        tickers_modify_in_sec_univ = sorted(set(tickers_to_rename).union(set(tickers_comp_name_change)).union(
            set(tickers_cusip_change)).union(set(tickers_listing_change)))

        # Tickers to process [these to be added / modified in security_universe table]
        tickers_to_process = sorted(set(tickers_add_to_sec_univ).union(tickers_modify_in_sec_univ))

        if len(tickers_to_process):
            # Get CA data [FOOTER] for tickdata universe. This step also filters tickers with no or empty CA files
            tickdata_ca_validated = self.get_latest_tickdata_ca(tickers_to_process)

            # Filter tickers that contains e.g. ['_W', '_F', '_PR'] in their names
            tickdata_ca_validated_filtered = self.filter_tickdata_universe(tickdata_ca_validated)

            # Pull CAPIQ data
            capiq_data = self.get_capiq_data(tickdata_ca_validated_filtered)

            # Postprocess TickData and CAPIQ data
            tickdata_final, capiq_data_final = self.postprocess_tickdata_and_capiq_data(
                tickdata_ca_validated_filtered, capiq_data)

            # Merge TickData and CAPIQ data into a security master
            tickdata_capiq = self.merge_tickdata_and_capiq_data(tickdata_final, capiq_data_final)

            # Retrieve RavenPack data from ravenpack_equities table
            rvpack_data = self.get_ravenpack_data()

            # Merge tickdata_capiq and rvpack_data into Security Master
            security_master = self.merge_tickdatacapiq_with_rvpack(tickdata_capiq, rvpack_data)

            # Filter security master
            # 1. Drop records with no rp_company_id (e.g. ETFs) if drop_no_rp_comp_id=True
            # 2. Remove ETFs that are not in chris/etfg/legacy_constituents/ universe if drop_etfs_not_in_etfg=True
            security_master = self.filter_security_master(security_master)

            # Check company names consistency
            # 1. Drop records with company name similarity below threshold if drop_comp_name_simil_below_thresh=True
            security_master = self.check_company_names_consistency(security_master)

            # Prepare Security Universe table
            security_universe_update = self.prepare_security_universe_update(security_master)

            # Update Security Universe table
            self.modify_security_universe(security_universe_update, tickers_add_to_sec_univ,
                                          tickers_modify_in_sec_univ, tickers_to_rename)

            # Save updated universe.txt file to S3
            self.save_updated_dcm_universe_to_s3(dcm_universe)

            # Remove adjustment stage status record (stage_key "pipeline") from the pipeline_stage_status table
            self.update_adjustment_stage_status(tickers_add_to_sec_univ, tickers_to_rename)

        _logger.info("Total time to update security_universe: %.2f s" % (time.time() - start))


def _main():
    from etl_workflow_steps import EquitiesETLTaskParameters

    start_dt = pd.Timestamp('2000-01-03')
    as_of_dt = pd.Timestamp.now()
    end_dt = marketTimeline.get_trading_day_using_offset(as_of_dt, -1)
    run_dt = end_dt

    task_params = EquitiesETLTaskParameters(ticker='*', as_of_dt=as_of_dt, run_dt=run_dt,
                                            start_dt=start_dt, end_dt=end_dt)

    chunk_size = 1000  # 1000 (number of symbols to process in 1 batch when pulling CAPIQ)
    pull_data = True  # if True -> pull data (prices, CA, CAPIQ, RPack, ...). If false -> read data from cache
    path_local_cache = r'c:\DCM\Tasks\7_Extension_of_universe'  # path on local disk where cached data is stored
    path_dcm_config_repo = r'c:\DCM\Work\dcm-config'  # path to local dcm-config repo

    drop_no_rp_comp_id = False  # if True -> drop records with no rp company id (e.g. ETFs)
    drop_etfs_not_in_etfg = False  # if True -> drop ETFs that are not chris/etfg/legacy_constituents/ universe
    drop_comp_name_simil_below_thresh = False  # if True -> drop records with company name similarity below thresh.
    min_thresh_match_str = 80.0  # minimum similarity score threshold [in %] for CAPIQ/TD company names vs RavenPack

    sec_universe_stage = UpdateSecurityUniverseStage(task_params.run_dt)
    sec_univ_handler = SecurityUniverseHandler(task_params, pull_data, min_thresh_match_str,
                                               drop_comp_name_simil_below_thresh, drop_no_rp_comp_id,
                                               drop_etfs_not_in_etfg, chunk_size, path_local_cache,
                                               path_dcm_config_repo)

    adjustment_completed = sec_universe_stage.price_adjustment_pipeline_completed()
    if adjustment_completed:
        sec_univ_handler.update_security_universe()


if __name__ == '__main__':
    _main()
