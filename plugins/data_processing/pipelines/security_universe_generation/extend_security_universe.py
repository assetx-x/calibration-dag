import os
import re
import time
import string
import logging
import tempfile
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
from pipelines.common.pipeline_util import get_postgres_engine
from pipelines.prices_ingestion.corporate_actions_holder import S3TickDataCorporateActionsHandler
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_redshift_engine, get_s3_client
from pipeline_util import read_txt_file, store_txt_file, store_dict_to_json, git_push, git_commit, git_clone, \
    rmtree, convert_cusip_to_nine_digits

# Filter InsecureRequestWarning (due to used verify=False in requests.get())
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# Global parameters
path = r'c:\DCM\Tasks\6_Extension_of_universe'
start_dt = pd.Timestamp('2000-01-03')
as_of_dt = pd.Timestamp.now()
end_dt = marketTimeline.get_trading_day_using_offset(as_of_dt, -1)
run_dt = end_dt


# Logging settings
log_format = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s"
log_formatter = logging.Formatter(log_format, "%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger('SM')
_logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(os.path.join(path, 'build_security_master.log'))
fh.setFormatter(log_formatter)
fh.setLevel(logging.DEBUG)
_logger.addHandler(fh)


# Mapper used to convert TickData exchange info to the CAPIQ format
td_to_ciq_exchange_map = {
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
td_exchange_to_mic_map = {
    'ARCA': 'ARCX',
    'BATS': 'BATS',
    'NasdaqCM': 'XNCM',  # NASDAQ CAPITAL MARKET
    'NasdaqGS': 'XNGS',  # NASDAQ/NGS (GLOBAL SELECT MARKET)
    'NasdaqGM': 'XNMS',  # NASDAQ/NMS (GLOBAL MARKET)
    'AMEX': 'XASE',
    'NYSE': 'XNYS',
    'IEX': 'IEXG',
}


# ====================================================================================
# Generic auxiliary functions
# ====================================================================================

def describe_security_master(df):
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
                 (len(set(duplicated_rp_comp_id)), len(duplicated_rp_comp_id), sorted(set(duplicated_rp_comp_id))[0:5]))
    _logger.info("Columns with nulls: %s" % list(df.isnull().sum().to_frame('nulls').query("nulls > 0").index))


# ====================================================================================
# Auxiliary functions to interact with RavenPack browser
# ====================================================================================

def setup_firefox_browser(headless=True):
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options

    _logger.info("Setting up firefox browser")
    options = Options()
    options.headless = headless
    driver = webdriver.Firefox(options=options, executable_path=os.environ["SELENIUM_WEB_DRIVER"])
    return driver


def login_to_ravenpack_website(driver):
    _logger.info("Logging-in to the RavenPack website")

    rvpack_user = credentials_conf["ravenpack"]["username"]
    rvpack_pwd = credentials_conf["ravenpack"]["password"]
    rvpack_login_page = credentials_conf["ravenpack"]["website_login_page"]

    driver.get(rvpack_login_page)
    driver.find_element_by_id('input_username').send_keys(rvpack_user)
    driver.find_element_by_id('input_password').send_keys(rvpack_pwd)
    driver.find_element_by_id("signin_button").click()
    time.sleep(5)


# ====================================================================================
# Auxiliary functions to interact with TickData API
# ====================================================================================

def get_tickdata_auth():
    authentication = (get_config()["raw_data_pull"]["TickData"]["Price"]["username"],
                      get_config()["raw_data_pull"]["TickData"]["Price"]["password"])
    return authentication


def create_tickdata_url(start_dt):
    tick_data_url = get_config()["raw_data_pull"]["TickData"]["Price"]["URL"] \
        .format(**{"start": start_dt.strftime("%m/%d/%Y")})
    return tick_data_url


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
# Get current DCM Universe status (security names, pending security changes, etc.)
# ====================================================================================

def get_dcm_universe():
    _logger.info(">>> Pulling DCM universe as of %s" % str(end_dt.date()))
    s3_client = get_s3_client()
    body = s3_client.get_object(Bucket=get_config()['bucket'], Key="chris/universe.txt")['Body']
    universe_tickers = body.read().decode('utf-8').splitlines()
    _logger.info('Number of securities in DCM universe as of {0}: {1}'.format(end_dt.date(), len(universe_tickers)))
    return universe_tickers


def get_pending_security_changes():
    pending_security_changes_table = get_config()["security_master"]["pending_security_changes_table"]
    start = time.time()
    _logger.info(">>> Reading pending security changes table as of %s" % str(end_dt.date()))
    sql_engine = get_postgres_engine()
    sql_command = (
        "SELECT * FROM {table_name} "
        "WHERE date(cob_date) = '{run_dt}'").format(run_dt=end_dt, table_name=pending_security_changes_table)
    pending_security_changes = pd.read_sql(sql_command, sql_engine)
    sql_engine.dispose()
    _logger.info("Done in : %.2f s" % (time.time() - start))
    return pending_security_changes


def process_pending_security_changes(pending_security_changes):
    security_universe_table = get_config()["security_master"]["security_universe_table"]

    if not pending_security_changes.empty:
        queries_to_update_sm = []
        tickers_to_drop_from_universe = []
        univ_tickers_to_rename = {}
        for i, ticker in enumerate(pending_security_changes['ticker']):
            _logger.debug('#%d. Processing %s' % (i, ticker))

            # Get CA raw file from S3 and parse it
            ca_data_normalized, _ = S3TickDataCorporateActionsHandler._get_tickdata_ca(ticker, start_dt, end_dt, as_of_dt)
            df_temp = ca_data_normalized.reset_index().copy()
            footer_date = df_temp.loc[df_temp['ACTION'] == 'FOOTER', 'ACTION DATE'].iloc[0]
            dcm_security_id = df_temp['COMPANY ID'].iloc[0]

            if footer_date < run_dt:  # check if security's FOOTER < run_date
                _logger.debug("Removing ticker {0} from universe.txt: FOOTER '{1}' < run_dt "
                              "'{2}'".format(ticker, footer_date.date(), run_dt.date()))

                # Query for de-activating ticker
                query = "UPDATE {table_name} SET is_active={is_active} WHERE dcm_security_id={dcm_security_id};".format(
                    table_name=security_universe_table, is_active=False, dcm_security_id=dcm_security_id)

                queries_to_update_sm.append(query)
                tickers_to_drop_from_universe.append(ticker)

            # Mask to search events that affect Security Master and have occured at run_dt
            ca_columns_affecting_sm = ['TICKER SYMBOL CHANGE', 'CUSIP CHANGE', 'CHANGE IN LISTING', 'NAME CHANGE']
            mask = (df_temp['ACTION'].isin(ca_columns_affecting_sm)) & (df_temp['ACTION DATE'] == run_dt)
            df_temp = df_temp.loc[mask]

            # Check if security have any CA on run_date that could affect Security Master
            if not df_temp.empty:

                # Preparing query to update Security Master with latest modifications due to CA events
                query_header = "UPDATE {table_name}".format(table_name=security_universe_table)
                query_footer = "WHERE dcm_security_id={dcm_security_id};".format(dcm_security_id=dcm_security_id)
                query_body = ""

                flag_first_change = True
                for action in df_temp['ACTION']:
                    if action == 'TICKER SYMBOL CHANGE':
                        old_symbol = df_temp.loc[df_temp['ACTION'] == action, 'OLD VALUE'].iloc[0]
                        new_symbol = df_temp.loc[df_temp['ACTION'] == action, 'NEW VALUE'].iloc[0]
                        _logger.debug("Updating SM for ticker '{0}'. Old symbol '{1}' changed to '{2}' due to {3} "
                                      "on {4}".format(ticker, old_symbol, new_symbol, action, run_dt.date()))
                        if flag_first_change:
                            query_body += "SET ticker='{ticker}'".format(ticker=new_symbol)
                            flag_first_change = False
                        else:
                            query_body += ", ticker='{ticker}'".format(ticker=new_symbol)
                        univ_tickers_to_rename[old_symbol] = new_symbol

                    if action == 'NAME CHANGE':
                        old_company_name = df_temp.loc[df_temp['ACTION'] == action, 'OLD VALUE'].iloc[0]
                        new_company_name = df_temp.loc[df_temp['ACTION'] == action, 'NEW VALUE'].iloc[0]
                        _logger.debug("Updating SM for ticker '{0}'. Old company name '{1}' changed to '{2}' due to {3}"
                                      " on {4}".format(ticker, old_company_name, new_company_name, action, run_dt.date()))
                        if flag_first_change:
                            query_body += "SET company_name='{company_name}'".format(company_name=new_company_name)
                            flag_first_change = False
                        else:
                            query_body += ", company_name='{company_name}'".format(company_name=new_company_name)

                    if action == 'CUSIP CHANGE':
                        old_cusip = df_temp.loc[df_temp['ACTION'] == action, 'OLD VALUE'].iloc[0]
                        new_cusip = convert_cusip_to_nine_digits(df_temp.loc[df_temp['ACTION'] == action,
                                                                             'NEW VALUE'].iloc[0])
                        _logger.debug("Updating SM for ticker '{0}'. Old CUSIP '{1}' changed to '{2}' due to {3} "
                                      "on {4}".format(ticker, old_cusip, new_cusip, action, run_dt.date()))
                        if flag_first_change:
                            query_body += "SET cusip='{cusip}'".format(cusip=new_cusip)
                            flag_first_change = False
                        else:
                            query_body += ", cusip='{cusip}'".format(cusip=new_cusip)

                    if action == 'CHANGE IN LISTING':
                        old_exchange = df_temp.loc[df_temp['ACTION'] == action, 'OLD VALUE'].iloc[0]
                        new_exchange = df_temp.loc[df_temp['ACTION'] == action, 'NEW VALUE'].iloc[0]
                        _logger.debug("Updating SM for ticker '{0}'. Old Exchange '{1}' changed to '{2}' due to {3} "
                                      "on {4}".format(ticker, old_exchange, new_exchange, action, run_dt.date()))

                        # Transform TickData 'NEW YORK STOCK EXCHANGE' -> Capiq Format 'NYSE' -> 'XNYS' MIC
                        new_exchange = td_exchange_to_mic_map[td_to_ciq_exchange_map[
                            ca_data_normalized['EXCHANGE'].iloc[1]]]

                        if flag_first_change:
                            query_body += "SET exchange='{exchange}'".format(exchange=new_exchange)
                            flag_first_change = False
                        else:
                            query_body += ", exchange='{exchange}'".format(exchange=new_exchange)

                query = '\n'.join([query_header, query_body, query_footer])
                queries_to_update_sm.append(query)

        queries_to_update_sm = '\n'.join(queries_to_update_sm)
        tickers_to_drop_from_universe = map(lambda x: x.encode('ascii', errors='ignore'), tickers_to_drop_from_universe)

        # Updating Security Master
        sa_engine = get_redshift_engine()
        with sa_engine.begin() as connection:
            connection.execute(queries_to_update_sm)
        sa_engine.dispose()

        # Get the most recent DCM universe and remove tickers that are delisted / change symbol who had CA event
        dcm_universe = get_dcm_universe()
        dcm_universe_updated = sorted(set(dcm_universe) - set(tickers_to_drop_from_universe))
        dcm_universe_updated = map(lambda x: univ_tickers_to_rename[x] if x in univ_tickers_to_rename else x,
                                   dcm_universe_updated)


# ====================================================================================
# Pull TickData Universe (based on Equity Prices)
# ====================================================================================

def _pull_latest_tickdata_universe(filename):
    _logger.info(">>> Pulling tickdata universe as of %s" % str(end_dt.date()))
    prices_url = create_tickdata_url(end_dt)
    data_request = requests.get(prices_url, auth=get_tickdata_auth(), verify=False)
    prices_data = ZipFile(BytesIO(data_request.content))

    # Taking names of tickers from names of zip files
    tickdata_universe = sorted([k.split('_')[0] for k in prices_data.namelist()])
    _logger.info("Number of tickers in tickdata universe on %s: %d "
                 "[ca not validated]" % (str(end_dt.date()), len(tickdata_universe)))

    # Saving names to disk
    with open(filename, 'w') as f:
        f.write('\n'.join(tickdata_universe))

    return tickdata_universe


def get_latest_tickdata_universe(forced_run=False):
    filename = os.path.join(path, 'td_universe_ca_not_validated.csv')
    start = time.time()
    if os.path.exists(filename) and forced_run is False:
        _logger.info(">>> Reading tickdata universe from disk as of %s" % str(end_dt.date()))
        tickdata_universe = pd.read_csv(filename, header=None).values.ravel().tolist()
    else:
        tickdata_universe = _pull_latest_tickdata_universe(filename)
    _logger.info("Done in : %.2f s" % (time.time() - start))
    return tickdata_universe


# ====================================================================================
# Pull TickData Corporate Actions
# ====================================================================================

def take_footer_only(ticker, auth):
    _logger.info(ticker)
    url = create_tickdata_ca_url_with_asof(ticker, end_dt, as_of_dt)
    try:
        # data_request = session.get(url)
        data_request = requests.get(url, auth=auth)
    except requests.exceptions.RequestException as e:
        _logger.error("Ticker: %s. Error: %s" % (ticker, e))
        time.sleep(3)  # sleep 3 seconds and retry
        data_request = requests.get(url, auth=auth)

    _logger.debug(ticker)
    if data_request.ok:
        content = data_request.content
        if content != '':  # to exclude cases with no data in ca file
            footer_record = content.split('\n')[-2].split('\r')[0]  # FOOTER
            return footer_record.startswith('FOOTER') and footer_record
    return


def _pull_latest_tickdata_ca(tickdata_universe, filename, pool_size=10):
    _logger.info(">>> Pulling tickdata CA information as of %s" % str(end_dt.date()))
    p = Pool(pool_size)  # Pool tells how many at a time
    tickdata_ca_validated = p.map(partial(take_footer_only, auth=get_tickdata_auth()), tickdata_universe)
    p.terminate()
    p.join()

    # Filtering out None records (if any)
    tickdata_ca_validated = list(filter(lambda x: x is not None, tickdata_ca_validated))
    _logger.info("Number of tickers in tickdata universe on %s: %d "
                 "[ca validated]" % (str(end_dt.date()), len(tickdata_ca_validated)))

    # Creating DataFrame out of FOOTER CA data
    footer_cols = ['metadata', 'td_security_id', 'td_end_dt', 'td_symbol', 'td_company_name', 'cusip', 'td_exchange']
    tickdata_ca_validated = pd.DataFrame([(k.split(',')) for k in tickdata_ca_validated], columns=footer_cols) \
        .drop('metadata', axis=1).sort_values('td_symbol').reset_index(drop=True)

    # Saving CA data [FOOTER only] for tickdata universe (!! does not contain tickers with no or empty CA files)
    tickdata_ca_validated.to_csv(filename, index=False)
    return tickdata_ca_validated


def get_latest_tickdata_ca(tickdata_universe, pool_size=10, forced_run=False):
    filename = os.path.join(path, 'td_ca_footer_info_validated.csv')
    start = time.time()
    if os.path.exists(filename) and forced_run is False:
        _logger.info(">>> Reading tickdata CA information from disk as of %s" % str(end_dt.date()))
        tickdata_ca_validated = pd.read_csv(filename)
    else:
        tickdata_ca_validated = _pull_latest_tickdata_ca(tickdata_universe, filename, pool_size)
    _logger.info("Done in : %.2f s" % (time.time() - start))
    return tickdata_ca_validated


# ====================================================================================
# Filtering TickData Universe names
# ====================================================================================

def filter_tickdata_universe(tickdata_ca_validated):
    start = time.time()
    _logger.info(">>> Filtering tickdata CA info for tickers containing '_PR', '_W', '_F'")
    filtered_tickdata_ca = tickdata_ca_validated.copy()

    # Filtering symbols that contains the following
    symbols_to_filter = ['_W', '_F', '_PR']
    for symbol in symbols_to_filter:
        filtered_tickdata_ca = filtered_tickdata_ca.loc[~filtered_tickdata_ca['td_symbol'].str.contains(symbol)]
        filtered_tickdata_ca.reset_index(drop=True, inplace=True)
    _logger.info('Tickers filtered: %d' % (tickdata_ca_validated.shape[0] - filtered_tickdata_ca.shape[0]))
    _logger.info("Done in : %.2f s" % (time.time() - start))

    # Saving info about filtered tickdata universe
    filtered_tickdata_ca.to_csv(os.path.join(path, 'td_ca_footer_info_validated_filtered.csv'), index=False)
    return filtered_tickdata_ca


# ====================================================================================
# Pull CAPIQ data
# ====================================================================================

def _pull_capiq_data(td_ca_data_valid_filtered, filename, chunk_size=100):

    def _apply_template_nd(template, ticker, metric):
        key = "{ticker};{metric}".format(ticker=ticker, metric=metric)
        formula = template.format(ticker=ticker, metric=metric)
        return key, formula

    def _ciq_nd(ticker, metric):
        return _apply_template_nd("""=CIQ("{ticker}", "{metric}")""", ticker, metric)

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
    for i, group in enumerate(_chunker(list(td_ca_data_valid_filtered['td_symbol']), chunk_size)):
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
    td_ca_data_valid_filtered['td_exchange'] = td_ca_data_valid_filtered['td_exchange'].map(td_to_ciq_exchange_map)
    exchange_mapper_values = sorted(set(td_to_ciq_exchange_map.values()))  # there are 2 'AMEX' values in the mapper

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
            capiq_response[tickers_wrong_exchange[symbol]] = capiq_response.pop(symbol)
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
        for ciq_symbol, ciq_record in capiq_response.items():
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
    capiq_data.to_csv(os.path.join(path, filename), header=True, index=False, encoding='utf-8')
    return capiq_data


def get_capiq_data(td_ca_data_valid_filtered, chunk_size, forced_run=False):
    filename = os.path.join(path, 'capiq_data.csv')
    start = time.time()
    if os.path.exists(filename) and forced_run is False:
        _logger.info(">>> Reading CAPIQ data from disk as of %s" % str(end_dt.date()))
        capiq_data = pd.read_csv(filename)
    else:
        capiq_data = _pull_capiq_data(td_ca_data_valid_filtered, filename, chunk_size)
    _logger.info("Done in : %.2f s" % (time.time() - start))
    return capiq_data


# ====================================================================================
# Postprocess and merge TickData and CAPIQ data
# ====================================================================================

def postprocess_tickdata_and_capiq_data(tickdata_data, capiq_data):

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
    capiq_data['isin'] = capiq_data['isin'].apply(lambda x: x.split('_')[1] if x is not pd.np.nan else pd.np.nan)

    _logger.info("Shape of TickData / CAPIQ: {0} {1}".format(tickdata_data.shape, capiq_data.shape))
    _logger.info("Done in : %.2f s" % (time.time() - start))
    return tickdata_data, capiq_data


def merge_tickdata_and_capiq_data(tickdata_data, capiq_data):
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
    tickdata_capiq.to_csv(os.path.join(path, 'tickdata_capiq.csv'), header=True, index=False, encoding='utf-8')
    return tickdata_capiq


# ====================================================================================
# Retrieve RavenPack data from ravenpack_equities table
# ====================================================================================

def _retrieve_ravenpack_data(filename):
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
    rvpack_data['symbol'] = rvpack_data['rp_symbol'].apply(lambda x: x.split('/')[1])  # from "US/AAPL" -> take "AAPL"
    rvpack_data['symbol'] = rvpack_data['symbol'].apply(lambda x: re.sub('[^0-9a-zA-Z]+', '', x))
    rvpack_data['isin'] = rvpack_data['rp_isin'].copy()  # this column is needed when merging to SM at later stage
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
    rvpack_data.to_csv(filename, header=True, index=False, encoding='utf-8')
    return rvpack_data


def get_ravenpack_data(forced_run=False):
    filename = os.path.join(path, 'ravenpack_data.csv')
    start = time.time()
    if os.path.exists(filename) and forced_run is False:
        _logger.info(">>> Reading RavenPack data from disk as of %s" % str(end_dt.date()))
        rvpack_data = pd.read_csv(filename)
    else:
        rvpack_data = _retrieve_ravenpack_data(filename)
    _logger.info("Done in : %.2f s" % (time.time() - start))
    return rvpack_data


# ====================================================================================
# Merge RavenPack and TickData_CAPIQ tables
# ====================================================================================

def merge_tickdatacapiq_with_rvpack(tickdata_capiq, rvpack_data):
    start = time.time()
    _logger.info(">>> Merging TickData_CAPIQ and RavenPack data")

    security_master = tickdata_capiq.merge(rvpack_data, how='left', on=['symbol', 'isin'], copy=True)
    columns = [
        'symbol', 'td_symbol', 'ciq_symbol', 'rp_symbol', 'cusip', 'isin', 'ciq_isin', 'rp_isin', 'td_exchange',
        'exchange', 'trading_currency', 'security_name', 'td_company_name', 'ciq_company_name',
        'rp_company_name', 'company_type', 'industry', 'industry_sector', 'country_name', 'td_security_id',
        'ciq_company_id', 'rp_company_id', 'ciqid_security_item', 'ciqid_trading_item', 'td_end_dt', 'rp_end_dt']

    security_master = security_master[columns].sort_values(['symbol', 'rp_company_id', 'rp_end_dt'])

    # Filter symbols with same symbol, rp_isin, rp_company_name, rp_company_id by taking record with latest 'rp_end_dt'
    # Note: records should be sorted by 'symbol', 'rp_company_id', 'rp_end_dt' (as above)!!
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
    security_master['td_exchange'] = security_master['td_exchange'].map(td_to_ciq_exchange_map)
    security_master.drop(['ciq_isin', 'td_end_dt'], axis=1, inplace=True)

    # Statistics on created table
    _logger.info("Final shape of merged table: {0}".format(security_master.shape))
    _logger.info("Symbols with no RavenPack company_id / isin (excl. ETFs): {0}".format(len(symbols_no_rp_company_id)))
    _logger.info("Done in : %.2f s" % (time.time() - start))
    return security_master


# ====================================================================================
# Look-up companies with missing ISIN in ravenpack_equities table on ravenpack website
# ====================================================================================

def seq_match_similarity(a, b):
    # A method to compute similarity score between company names
    return SequenceMatcher(None, a, b).ratio() * 100


def clean_company_name(comp_name):
    try:
        comp_name = comp_name.decode('ascii', errors='ignore').encode('ascii')
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
        comp_name = comp_name.translate(None, string.punctuation)
    except TypeError as e:
        _logger.error("Problem of removing non-alphanumeric from {0}. Message: {1}".format(comp_name, e))

    return comp_name


def compute_company_name_similarity(df):
    cols_of_interest = ['symbol', 'isin', 'rp_company_id', 'td_company_name',
                        'ciq_company_name', 'rp_company_name', 'rp_end_dt']
    df_temp = df[cols_of_interest].copy()

    for col in ['td_company_name', 'ciq_company_name', 'rp_company_name']:
        df_temp[col + '_clean'] = df_temp[col].apply(lambda x: clean_company_name(x))

    # Compute company names similarity scores
    df_temp['cpq_name_vs_rp_name_sr1'] = df_temp.apply(lambda x: fuzz.ratio(x['ciq_company_name_clean'],
                                                                            x['rp_company_name_clean']), axis=1)
    df_temp['cpq_name_vs_rp_name_sr2'] = df_temp.apply(lambda x: seq_match_similarity(x['ciq_company_name_clean'],
                                                                                      x['rp_company_name_clean']), axis=1)
    df_temp['td_name_vs_rp_name_sr1'] = df_temp.apply(lambda x: fuzz.ratio(x['td_company_name_clean'],
                                                                           x['rp_company_name_clean']), axis=1)
    df_temp['td_name_vs_rp_name_sr2'] = df_temp.apply(lambda x: seq_match_similarity(x['td_company_name_clean'],
                                                                                     x['rp_company_name_clean']), axis=1)

    # Compute min & max company names similarity score (CAPIQ vs RavenPack vs TickData)
    cols_agg = ['cpq_name_vs_rp_name_sr1', 'cpq_name_vs_rp_name_sr2', 'td_name_vs_rp_name_sr1', 'td_name_vs_rp_name_sr2']
    df_temp['min_sr'] = df_temp[cols_agg].apply(min, axis=1)
    df_temp['max_sr'] = df_temp[cols_agg].apply(max, axis=1)

    df_temp = df_temp[cols_of_interest + ['max_sr']].reset_index(drop=True)
    return df_temp


def _process_single_search_result(search_results, company_dict, similarity_keys, symbol, rp_data,
                                  rp_data_for_manual_check, min_thresh_match_str=80.0):
    # Creating dict with rp_company_name, type and company_id
    ravenpack_data = {x[0].rsplit(' ', 1)[0]: {
        'RPCompanyID': x[1], 'Type': re.sub(r'[^\w,]', '', x[0].rsplit(' ', 1)[1])
    } for x in zip(search_results[0::2], search_results[1::2])}
    
    for rp_company_name in ravenpack_data:
        for key in company_dict:
            # Computing similarity between company names [RavenPack vs TickData and CAPIQ]
            sm_comp_name = clean_company_name(company_dict[key])
            rp_comp_name = clean_company_name(rp_company_name)
            sr_1 = fuzz.ratio(sm_comp_name, rp_comp_name)
            sr_2 = seq_match_similarity(sm_comp_name, rp_comp_name)
            ravenpack_data[rp_company_name][key] = company_dict[key]
            ravenpack_data[rp_company_name][key + '_vs_rp_name_sr1'] = sr_1
            ravenpack_data[rp_company_name][key + '_vs_rp_name_sr2'] = sr_2
            _logger.debug("{0} company: {1}; RP Company: {2}. Similarity ratio 1-2: {3:0.2f}%/{4:0.2f}%".format(
                key.upper(), company_dict[key], rp_company_name, sr_1, sr_2))

        # Computing min and max scores for scores for given symbol
        max_sr = max([value for key, value in ravenpack_data[rp_company_name].items() if key in similarity_keys])
        min_sr = min([value for key, value in ravenpack_data[rp_company_name].items() if key in similarity_keys])
        ravenpack_data[rp_company_name]['max_sr'] = max_sr
        ravenpack_data[rp_company_name]['min_sr'] = min_sr

    if len(ravenpack_data) == 1:  # 1 search result
        _logger.debug("1 search result found for symbol {0}: {1}".format(symbol, ravenpack_data.keys()))
        if ravenpack_data[rp_company_name]['max_sr'] >= min_thresh_match_str:
            _logger.debug("{0} is matched to security master with similarity ratio {1:0.2f}%".format(
                rp_company_name, ravenpack_data[rp_company_name]['max_sr']))
            rp_data[symbol]['rp_company_name'] = rp_company_name
            rp_data[symbol]['rp_company_id'] = ravenpack_data[rp_company_name]['RPCompanyID']
            rp_data[symbol]['rp_company_type'] = ravenpack_data[rp_company_name]['Type']
            rp_data[symbol]['max_sr'] = ravenpack_data[rp_company_name]['max_sr']
            rp_data[symbol]['min_sr'] = ravenpack_data[rp_company_name]['min_sr']
        else:
            _logger.debug("Manual check of RP data for symbol {0} is needed".format(symbol))
            rp_data_for_manual_check[symbol] = ravenpack_data
    else:
        _logger.debug("More than 1 search result found for symbol {0}: {1}" \
                      .format(symbol, ravenpack_data.keys()))

        # Selecting rp_company name with max similarity ratio
        rp_company_name = sorted(ravenpack_data.keys(), key=lambda x: (ravenpack_data[x]['max_sr']),
                                 reverse=True)[0]
        if ravenpack_data[rp_company_name]['max_sr'] >= min_thresh_match_str:
            _logger.debug("{0} is matched to security master with similarity ratio {1:0.2f}%".format(
                rp_company_name, ravenpack_data[rp_company_name]['max_sr']))
            rp_data[symbol]['rp_company_name'] = rp_company_name
            rp_data[symbol]['rp_company_id'] = ravenpack_data[rp_company_name]['RPCompanyID']
            rp_data[symbol]['rp_company_type'] = ravenpack_data[rp_company_name]['Type']
            rp_data[symbol]['max_sr'] = ravenpack_data[rp_company_name]['max_sr']
            rp_data[symbol]['min_sr'] = ravenpack_data[rp_company_name]['min_sr']
        else:
            _logger.debug("Manual check of RP data for symbol {0} is needed%".format(symbol))
            rp_data_for_manual_check[symbol] = ravenpack_data


def _lookup_ravenpack_website(security_master, symbols_no_rp_company_id, filename,
                              min_thresh_match_str=80.0, headless=True):
    with setup_firefox_browser(headless) as driver:
        try:
            # Launch firefox browser and login to RavenPack
            login_to_ravenpack_website(driver)

            # Navigate to watchlist where to search news on companies
            driver.find_element_by_xpath("/html/body/div[2]/div[2]/button").click()
            time.sleep(5)

            # Clear search bar
            driver.find_element_by_xpath("/html/body/div[2]/div[3]/div[1]/div[1]/input").clear()
            time.sleep(0.5)

            # Name of columns with company's names similarity scores (2 method to compare strings x 2 comparisons:
            # TickData vs RavenPack and CAPIQ vs RavenPack)
            similarity_keys = ['cpq_name_vs_rp_name_sr1', 'cpq_name_vs_rp_name_sr2',
                               'td_name_vs_rp_name_sr1', 'td_name_vs_rp_name_sr2']
            idx = 0
            rp_data = {}
            no_results_found = []
            rp_data_for_manual_check = {}
            for _, record in security_master.loc[security_master.symbol.isin(symbols_no_rp_company_id)].iterrows():
                idx = idx + 1
                symbol = record['symbol']
                sm_cusip = record['cusip']
                td_company_name = record['td_company_name'].decode('ascii', errors='ignore').encode('ascii')
                try:
                    capiq_company_name = record['ciq_company_name'].decode('ascii', errors='ignore').encode('ascii')
                except UnicodeEncodeError as e:
                    _logger.debug("Symbol: {0}. CAPIQ company name: {1}. Error: {2}".format(
                        symbol, record['td_company_name'], e))
                    capiq_company_name = re.sub(r'[^\x00-\x7F]+', '', record['ciq_company_name'])  # drop non-ASCII
                    capiq_company_name = capiq_company_name.encode('ascii', 'ignore')

                company_dict = {'td_name': td_company_name, 'cpq_name': capiq_company_name}
                _logger.debug("#{0}. Symbol: {1}. Cusip: {2}. CAPIQ company name: {3}".format(
                    idx, symbol, sm_cusip, capiq_company_name))

                rp_data[symbol] = {}
                rp_data[symbol]['cusip'] = sm_cusip
                rp_data[symbol]['td_company_name'] = td_company_name
                rp_data[symbol]['capiq_company_name'] = capiq_company_name

                # Put data in search bar
                driver.find_element_by_xpath("/html/body/div[2]/div[3]/div[1]/div[1]/input").clear()
                driver.find_element_by_xpath("/html/body/div[2]/div[3]/div[1]/div[1]/input").send_keys(sm_cusip)
                time.sleep(2)

                search_results = driver.find_element_by_xpath('//*[@id="ui-id-3"]').text.encode(
                    'ascii', 'ignore').split('\n')
                if len(search_results) > 1:  # (valid search result contains at least one [company_name, rp_company_id])
                    _process_single_search_result(search_results, company_dict, similarity_keys, symbol,
                                                  rp_data, rp_data_for_manual_check, min_thresh_match_str)
                else:
                    _logger.debug("No search results found for symbol {0}".format(symbol))
                    no_results_found.append(symbol)

                # Clearing search field from used cusip
                driver.find_element_by_xpath("/html/body/div[2]/div[3]/div[1]/div[1]/input").clear()
                time.sleep(0.2)
        finally:
            driver.close()

    # Postprocessing results
    rpack_aux_data = pd.DataFrame(rp_data).T.reset_index().rename(columns={'index': 'symbol'})
    rpack_aux_data = rpack_aux_data[['symbol', 'cusip', 'capiq_company_name', 'td_company_name', 'rp_company_name',
                                     'rp_company_id', 'rp_company_type', 'max_sr', 'min_sr']]

    _logger.info("Symbols with no search results found: {0}".format(len(no_results_found)))
    _logger.info("Symbols whose search results require manual check: {0}".format(len(rp_data_for_manual_check.keys())))

    # Saving RavenPack search data to the local disk
    rpack_aux_data.to_csv(filename, header=True, index=False, encoding='utf-8')
    store_txt_file(no_results_found, path, "rvpack_symbols_with_no_search_results.dat")
    store_dict_to_json(rp_data_for_manual_check, path, "rvpack_symbols_requiring_manual_check.json")
    return rpack_aux_data


def get_missing_rvpack_data_from_web(security_master, min_thresh_match_str=80.0, headless_browser=True, forced_run=False):
    # Symbols with no Raven Pack company_id / isin (thus be looked-up by cusip + company name in RavenPack Website)
    # Note: we exclude all Funds since we don't have cusip/isin for them (searching only for Companies)
    mask = (security_master['company_type'] != 'Public Fund') & (security_master['rp_isin'].isnull())
    symbols_no_rp_company_id = sorted(security_master.loc[mask, 'symbol'].values)

    if bool(symbols_no_rp_company_id):
        filename = os.path.join(path, 'fetched_rvpack_website.csv')
        start = time.time()
        if os.path.exists(filename) and forced_run is False:
            _logger.info(">>> Reading RavenPack Auxiliary data from disk as of %s" % str(end_dt.date()))
            rpack_aux_data = pd.read_csv(filename)
        else:
            _logger.info(">>> Checking data for missing RavenPack company_id in the RavenPack website")
            rpack_aux_data = _lookup_ravenpack_website(security_master, symbols_no_rp_company_id, filename,
                                                       min_thresh_match_str, headless_browser)
        # Statistics on pulled Raven Pack Data
        symbols_found_rp_company_id = sorted(rpack_aux_data.loc[rpack_aux_data['rp_company_id'].notnull(), 'symbol'])
        _logger.info("Symbols with no RavenPack company_id before lookup to the website (excl. ETFs): {0}".format(
            len(symbols_no_rp_company_id)))
        _logger.info("Symbols with no RavenPack company_id after lookup to the website (excl. ETFs): {0}".format(
            (len(symbols_no_rp_company_id) - len(symbols_found_rp_company_id))))
        _logger.info("Successfully recovered {0:0.2f}% [{1}] of missing RavenPack company_id".format(
            (len(symbols_found_rp_company_id)) / float(len(symbols_no_rp_company_id)) * 100.,
            len(symbols_found_rp_company_id)))
        _logger.info("Done in : %.2f s" % (time.time() - start))
        return rpack_aux_data
    return None


# ====================================================================================
# Update Security Master records with missing RavenPack ID
# ====================================================================================

def update_security_master_with_missing_rvpackid(security_master, rpack_aux_data):
    if rpack_aux_data is not None and not rpack_aux_data.empty:
        _logger.info(">>> Updating SM records with missing RavenPack company_id")

        # Updating records that have no RavenPack company_id with the data fetched from the website
        columns = ['symbol', 'cusip', 'rp_company_name', 'rp_company_id']
        security_master = security_master.merge(rpack_aux_data[columns], how='left', on=['symbol', 'cusip'],
                                                suffixes=('', '_new'), copy=True)
        columns = ['rp_company_name', 'rp_company_id']
        for col in columns:
            new_col = col + '_new'
            security_master[col] = np.where(pd.notnull(security_master[new_col]), security_master[new_col],
                                            security_master[col])
            security_master.drop(new_col, axis=1, inplace=True)

    _logger.info("Shape of Security Master: {0}".format(security_master.shape))
    return security_master


# ====================================================================================
# Pull ETF universe names (based on S3 chris/etfg/legacy_constituents/)
# ====================================================================================

def _pull_etf_universe_names(col_names):
    _logger.info(">>> Pulling ETF universe from AWS S3")
    s3_client = get_s3_client()
    obj = s3_client.get_object(Bucket=get_config()['bucket'], Key="chris/etfg/20181031_constituents_export.csv")
    etfg_data = pd.read_csv(StringIO(obj['Body'].read()), header=None, parse_dates=[0], index_col=[0],
                            names=col_names)
    return etfg_data


def get_etf_universe_names(forced_run=False):
    col_names = ['Date', 'etp_ticker', 'constitutent_ticker', 'constituent_name', 'constituent_weight',
                 'constituent_market_value', 'constituent_cusip', 'constituent_isin', 'constituent_figi',
                 'constituent_sedol', 'constituent_exchange', 'constituent_iso_country', 'total_shares_held',
                 'asset_class', 'security_type']

    if os.path.exists(os.path.join(path, 'ETF_universe', '20181031_constituents_export.csv')) and forced_run is False:
        _logger.info(">>> Reading ETF universe from local disk")
        etfg_data = pd.read_csv(os.path.join(path, 'ETF_universe', '20181031_constituents_export.csv'),
                                header=None, parse_dates=[0], index_col=[0], names=col_names)
    else:
        etfg_data = _pull_etf_universe_names(col_names)

    etfg_data.columns = etfg_data.columns.str.lower()
    etf_names = sorted(etfg_data['etp_ticker'].unique())
    _logger.info("Number of unique names in ETF universe: %d" % len(etf_names))
    return etf_names


# ====================================================================================
# Validate and Save Security Master
# ====================================================================================

def filter_security_master(security_master, etf_names, drop_no_rp_comp_id=False, drop_etfs_not_in_etfg=False,
                           forced_run=False):
    _logger.info(">>> Filtering Security Master")    
    _logger.info("Shape of SM before cleaning: {0}".format(security_master.shape))
    
    if drop_no_rp_comp_id:
        # Remove records that have no RP company id (e.g. ETFs)
        mask = security_master['rp_company_id'].notnull()
        security_master = security_master.loc[mask].sort_values('symbol').reset_index(drop=True)
        _logger.info("Removing %d names with no rp_company_id" % (len(list(filter(lambda x: not x, mask)))))
        
    if drop_etfs_not_in_etfg:
        # Reading ETF universe (for verifying ETFs in Security Master)
        etf_names = get_etf_universe_names(forced_run)

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


def check_company_names_consistency(security_master, min_thresh_match_str=80.0, drop_comp_name_simil_below_thresh=False):
    _logger.info(">>> Checking consistency of company names")

    # Computing max similarity score for company names: TD vs RP, CAPIQ vs RP (only companies that are in RavenPack)
    company_name_df = compute_company_name_similarity(security_master.loc[security_master['rp_company_id'].notnull()])

    # Adding column to SM
    security_master = security_master.merge(company_name_df[['symbol', 'isin', 'max_sr']], how='left',
                                            on=['symbol', 'isin'])
    _logger.info("Shape of SM before cleaning: {0}".format(security_master.shape))

    # For duplicated records with same 'symbol', 'isin' -> take the record with highest max_sr and then
    # the most recent (i.e. rp_end_dt is max)
    _logger.info("Removing {0} duplicated records with same 'symbol', 'isin' (keeping the latest)".format(
        security_master.loc[security_master.duplicated(subset=['symbol', 'isin'])].shape[0]))
    security_master = security_master.sort_values(['symbol', 'max_sr', 'rp_end_dt'], ascending=[True, False, False]) \
        .groupby(['symbol', 'isin']).head(1)

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

    if drop_comp_name_simil_below_thresh:
        # Remove records that did not pass company name verification (with threshold). Note: majority of these companies
        # had ticker symbol change CA but Raven Pack did not sync with it
        mask = (security_master['max_sr'] < min_thresh_match_str)
        security_master = security_master.loc[~mask].copy()
        _logger.info("Removing {0} symbols from SM [company_names similarity < {1:0.2f}%]".format(
            len(list(filter(lambda x: x, mask))), min_thresh_match_str))
        
    _logger.info("Shape of SM after cleaning: {0}".format(security_master.shape))
    _logger.info("==== Final stats on SM ====")
    describe_security_master(security_master)
    security_master.to_csv(os.path.join(path, "security_master.csv"), header=True, index=False, encoding='utf-8')

    return security_master


def prepare_security_universe_table(security_master):
    columns = [
        'symbol', 'cusip', 'isin', 'td_exchange', 'trading_currency', 'ciq_company_name', 'security_name',
        'td_security_id', 'ciq_company_id', 'ciqid_trading_item', 'rp_company_id'
    ]
    security_universe_table = security_master[columns].copy()
    security_universe_table.insert(loc=0, column='dcm_security_id', value=security_universe_table['td_security_id'])
    security_universe_table['is_active'] = True
    security_universe_table['td_exchange'] = security_universe_table['td_exchange'].map(td_exchange_to_mic_map)

    cols_rename = {
        'symbol': 'ticker', 'td_exchange': 'exchange', 'ciq_company_name': 'company_name',
        'ciqid_trading_item': 'ciq_trading_item_id', 'rp_company_id': 'rp_entity_id'
    }
    security_universe_table.rename(columns=cols_rename, inplace=True)
    security_universe_table.to_csv(os.path.join(path, "security_universe_table.csv"), header=True,
                                   index=False, encoding='utf-8')
    return security_universe_table


def update_universe_txt_github(tickers_to_drop_from_universe, univ_tickers_to_rename):
    if len(tickers_to_drop_from_universe) or len(univ_tickers_to_rename):
        try:
            temp_dir_path = tempfile.mkdtemp()
            repo_dir = "dcm-config"
            filename = "universe.txt"

            universe_txt_file_path = os.path.join(temp_dir_path, repo_dir, "data", "universe", filename)

            # Clone the 'dcm-config' remote GitHub repository.
            in_repo_url = credentials_conf["git"]["config_repo"]
            git_branch = get_config()["security_master"]["dcm_config_branch"]  # branch of dcm-config repo
            git_clone(in_repo_url, temp_dir_path, git_branch)

            if os.path.exists(universe_txt_file_path):
                # Update dcm_universe.txt
                dcm_universe = sorted(read_txt_file(os.path.dirname(universe_txt_file_path), filename))
                dcm_universe_updated = list(set(dcm_universe) - set(tickers_to_drop_from_universe))
                dcm_universe_updated = sorted(
                    map(lambda x: univ_tickers_to_rename[x] if x in univ_tickers_to_rename else x,
                        dcm_universe_updated))
                _logger.info("Size of universe.txt: original [%d] / modified [%d]" % (len(dcm_universe),
                                                                                      len(dcm_universe_updated)))
                # Saving modified universe.txt file
                store_txt_file(dcm_universe_updated, os.path.dirname(universe_txt_file_path), filename)

                # Commit and push the changes
                git_commit('Updated universe.txt file', os.path.abspath(os.path.join(temp_dir_path, repo_dir)))
                git_push(os.path.abspath(os.path.join(temp_dir_path, repo_dir)))
        finally:
            # Delete the temporary directory holding the cloned project
            rmtree(temp_dir_path)


# ====================================================================================
# Main Function
# ====================================================================================

def build_security_master():
    pool_size = 10  # size of multiprocessing pool when pulling CA data from TickData (max. 12)
    chunk_size = 1000  # 1000 (number of symbols to process in 1 batch when pulling CAPIQ)
    forced_run = False  # if True -> retrieve data (prices, CA, CAPIQ, RPack, ...). If false -> read data from disk
    headless_browser = True  # if True -> use headless browser (no GUI launched)

    drop_no_rp_comp_id = False  # if True -> drop records with no rp company id (e.g. ETFs)
    drop_etfs_not_in_etfg = False  # if True -> drop ETFs that are not chris/etfg/legacy_constituents/ universe
    drop_comp_name_simil_below_thresh = False  # if True -> drop records with company name similarity below threshold
    min_thresh_match_str = 80.0  # minimum similarity score threshold [in %] for CAPIQ/TD company names vs RavenPack

    start = time.time()

    # Get TickData tickers universe based on prices available at the given date
    tickdata_universe = get_latest_tickdata_universe(forced_run)

    # Get the most recent DCM universe
    dcm_universe = get_dcm_universe()

    # This line is needed since not all tickers have prices on daily basis (though they have tickdata CA files)
    # tickers_membership = dcm_universe
    tickers_membership = [x.encode() for x in sorted(set(dcm_universe).union(set(tickdata_universe)))]

    # Get CA data [FOOTER] for tickdata universe. This step also filters implicitly tickers with no or empty CA files
    tickdata_ca_validated = get_latest_tickdata_ca(tickers_membership, pool_size, forced_run)

    # Filter tickers that contains e.g. ['_W', '_F', '_PR'] in their names
    tickdata_ca_validated_filtered = filter_tickdata_universe(tickdata_ca_validated)

    # Pull CAPIQ data
    capiq_data = get_capiq_data(tickdata_ca_validated_filtered, chunk_size, forced_run)

    # Postprocess TickData and CAPIQ data
    tickdata_final, capiq_data_final = postprocess_tickdata_and_capiq_data(tickdata_ca_validated_filtered, capiq_data)

    # Merge TickData and CAPIQ data into a security master
    tickdata_capiq = merge_tickdata_and_capiq_data(tickdata_final, capiq_data_final)

    # Retrieve RavenPack data from ravenpack_equities table
    rvpack_data = get_ravenpack_data(forced_run)

    # Merge tickdata_capiq and rvpack_data into Security Master
    security_master = merge_tickdatacapiq_with_rvpack(tickdata_capiq, rvpack_data)

    # Fetching missing RavenPack data
    rpack_aux_data = get_missing_rvpack_data_from_web(security_master, min_thresh_match_str, headless_browser, forced_run)

    # Update missing data in Security Master with fetched data from RavenPack Website
    security_master = update_security_master_with_missing_rvpackid(security_master, rpack_aux_data)

    # Filter security master
    # 1. Drop records with no rp_company_id (e.g. ETFs) if drop_no_rp_comp_id=True
    # 2. Remove ETFs that are not in chris/etfg/legacy_constituents/ universe if drop_etfs_not_in_etfg=True
    security_master = filter_security_master(security_master, drop_no_rp_comp_id, drop_etfs_not_in_etfg, forced_run)
    
    # Check company names consistency
    # 1. Drop records with company name similarity below threshold if drop_comp_name_simil_below_thresh=True
    security_master = check_company_names_consistency(security_master, min_thresh_match_str, 
                                                      drop_comp_name_simil_below_thresh)

    # Prepare Security Universe table
    security_universe_table = prepare_security_universe_table(security_master)

    _logger.info("Total time to build Security Master: %.2f s" % (time.time() - start))


if __name__ == '__main__':
    build_security_master()
