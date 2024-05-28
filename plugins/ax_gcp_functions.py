import os
import pickle
import numpy as np
from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import yfinance as yf


###### QUERIES TO GRAB ticker,sector information, or dcm security ID from security master


def ticker_to_sector(ticker, security_master):
    return security_master[(security_master.ticker == ticker)]["sector"].iloc[0]


def get_sector_sans_secid_matrix(ticker):
    sm_query = "select sector from marketdata.security_master_20230603 where ticker='{}'".format(
        ticker
    )
    client = bigquery.Client()
    query_results = client.query(sm_query).to_dataframe()

    return query_results.iloc[0][0]


def get_all_sectors():
    sm_query = "select distinct sector from marketdata.security_master_20230603"
    client = bigquery.Client()
    query_results = client.query(sm_query).to_dataframe()

    return query_results


def get_all_tickers():
    sm_query = (
        "select distinct dcm_security_id from marketdata.security_master_20230603"
    )
    client = bigquery.Client()
    query_results = client.query(sm_query).to_dataframe()

    return query_results


def gather_full_security_master_df():
    sm_query = "select * from marketdata.security_master_with_sector_enhanced"
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()

    return security_master_tickers


def get_ticker_from_security_id(security_id):
    """

    Args:
        security_id: (int) of DCM security ID

    Returns: ticker (str), ticker that matches security ID

    """

    if type(security_id) == int:
        sm_query = "select ticker from marketdata.security_master_20230603 where dcm_security_id={}".format(
            security_id
        )

    elif type(security_id) == list:
        tuple_of_sec_ids = tuple(security_id)
        sm_query = "select ticker from marketdata.security_master_20230603 where dcm_security_id in {}".format(
            tuple_of_sec_ids
        )

    else:
        raise ValueError(
            "DCM Security ID can only be of data type int or can be put into a list if multiple ID's apply (i.e [777,888,999])"
        )
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    if len(security_master_tickers["ticker"].tolist()) < 2:
        query_results = security_master_tickers["ticker"].tolist()[0]
    else:
        query_results = security_master_tickers["ticker"].tolist()
    return query_results


def get_security_master_full():
    sm_query = "select * from marketdata.security_master_20230603"
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    return security_master_tickers


def get_sector_names_from_ticker(ticker):
    sm_query = "select * from marketdata.security_master_20230603"
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    sector = security_master_tickers[
        security_master_tickers["dcm_security_id"] == ticker
    ].sector.iloc[0]
    id_list = security_master_tickers[
        security_master_tickers["sector"] == sector
    ].dcm_security_id.tolist()
    id_list.remove(ticker)

    return id_list


def get_company_name_from_ticker(ticker):
    sm_query = "select company_name from marketdata.security_master_20230603 where ticker='{}'".format(
        ticker
    )
    client = bigquery.Client()
    security_master_tickers = (
        client.query(sm_query).to_dataframe().values.tolist()[0][0]
    )

    return security_master_tickers


def get_security_id_from_ticker(ticker):
    """

    Args:
        ticker: (str) ticker to get DCM Security ID from

    Returns: security master ID (int)

    """
    if type(ticker) == str:
        sm_query = "select dcm_security_id from marketdata.security_master_20230603 where ticker='{}'".format(
            ticker
        )

    elif type(ticker) == list:
        tuple_of_tickers = tuple(ticker)
        sm_query = "select dcm_security_id from marketdata.security_master_20230603 where ticker in {}".format(
            tuple_of_tickers
        )

    else:
        raise ValueError(
            "Tickers can only be of data type str or can be put into a list if multiple Tickers apply (i.e [AAPL,GOOGL,MSFT])"
        )
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()

    if len(security_master_tickers["dcm_security_id"].tolist()) < 2:
        query_results = security_master_tickers["dcm_security_id"].tolist()[0]

    else:
        query_results = security_master_tickers["dcm_security_id"].tolist()

    return query_results


###### QUERIES TO GRAB SPECIFIC PIECES OF DATA


def grab_csv_from_gcp(bucket, data):
    return pd.read_csv(
        os.path.join("gs://{}".format(bucket), data),
        # index_col=[0]
    )


def gather_security_master():
    """

    Returns: Returns all tickers in security_master

    """
    sm_query = "select ticker from marketdata.security_master_20230603"
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    return security_master_tickers["ticker"].tolist()


def instantiate_gcp_bucket(bucket):
    storage_client = storage.Client()
    # bucket_name = bucket_name
    bucket = storage_client.get_bucket(bucket)
    return storage_client, bucket


class GCPInstance(ABC):
    """@abstractclassmethod
    def get_data():
        NotImplementedError()"""

    @abstractmethod
    def get_bucket(self):
        NotImplementedError()


class GCPInstanceSingleton(GCPInstance):
    __instance = None

    """@staticmethod
    def get_instance():
        if GCPInstanceSingleton.__instance == None:
            GCPInstanceSingleton(os.environ['GCS_BUCKET'])

        return GCPInstanceSingleton.__instance"""

    def __init__(self, bucket_name):
        if GCPInstanceSingleton.__instance != None:
            raise Exception("Singleton cannot be instantiated twice")
        else:
            self.bucket_name = bucket_name
            GCPInstanceSingleton.__instance = self

    def get_bucket(self):
        _, bucket = instantiate_gcp_bucket(self.bucket_name)

        return bucket

    def get_data(self, path):
        # _,bucket = instantiate_gcp_bucket(self.bucket_name)
        csv_data = grab_csv_from_gcp(self.bucket_name, path)
        return csv_data


def grab_interpretation_data(data_type, lookahead, **keyword):
    storage_client, bucket = instantiate_gcp_bucket()

    if keyword["data_format"] == "csv":
        shap_file_location = os.path.join(
            "decision_support", "{}/{}/{}.csv".format(data_type, lookahead, data_type)
        )
        return pd.read_csv(
            os.path.join("gs://{}".format(os.environ["GCS_BUCKET"]), shap_file_location)
        )

    else:
        pkl_file_location = os.path.join(
            "decision_support",
            "{}/{}/{}.{}".format(
                data_type, lookahead, data_type, keyword["data_format"]
            ),
        )

        blob = bucket.blob(pkl_file_location)
        pickle_in = blob.download_as_string()
        model = pickle.loads(pickle_in)

        return model


def fetch_returns(security_id, return_col="ret_21B"):
    tuple_of_sec_ids = tuple(security_id)
    sm_query = (
        "select date,ticker,{} from marketdata.past_returns where ticker in {}".format(
            return_col, tuple_of_sec_ids
        )
    )

    client = bigquery.Client()
    returns = client.query(sm_query).to_dataframe()
    returns["date"] = returns["date"].apply(pd.Timestamp)
    return returns.set_index("date")


def active_matrix_return_cross_product(returns_df, active_matrix):
    product_df = pd.DataFrame(
        index=returns_df.index, columns=returns_df.columns
    ).fillna(0)

    for date, row in active_matrix.iterrows():
        # Get the next date from active_matrix
        next_date = active_matrix.index[active_matrix.index > date].min()
        if pd.isna(next_date):
            # If it's the last date in the active_matrix, use weights for all subsequent dates
            date_slice = returns_df.loc[date:]
        else:
            date_slice = returns_df.loc[date : next_date - pd.Timedelta(days=1)]

        for ticker in row.index:
            product_df.loc[date_slice.index, ticker] = date_slice[ticker] * row[ticker]

    return product_df[(product_df.index >= active_matrix.index[0])].sum(axis=1)


def return_data_portfolio(sec_ids, all_tickers, active_matrix, return_col="ret_21B"):
    returns_data = fetch_returns(sec_ids)
    returns_df = returns_data.pivot(columns="ticker", values=return_col).fillna(0)

    ticker_id_dict = dict(zip(sec_ids, all_tickers))
    returns_df.rename(columns=ticker_id_dict, inplace=True)
    valid_tickers = returns_df.columns.tolist()
    valid_active_matrix = active_matrix[valid_tickers]
    port_returns = active_matrix_return_cross_product(returns_df, valid_active_matrix)
    benchmark_return_data = fetch_benchmark_returns(return_col)
    benchmark_return_truncated = benchmark_return_data[
        (benchmark_return_data.index >= port_returns.index[0])
    ]
    portfolio_rolling_ret = (1 + port_returns).cumprod() - 1
    benchmark_rolling_ret = (1 + benchmark_return_data).cumprod() - 1
    return portfolio_rolling_ret, benchmark_rolling_ret


def fetch_benchmark_returns(ret_col):
    sm_query = "SELECT date,ticker,{} FROM `marketdata.past_returns` where ticker in (21517)".format(
        ret_col
    )

    client = bigquery.Client()
    returns = client.query(sm_query).to_dataframe()
    returns["date"] = returns["date"].apply(pd.Timestamp)
    return returns.groupby(["date"]).mean()[ret_col]


def get_available_signals(tickers):
    tuple_of_sec_ids = tuple(list(tickers))

    sm_query = (
        "select * from marketdata.value_growth_predictions where ticker in {}".format(
            tuple_of_sec_ids
        )
    )

    client = bigquery.Client()
    return_signals = client.query(sm_query).to_dataframe()
    last_date = sorted(return_signals.date.unique())[-1]
    signal_predictions_1 = return_signals[(return_signals.date == last_date)]
    ticker_to_signal_dict = (
        signal_predictions_1[["ticker", "ensemble_qt"]]
        .set_index(["ticker"])
        .to_dict()["ensemble_qt"]
    )
    return ticker_to_signal_dict


def get_security_id_from_ticker_mapper(ticker):
    """

    Args:
        ticker: (str) ticker to get DCM Security ID from

    Returns: security master ID (int)

    """
    if type(ticker) == str:
        sm_query = "select ticker,dcm_security_id from marketdata.security_master_20230603 where ticker='{}'".format(
            ticker
        )

    elif type(ticker) == list:
        tuple_of_tickers = tuple(ticker)
        sm_query = "select ticker,dcm_security_id from marketdata.security_master_20230603 where ticker in {}".format(
            tuple_of_tickers
        )

    else:
        raise ValueError(
            "Tickers can only be of data type str or can be put into a list if multiple Tickers apply (i.e [AAPL,GOOGL,MSFT])"
        )
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    security_master_tickers.set_index(["ticker"], inplace=True)

    if len(security_master_tickers["dcm_security_id"].tolist()) < 2:
        query_results = security_master_tickers

    else:
        query_results = security_master_tickers

    return query_results


def get_ticker_from_security_id_mapper(security_id):
    """

    Args:
        security_id: (int) of DCM security ID

    Returns: ticker (str), ticker that matches security ID

    """

    if len(security_id) == 1:
        sm_query = "select ticker,dcm_security_id from marketdata.security_master_20230603 where dcm_security_id={}".format(
            security_id[0]
        )

    elif type(security_id) == list:
        tuple_of_sec_ids = tuple(security_id)
        sm_query = "select ticker,dcm_security_id from marketdata.security_master_20230603 where dcm_security_id in {}".format(
            tuple_of_sec_ids
        )

    else:
        raise ValueError(
            "DCM Security ID can only be of data type int or can be put into a list if multiple ID's apply (i.e [777,888,999])"
        )
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    security_master_tickers.set_index(["dcm_security_id"], inplace=True)

    if len(security_master_tickers["ticker"].tolist()) < 2:
        query_results = security_master_tickers
    else:
        query_results = security_master_tickers
    return query_results


def map_trading_book_to_predictions(trading_book, tick_to_secid, all_curr_signals):

    signal_to_text = {
        4: "Strong Buy",
        3: "Weak Buy",
        2: "Neutral",
        1: "Weak Sell",
        0: "Strong Sell",
    }

    trading_book_tickers = [i['ticker'] for i in trading_book]

    signals_from_trading_book = (
        all_curr_signals[(all_curr_signals.ticker.isin(trading_book_tickers))][
            ['ticker', 'signal']
        ]
        .set_index(['ticker'])['signal']
        .to_dict()
    )

    def verify_signal_exists(asset, tick_to_secid, signals_from_trading_book):
        asset_sec_id = asset
        if asset_sec_id in signals_from_trading_book.keys():
            prediction = signal_to_text[signals_from_trading_book[asset_sec_id]]
            # asset["AssetX Signal"] = prediction
            return prediction

        else:
            # asset["AssetX Signal"] = "No Forecast"
            return "No Forecast"

    trading_book_signals = [
        verify_signal_exists(asset, tick_to_secid, signals_from_trading_book)
        for asset in trading_book_tickers
    ]

    return dict(zip(trading_book_tickers, trading_book_signals))


def get_top_prediction_by_signal(date, signal):
    sm_query = "select * from marketdata.value_growth_predictions where date ='{}' and ensemble_qt={}".format(
        date, signal
    )
    client = bigquery.Client()
    return_signals = client.query(sm_query).to_dataframe()
    return_signals.sort_values(by=["future_ret_5B"], ascending=False)
    return return_signals.sort_values(by=["future_ret_5B"], ascending=False)[
        ["ticker", "ensemble_qt"]
    ][0:10]


def select_all_distinct_tickers_from_factor_attribution():
    value_tickers_query = "select distinct ticker from marketdata.value_shap"
    growth_tickers_query = "select distinct ticker from marketdata.growth_shap"

    client = bigquery.Client()
    value_tickers = client.query(value_tickers_query).to_dataframe()
    growth_tickers = client.query(growth_tickers_query).to_dataframe()
    return list(
        set(growth_tickers['ticker'].tolist() + value_tickers['ticker'].tolist())
    )


def grab_single_name_prediction(model_type, ticker):
    if model_type == 'value':
        sm_query = (
            "select * from marketdata.value_predictions_2023 where ticker = {}".format(
                ticker
            )
        )
        client = bigquery.Client()
        data = client.query(sm_query).to_dataframe()

        if len(data) == 0:
            sm_query = "select * from marketdata.growth_predictions_2023 where ticker = {}".format(
                ticker
            )
            data = client.query(sm_query).to_dataframe()

    else:

        sm_query = (
            "select * from marketdata.growth_predictions_2023 where ticker = {}".format(
                ticker
            )
        )
        client = bigquery.Client()
        data = client.query(sm_query).to_dataframe()

        if len(data) == 0:
            sm_query = "select * from marketdata.value_predictions_2023 where ticker = {}".format(
                ticker
            )
            data = client.query(sm_query).to_dataframe()

    data.sort_values(by=['date'], inplace=True)

    return data


def grab_multiple_predictions(model_type, ticker):
    if model_type == 'value':
        try:
            sm_query = "select * from marketdata.value_predictions_2023 where ticker in {}".format(
                tuple(ticker)
            )
            client = bigquery.Client()
            data = client.query(sm_query).to_dataframe()

        except Exception:
            sm_query = "select * from marketdata.growth_predictions_2023 where ticker in {}".format(
                tuple(ticker)
            )
            client = bigquery.Client()
            data = client.query(sm_query).to_dataframe()

    else:
        try:
            sm_query = "select * from marketdata.growth_predictions_2023 where ticker in {}".format(
                tuple(ticker)
            )
            client = bigquery.Client()
            data = client.query(sm_query).to_dataframe()

        except Exception:
            sm_query = "select * from marketdata.value_predictions_2023 where ticker in {}".format(
                tuple(ticker)
            )
            client = bigquery.Client()
            data = client.query(sm_query).to_dataframe()

    data.sort_values(by=['date'], inplace=True)
    # data.set_index(['date'],inplace=True)
    return data


def pull_single_name_returns(ticker, end='2023-10-01'):
    """sm_query = ("select * from marketdata.past_returns_2009 where ticker = {}".format(ticker))
    client = bigquery.Client()
    data = client.query(sm_query).to_dataframe()

    # Need to add catch for if we dont have the data
    return data.sort_values(by=['date'])"""

    ticker_pnl = yf.download([ticker], end=end)
    ticker_pnl['ret_1B'] = ticker_pnl['Adj Close'].pct_change()
    ticker_pnl.dropna(inplace=True)
    ticker_pnl.index.rename('date', inplace=True)
    return ticker_pnl


def calculate_looped_returns_from_past_signals(returns_df, strike_dates, col):
    # Number of days to analyze
    days_to_analyze = 30

    # Create a dictionary to store the data for the final DataFrame
    data_dict = {}

    # Loop through each strike date
    for strike_date in strike_dates:
        # Find the index of the strike date in the returns DataFrame
        index = returns_df.index.get_loc(strike_date)

        # Extract the next N days of returns
        next_days_returns = returns_df.iloc[index + 1 : index + 1 + days_to_analyze][
            col
        ]

        # If the length of next_days_returns is less than days_to_analyze,
        # pad it with NaN values
        if len(next_days_returns) < days_to_analyze:
            next_days_returns = next_days_returns.tolist()
            next_days_returns.extend(
                [np.nan] * (days_to_analyze - len(next_days_returns))
            )

        else:
            next_days_returns = next_days_returns.tolist()

        # Store the returns in the data dictionary
        data_dict[strike_date] = next_days_returns

    # Create the final DataFrame
    result_df = pd.DataFrame(data_dict)

    # Rename the index to represent the days
    result_df.index = range(0, days_to_analyze)

    return result_df


def group_flattened_returns_to_quantile(pnl_timeseries):
    cumulative_prod = (((1 + pnl_timeseries).cumprod()) - 1).iloc[-1].to_frame()
    cumulative_prod["cut"] = pd.qcut(
        cumulative_prod.iloc[:, 0], q=5, labels=False, duplicates="drop"
    ).values
    cumulative_prod.sort_values(by="cut", inplace=True)
    cumulative_prod_dict = {
        k: pnl_timeseries[
            cumulative_prod[(cumulative_prod["cut"] == k)].index.tolist()
        ].mean(axis=1)
        for k in cumulative_prod["cut"].unique()
    }
    return pd.DataFrame(cumulative_prod_dict)


def return_summary_chart(rating, signal_past_rolling_returns):
    mapper = {4: 'buy', 3: 'buy', 2: 'neutral', 1: 'sell', 0: 'sell'}

    rolling_len = len(signal_past_rolling_returns)
    rolling_avg_df = signal_past_rolling_returns.rolling(rolling_len).mean()

    if rating == 4 or rating == 3:
        positive_percentage = (
            (rolling_avg_df > 0).sum().sum()
            / signal_past_rolling_returns.shape[1]
            * 100
        )
        hit_ratio = str(round(positive_percentage, 2)) + '%'
    elif rating == 1 or rating == 0:
        negative_percentage = (
            (rolling_avg_df < 0).sum().sum()
            / signal_past_rolling_returns.shape[1]
            * 100
        )
        hit_ratio = str(round(negative_percentage, 2)) + '%'
    elif rating == 2:
        hit_ratio = 'X.XX%'

    if rating != 2:
        sentence = 'AssetX has detected a {} signal {} times in the past based on the current set of factors driving the asset\'s movement. Out of the distinct {} times this signal has been triggered, the hit ratio of how many times I\'ve been correct is {}'.format(
            mapper[rating],
            len(signal_past_rolling_returns.columns),
            len(signal_past_rolling_returns.columns),
            hit_ratio,
        )
    else:
        sentence = 'AssetX has detected a {} signal {} times in the past based on the current set of factors driving the asset\'s movement. This ambivalent signal has been triggered {} times in the past,'.format(
            mapper[rating],
            len(signal_past_rolling_returns.columns),
            len(signal_past_rolling_returns.columns),
        )

    return hit_ratio, sentence


def signal_to_forecast(signal):
    signal_ticker = signal['ticker'][0]
    ticker_name = get_ticker_from_security_id_mapper([signal['ticker'][0]])[
        'ticker'
    ].iloc[0]
    current_signal = signal.iloc[-1]['ensemble_qt']
    historical_signal_dates = signal[(signal['ensemble_qt'] == current_signal)][
        'date'
    ].to_list()
    historical_signal_dates = [i.strftime('%Y-%m-%d') for i in historical_signal_dates]
    single_name_returns = pull_single_name_returns(ticker_name)
    # single_name_returns.set_index(['date'], inplace=True)
    single_name_returns.index = [
        i.strftime('%Y-%m-%d') for i in single_name_returns.index
    ]
    if len(historical_signal_dates) <= 5:
        signal_past_rolling_returns = calculate_looped_returns_from_past_signals(
            single_name_returns, historical_signal_dates, col='ret_1B'
        )
    else:
        signal_past_rolling_returns = calculate_looped_returns_from_past_signals(
            single_name_returns, historical_signal_dates[0:-1], col='ret_1B'
        )

    quantiles = group_flattened_returns_to_quantile(signal_past_rolling_returns)
    signal_hit_ratio, signal_sentence = return_summary_chart(
        current_signal, signal_past_rolling_returns
    )
    median_returns = round((((1 + quantiles).cumprod() - 1).iloc[-1].median()) * 100, 2)
    potential_paths = pd.DataFrame([])
    pnl = single_name_returns
    six_month_lookback = (pd.Timestamp(datetime.now()) - pd.DateOffset(282)).strftime(
        "%Y-%m-%d"
    )
    pnl = pnl[(pnl.index > six_month_lookback)]
    ticker_pnl = pnl['ret_1B']
    ticker_pnl.index = [pd.Timestamp(i) for i in ticker_pnl.index]
    forecast_indexs = [
        ticker_pnl.index[-1] + pd.DateOffset(i) for i in range(0, len(quantiles))
    ]
    quantiles.index = forecast_indexs

    ticker_pnl_with_forecast_mean = (
        1
        + pd.Series(
            ticker_pnl.values.tolist()
            + quantiles.iloc[1:].median(axis=1).values.tolist(),
            index=ticker_pnl.index.tolist()
            + quantiles.iloc[1:].median(axis=1).index.tolist(),
        )
    ).cumprod() - 1
    final_date = ticker_pnl.index[-1].strftime("%Y-%m-%d")
    for i in quantiles.columns:
        potential_paths[i] = (
            1
            + pd.Series(
                ticker_pnl.values.tolist() + quantiles[i].iloc[1:].values.tolist(),
                index=ticker_pnl.index.tolist() + quantiles[i].iloc[1:].index.tolist(),
            )
        ).cumprod() - 1

    ticker_pnl_with_forecast_lower_range = potential_paths.loc[
        forecast_indexs[1] :
    ].min(axis=1)
    ticker_pnl_with_forecast_upper_range = potential_paths.loc[
        forecast_indexs[1] :
    ].max(axis=1)

    pnl_up_to_forecast = ticker_pnl_with_forecast_mean.iloc[
        : -len(ticker_pnl_with_forecast_upper_range)
    ]

    pd.Series(
        pnl_up_to_forecast.values.tolist()
        + ticker_pnl_with_forecast_upper_range.values.tolist(),
        index=pnl_up_to_forecast.index.tolist()
        + ticker_pnl_with_forecast_upper_range.index.tolist(),
    )

    upper_range_forecast = pd.Series(
        pnl_up_to_forecast.values.tolist()
        + ticker_pnl_with_forecast_upper_range.values.tolist(),
        index=pnl_up_to_forecast.index.tolist()
        + ticker_pnl_with_forecast_upper_range.index.tolist(),
    )

    lower_range_forecast = pd.Series(
        pnl_up_to_forecast.values.tolist()
        + ticker_pnl_with_forecast_lower_range.values.tolist(),
        index=pnl_up_to_forecast.index.tolist()
        + ticker_pnl_with_forecast_lower_range.index.tolist(),
    )

    ticker_pnl_with_forecast_mean.index = [
        i.strftime("%Y-%m-%d") for i in ticker_pnl_with_forecast_mean.index
    ]
    lower_range_forecast.index = [
        i.strftime("%Y-%m-%d") for i in lower_range_forecast.index
    ]
    upper_range_forecast.index = [
        i.strftime("%Y-%m-%d") for i in upper_range_forecast.index
    ]

    final_tick_pnl = ticker_pnl_with_forecast_mean.loc[:final_date]
    forecast_pnl = ticker_pnl_with_forecast_mean.loc[final_date:]
    forecast_pnl_upper = ticker_pnl_with_forecast_upper_range.loc[final_date:]
    forecast_pnl_lower = ticker_pnl_with_forecast_lower_range.loc[final_date:]
    forecast_days = [i.strftime('%Y-%m-%d') for i in forecast_indexs[1:]]

    LINESERIES = [
        {'time': k, 'value': final_tick_pnl.loc[k]} for k in final_tick_pnl.index
    ]
    AVGSERIES = [{'time': k, 'value': forecast_pnl.loc[k]} for k in forecast_pnl.index]
    BOTTOM_SERIES = [
        {'time': k.strftime('%Y-%m-%d'), 'value': forecast_pnl_lower.loc[k]}
        for k in forecast_pnl_lower.index
    ]
    TOP_SERIES = [
        {'time': k.strftime('%Y-%m-%d'), 'value': forecast_pnl_upper.loc[k]}
        for k in forecast_pnl_upper.index
    ]

    return (
        {
            'lineSeries': LINESERIES,
            'average_series': AVGSERIES,
            'bottom_forecast_series': BOTTOM_SERIES,
            'top_forecast_series': TOP_SERIES,
        },
        signal_hit_ratio,
        signal_sentence,
        median_returns,
    )


"""def grab_feature_importance_data(model_type, date, tickers):
    if model_type == "value":
        if type(tickers) == list:
            sm_query = "select * from marketdata.value_shap where ticker in {}".format(tuple(tickers))
        else:
            sm_query = "select * from marketdata.value_shap where ticker={}".format(tickers)
    else:
        if type(tickers) == list:
            sm_query = "select * from marketdata.growth_shap where ticker in {}".format(tuple(tickers))
        else:
            sm_query = "select * from marketdata.growth_shap where ticker={}".format(tickers)

    client = bigquery.Client()
    feature_importance = client.query(sm_query).to_dataframe()
    feature_importance.set_index(["date", "ticker"], inplace=True)
    return feature_importance"""


def grab_feature_importance_data(model_type, date, tickers):
    if model_type == 'value':
        value_shap = pd.read_csv(
            'gs://dcm-prod-ba2f-us-dcm-data-test/alex/value_shap (2).csv'
        ).set_index(['date', 'ticker'])
        large_cap_value_shap = pd.read_csv(
            'gs://dcm-prod-ba2f-us-dcm-data-test/alex/value_shap_largecap (1).csv'
        ).set_index(['date', 'ticker'])

        ALL_SHAP = pd.concat([value_shap, large_cap_value_shap]).drop_duplicates()
    else:
        growth_shap = pd.read_csv(
            'gs://dcm-prod-ba2f-us-dcm-data-test/alex/growth_shap (2).csv'
        ).set_index(['date', 'ticker'])
        large_cap_growth_shap = pd.read_csv(
            'gs://dcm-prod-ba2f-us-dcm-data-test/alex/growth_shap_largecap (1).csv'
        ).set_index(['date', 'ticker'])

        ALL_SHAP = pd.concat([growth_shap, large_cap_growth_shap])

    return ALL_SHAP[(ALL_SHAP.index.get_level_values(1).isin(tickers))]


def get_sector_ids_from_ticker(ticker):
    sm_query = 'select distinct dcm_security_id from `marketdata.security_master_20230603` where Sector = (select Sector from `marketdata.security_master_20230603` where dcm_security_id={})'.format(
        ticker
    )

    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    return security_master_tickers


def get_company_information_from_ticker(ticker):
    sm_query = 'select distinct ticker, company_name,Sector,dcm_security_id from `marketdata.security_master_20230603` where Sector = (select Sector from `marketdata.security_master_20230603` where dcm_security_id={})'.format(
        ticker
    )
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    return security_master_tickers


def get_selected_company_info(ticker):
    sm_query = 'select distinct ticker, company_name,Sector,dcm_security_id from `marketdata.security_master_20230603` where dcm_security_id in {}'.format(
        tuple(ticker)
    )
    client = bigquery.Client()
    security_master_tickers = client.query(sm_query).to_dataframe()
    return security_master_tickers


import yfinance as yf


def signal_to_forecast2(
    signal_past_rolling_returns, current_signal, single_name_returns
):

    quantiles = group_flattened_returns_to_quantile(signal_past_rolling_returns)
    signal_hit_ratio, signal_sentence = return_summary_chart(
        current_signal, signal_past_rolling_returns
    )
    median_returns = round((((1 + quantiles).cumprod() - 1).iloc[-1].median()) * 100, 2)
    potential_paths = pd.DataFrame([])
    pnl = single_name_returns
    six_month_lookback = (pd.Timestamp(datetime.now()) - pd.DateOffset(282)).strftime(
        "%Y-%m-%d"
    )
    pnl = pnl[(pnl.index > six_month_lookback)]
    ticker_pnl = pnl['Adj Close']
    ticker_pnl.index = [pd.Timestamp(i) for i in ticker_pnl.index]
    forecast_indexs = [
        ticker_pnl.index[-1] + pd.DateOffset(i) for i in range(0, len(quantiles))
    ]
    quantiles.index = forecast_indexs

    ticker_pnl_with_forecast_mean = (
        1
        + pd.Series(
            ticker_pnl.values.tolist()
            + quantiles.iloc[1:].median(axis=1).values.tolist(),
            index=ticker_pnl.index.tolist()
            + quantiles.iloc[1:].median(axis=1).index.tolist(),
        )
    ).cumprod() - 1
    final_date = ticker_pnl.index[-1].strftime("%Y-%m-%d")
    for i in quantiles.columns:
        potential_paths[i] = (
            1
            + pd.Series(
                ticker_pnl.values.tolist() + quantiles[i].iloc[1:].values.tolist(),
                index=ticker_pnl.index.tolist() + quantiles[i].iloc[1:].index.tolist(),
            )
        ).cumprod() - 1

    ticker_pnl_with_forecast_lower_range = potential_paths.loc[
        forecast_indexs[1] :
    ].min(axis=1)
    ticker_pnl_with_forecast_upper_range = potential_paths.loc[
        forecast_indexs[1] :
    ].max(axis=1)

    pnl_up_to_forecast = ticker_pnl_with_forecast_mean.iloc[
        : -len(ticker_pnl_with_forecast_upper_range)
    ]

    pd.Series(
        pnl_up_to_forecast.values.tolist()
        + ticker_pnl_with_forecast_upper_range.values.tolist(),
        index=pnl_up_to_forecast.index.tolist()
        + ticker_pnl_with_forecast_upper_range.index.tolist(),
    )

    upper_range_forecast = pd.Series(
        pnl_up_to_forecast.values.tolist()
        + ticker_pnl_with_forecast_upper_range.values.tolist(),
        index=pnl_up_to_forecast.index.tolist()
        + ticker_pnl_with_forecast_upper_range.index.tolist(),
    )

    lower_range_forecast = pd.Series(
        pnl_up_to_forecast.values.tolist()
        + ticker_pnl_with_forecast_lower_range.values.tolist(),
        index=pnl_up_to_forecast.index.tolist()
        + ticker_pnl_with_forecast_lower_range.index.tolist(),
    )

    ticker_pnl_with_forecast_mean.index = [
        i.strftime("%Y-%m-%d") for i in ticker_pnl_with_forecast_mean.index
    ]
    lower_range_forecast.index = [
        i.strftime("%Y-%m-%d") for i in lower_range_forecast.index
    ]
    upper_range_forecast.index = [
        i.strftime("%Y-%m-%d") for i in upper_range_forecast.index
    ]

    final_tick_pnl = ticker_pnl_with_forecast_mean.loc[:final_date]
    forecast_pnl = ticker_pnl_with_forecast_mean.loc[final_date:]
    forecast_pnl_upper = ticker_pnl_with_forecast_upper_range.loc[final_date:]
    forecast_pnl_lower = ticker_pnl_with_forecast_lower_range.loc[final_date:]
    forecast_days = [i.strftime('%Y-%m-%d') for i in forecast_indexs[1:]]

    LINESERIES = [
        {'time': k, 'value': final_tick_pnl.loc[k]} for k in final_tick_pnl.index
    ]
    AVGSERIES = [{'time': k, 'value': forecast_pnl.loc[k]} for k in forecast_pnl.index]
    BOTTOM_SERIES = [
        {'time': k.strftime('%Y-%m-%d'), 'value': forecast_pnl_lower.loc[k]}
        for k in forecast_pnl_lower.index
    ]
    TOP_SERIES = [
        {'time': k.strftime('%Y-%m-%d'), 'value': forecast_pnl_upper.loc[k]}
        for k in forecast_pnl_upper.index
    ]

    return (
        {
            'lineSeries': LINESERIES,
            'average_series': AVGSERIES,
            'bottom_forecast_series': BOTTOM_SERIES,
            'top_forecast_series': TOP_SERIES,
        },
        signal_hit_ratio,
        signal_sentence,
        median_returns,
    )


def query_function(query):
    client = bigquery.Client()
    return client.query(query).to_dataframe()


def v3_feature_importance(ticker):
    ticker = 24
    query_len_dict = {
        'growth': 'select COUNT(*) from `marketdata.growth_shap2023` where ticker={}',
        'value': 'select COUNT(*) from `marketdata.value_shap_2023` where ticker={}',
        #'largecap_growth': 'select COUNT(*) from `marketdata.growth_shap_largecap2023` where ticker={}',
        #'largecap_value': 'select COUNT(*) from `marketdata.value_shap_largecap2023` where ticker={}'
    }

    query_dict = {
        'growth': 'select * from `marketdata.growth_shap2023` where ticker={}',
        'value': 'select * from `marketdata.value_shap_2023` where ticker={}',
        #'largecap_growth': 'select * from `marketdata.growth_shap_largecap2023` where ticker={}',
        #'largecap_value': 'select * from `marketdata.value_shap_largecap2023` where ticker={}'
    }

    result_dict = {
        'growth': None,
        'value': None,
        #'largecap_growth':None,
        #'largecap_value':None
    }

    for k in result_dict.keys():
        result_dict[k] = query_function(query_len_dict[k].format(ticker))['f0_'][0]

    top_key = max(result_dict, key=result_dict.get)
    return query_function(query_dict[top_key].format(ticker))


def v3_feature_importance_edit(ticker):
    ticker = 24
    query_len_dict = {
        'growth': 'select COUNT(*) from `marketdata.growth_shap2023` where ticker={}',
        'value': 'select COUNT(*) from `marketdata.value_shap_2023` where ticker={}',
        #'largecap_growth': 'select COUNT(*) from `marketdata.growth_shap_largecap2023` where ticker={}',
        #'largecap_value': 'select COUNT(*) from `marketdata.value_shap_largecap2023` where ticker={}'
    }

    query_dict = {
        'growth': 'select * from `marketdata.growth_shap2023` where ticker={}',
        'value': 'select * from `marketdata.value_shap_2023` where ticker={}',
        #'largecap_growth': 'select * from `marketdata.growth_shap_largecap2023` where ticker={}',
        #'largecap_value': 'select * from `marketdata.value_shap_largecap2023` where ticker={}'
    }

    result_dict = {
        'growth': None,
        'value': None,
        #'largecap_growth':None,
        #'largecap_value':None
    }

    for k in result_dict.keys():
        result_dict[k] = query_function(query_len_dict[k].format(ticker))['f0_'][0]

    top_key = max(result_dict, key=result_dict.get)
    return query_function(query_dict[top_key].format(ticker)), top_key


def fetch_raw_data(ticker):
    query = 'select * from `marketdata.merged_data_econ_industry` where ticker ={}'
    return query_function(query.format(ticker))
