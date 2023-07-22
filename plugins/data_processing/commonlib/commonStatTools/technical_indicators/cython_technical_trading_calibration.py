from __future__ import print_function
#from particle_swarm import ParticleSwarmOptimization, technical_strategy_performance_sharpe, \
#     calculate_return_from_digitized_decisons, technical_strategy_performance, technical_strategy_performance_calmar, \
#     technical_strategy_performance_sortino, ParticleSwarmOptimizationFitFunction
from copy import deepcopy
import itertools
from functools import reduce
import pandas as pd
from datetime import datetime, timedelta, time, date
from concurrent.futures import ThreadPoolExecutor
#TODO: when this files is transfered to intuition trading, cleanup these imports
import os, sys
import tempfile
#sys.path.append(os.path.join(os.path.dirname(__file__),"..","..","..","strategies", "cointegration"))
#sys.path.append(os.path.join(os.path.dirname(__file__),"..","..")) #TODO: check import
import online_indicators_cython as onc
import offline
#from technical_trading_indicators import TechnicalTradingSignal, generic_technical_digitization, \
#     create_technical_trading_cohorts_from_dict_specification
from commonlib.config import get_config
from commonlib.util_functions import retrieve_and_initialize_class_instance_with_parameters, bind_parameters_in_standalone_function
from commonlib.intuition_base_objects  import DCMException
from historical_equity_data_provider import HistoricalEquities
import json
from sqlalchemy import create_engine
from contextlib import closing

import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict
#import wingdbstub

import boto3
from boto3 import resource as aws_resource, client as aws_client, setup_default_session as boto3_setup_default_session
import StringIO
global import_global_modules

SDEV_LOOKBACK = "252B"
MIN_CALMAR_THRESHOLD = 0.0
EQUITY_TABLE_NAME = HistoricalEquities.__tablename__

def calculate_return_from_digitized_decisons(aggregate_digitize, returns, fee):
    position_signals = aggregate_digitize[np.abs(aggregate_digitize.values) == 1.0].reindex(aggregate_digitize.index)
    closing_signals = pd.Series(0, aggregate_digitize.index[(np.sign(aggregate_digitize).diff() != 0)])
    aggregate_rec = position_signals.combine_first(closing_signals).fillna(method="ffill").fillna(0)
    pnl = aggregate_rec.shift()*returns
    pnl_with_fees = pnl - aggregate_rec.diff().abs()*fee
    return pnl_with_fees

def load_python_conf(filepath, context=None):
    try:
        with closing(open(filepath, 'r')) as f:
            return eval(f.read())
    except Exception as e:
        print("Could not load config from %s as python dict: %s", filepath, e)
    return None


class TechnicalTradingSignalHistoricalSimulator(object):
    '''
    A class that collects the historical trading signal recommendations against market data
    '''
    def __init__(self, technical_indicator, price_series, with_details=False):
        if not isinstance(price_series, pd.Series):
            raise DCMException("The parameter 'price_series' must be of type pandas Series; received type {0}"
                               .format(type(price_series)))
        if not isinstance(technical_indicator, onc.TechnicalTradingSignalBase):
            raise DCMException("The parameter 'technical_indicator' must be of type TechnicalTradingSignalBase; "
                               "received type {0}".format(type(technical_indicator)))
        self.technical_indicator = technical_indicator
        self.price_series = price_series
        self.with_details = with_details
        self.last_run_date = self.price_series.index[0] - pd.tseries.offsets.Minute()

    def run_against_historical_data(self, until_dt):
        price_data = self.price_series[(self.price_series.index > self.last_run_date)
                                       & (self.price_series.index <= until_dt)]
        recs = []
        signals = []
        n = 0
        self.previous_run_date = self.last_run_date
        orig_price_data = price_data.copy()
        price_data.index = price_data.index.asi8.astype(long)
        for d,value in price_data.iteritems():
            self.technical_indicator.update(value, d)
            recs.append(self.technical_indicator.get_recommendation(self.with_details))
            signals.append([k.get_metric() for k in self.technical_indicator.technical_signals])
            n+=1
            if n%1000 == 0:
                print(n)
        self.last_run_date = orig_price_data.index[-1]
        signal_names = [k.get_name() for k in self.technical_indicator.technical_signals]
        return (pd.DataFrame(recs, index=orig_price_data.index), pd.DataFrame(signals, index=orig_price_data.index, columns=signal_names))

    def get_calibration_signal_block(self, begin_dt, until_dt):
        price_data = self.price_series[(self.price_series.index > begin_dt)
                                       & (self.price_series.index <= until_dt)]
        signals = []
        orig_price_data = price_data.copy()
        price_data.index = price_data.index.asi8.astype(long)
        for d,value in price_data.iteritems():
            signals.append([k.get_metric() for k in self.technical_indicator.technical_signals])
        signal_names = [k.get_name() for k in self.technical_indicator.technical_signals]
        return pd.DataFrame(signals, index=orig_price_data.index, columns=signal_names)

    def run_against_historical_data_for_specified_period(self, begin_dt, until_dt):
        price_data = self.price_series[(self.price_series.index > begin_dt)
                                       & (self.price_series.index <= until_dt)]
        self.previous_run_date = self.last_run_date
        recs = []
        signals = []
        n = 0
        orig_price_data = price_data.copy()
        price_data.index = price_data.index.asi8.astype(long)
        for d,value in price_data.iteritems():
            self.technical_indicator.update(value, d)
            recs.append(self.technical_indicator.get_recommendation(self.with_details))
            signals.append([k.get_metric() for k in self.technical_indicator.technical_signals])
            n+=1
            if n%1000 == 0:
                print(n)
        self.last_run_date = orig_price_data.index[-1]
        signal_names = [k.get_name() for k in self.technical_indicator.technical_signals]

        recs_df = pd.DataFrame(recs, index=orig_price_data.index)
        signals_df = pd.DataFrame(signals, index=orig_price_data.index, columns=signal_names)
        return (recs_df.ix[self.previous_run_date:], signals_df)

    def set_voting_scheme_for_simulation(self, voting_scheme):
        #TODO(Jack): fix later
        #self.technical_indicator.set_voting_scheme_from_specification(scheme_name, scheme_parameters)
        self.technical_indicator.voting_scheme = voting_scheme

    def reset(self):
        self.technical_indicator.reset()
        self.last_run_date = self.price_series.index[0] - pd.tseries.offsets.Minute()

    def __iter__(self):
        #return iter(self.technical_indicator)
        return

def read_technical_indicators_specification(file_name):
    #TODO: link this into the config file
    path_to_indicators_file = file_name
    with open(path_to_indicators_file) as f:
        indicators_specification = json.load(f)
    return indicators_specification


def build_technical_indicator(stock, indicator_name, indicator_params, stock_history_for_cache=None):
    cython_indicator = getattr(onc, indicator_name)(stock, **indicator_params)
    indicator = cython_indicator.get_cached_indicator(stock_history_for_cache) if stock_history_for_cache is not None \
        else cython_indicator
    return indicator

def build_digitizer(digitizer_name, digitizer_params):
    digitizer = getattr(onc, digitizer_name)("test", **digitizer_params)
    return digitizer

def build_technical_signal(stock, indicator_name, indicator_params, digitizer_name, digitizer_params,
                           stock_history_for_cache=None):
    indicator = build_technical_indicator(stock, indicator_name, indicator_params, stock_history_for_cache)
    digitizer = build_digitizer(digitizer_name, digitizer_params)
    trad_signal = onc.SimpleTechnicalTradingSignal(indicator, digitizer)
    return trad_signal

def build_technical_signal_cohort(stock, technical_specs, stock_history_for_cache=None):
    signals = []
    all_indicators = []
    for technical_spec in technical_specs:
        signal = build_technical_signal(stock, *technical_spec, stock_history_for_cache=stock_history_for_cache)
        all_indicators.append(pd.Series(signal.indicator.get_cached_values(), name = signal.get_name(), index=stock_history_for_cache.index))
        signals.append(signal)
    names = [signal.get_name() for signal in signals]
    # The values are just for initialization
    voting_scheme = onc.CythonWeightedAverageVotingScheme(names, np.ones(len(names)), 0.0, 1.0, 0.5, 1)
    cohort = onc.TechnicalTradingSignalCohort(stock, "WeightedAverage", voting_scheme, signals, False)
    return cohort


def acquire_data_from_redshift(ticker, start_date, end_date):
    print("About to acquire price data for ticker {0} from {1} to {2}".format(ticker, start_date, end_date))
    engine = create_engine(get_config().get_string('DataProvider.HistoricalEquityDataProvider.redshift_uri'))
    query = "SELECT cob, close FROM {0} where ticker = '{1}' and as_of_end is null and "\
        "cob >= '{2}' and cob <= '{3}' order by cob".format(EQUITY_TABLE_NAME, ticker,
                                                            start_date.date(), end_date.date())
    equity_data = pd.read_sql(query, engine, index_col="cob")
    price_data = equity_data.iloc[:,0]
    price_data = np.around(price_data, 5)
    returns_data = pd.np.log(price_data/price_data.shift(1)).fillna(value=0.0)
    returns_data = np.around(returns_data, 5)
    print("Acquired price data of ticker {0} from {1} to {2}".format(ticker, start_date, end_date))
    #return (price_data, returns_data)
    return (price_data, returns_data)

def read_calibration_simulation_dates(file_name):
    path_to_date_file = file_name
    with open(path_to_date_file) as f:
        date_specs = json.load(f)
    return date_specs


def read_ticker_universe():
    #TODO: link this into the config file
    path_to_tickers_file = r"C:\DCM\vistools\Tools\ticker_list.json"
    with open(path_to_tickers_file) as f:
        ticker_list = json.load(f)
    return ticker_list


def build_simulator(ticker, tech_indicators_specification, price_series):
    print("About to create technical indicator cohort for calibration of ticker {0}".format(ticker))
    cohort_name = "calibration_{0}".format(ticker)
    indicator = build_technical_signal_cohort(ticker, tech_indicators_specification, stock_history_for_cache=price_series)
    print("Created technical indicator cohort for ticker {0}".format(ticker))
    simulator = TechnicalTradingSignalHistoricalSimulator(indicator, price_series, True)
    return simulator

def build_technical_indicator_optimizer(technical_indicator, util_func, returns_data, raw_signals):
    #ind_sigma = raw_signals.std()
    #ind_mean = raw_signals.mean()
    assert np.allclose(returns_data.index.astype(long), raw_signals.index.astype(long))
    ind_min = raw_signals.min()
    ind_max = raw_signals.max()
    ind_range = ind_max - ind_min
    ind_lb = ind_min - 0.005*ind_range
    ind_ub = ind_max + 0.005*ind_range
    # (Jack)
    ind_mid = 0.5*(ind_lb + ind_ub)
    lower_bounds = np.array([0.0, ind_lb, ind_lb])
    upper_bounds = np.array([1.0, ind_ub, ind_ub])
    optim_func = onc.SimpleTechnicalTradingSignalOptimizationFunction(technical_indicator.digitizer,
                                                                      returns_data.values,
                                                                      raw_signals.values,
                                                                      util_func)
    positions_to_include = np.asarray([[0.5, ind_lb, ind_ub], optim_func.get_parameter_values_to_optimize().tolist()])

    pso = onc.ParticleSwarmOptimization(600, 10, 3, np.array([0.2, 0.5, 0.1, 0.05, 0.1, 1.0]), lower_bounds, upper_bounds,
                                        optim_func, True, "max", 1e-5, 50, positions_to_include)
    return pso

def build_technical_cohort_optimizer(technical_cohort, util_func, returns_data, recommendations):
    assert np.allclose(returns_data.index.astype(long), recommendations.index.astype(long))

    optim_func = onc.VotingSchemeOptimizationFunction(technical_cohort.voting_scheme, returns_data.values,
                                                      recommendations.to_dict(orient="records"),
                                                      util_func)

    param_names = optim_func.get_names_of_parameter_values_to_optimize()
    num_voting_params = len(param_names) - 3
    special_position_and_value_list = []

    voting_threshold_limits = [-num_voting_params*1.005, num_voting_params*1.005]
    special_position_and_value_list.append([param_names.index("exit_position"), 0.5, [0.0, 1.0]])
    special_position_and_value_list.append([param_names.index("lower_threshold"), -0.99, voting_threshold_limits])
    special_position_and_value_list.append([param_names.index("upper_threshold"), 0.99, voting_threshold_limits])
    special_position_and_value_list = sorted(special_position_and_value_list, key=lambda x:x[0])

    positions_to_include = np.eye(num_voting_params)
    positions_to_include = np.vstack((positions_to_include, np.zeros(positions_to_include.shape[1])))
    for column, value, _ in special_position_and_value_list:
        positions_to_include = np.insert(positions_to_include, column, value, axis=1)

    lower_bounds = np.array([0.0]*len(param_names))
    upper_bounds = np.array([1.0]*len(param_names))
    for column, _, limits in special_position_and_value_list:
        lower_bounds[column] = limits[0]
        upper_bounds[column] = limits[1]

    pso = onc.ParticleSwarmOptimization(2000, 15, len(param_names), np.array([0.2, 0.5, 0.1, 0.05, 0.1, 1.0]), lower_bounds, upper_bounds,
                                        optim_func, True, "max", 1e-5, 50, positions_to_include)
    return pso

def run_calibration_and_get_performance(simulator, calibration_dates, price_data, returns_data, fit_func, lookback=None):
    all_recommendations = None
    all_raw_signals = None
    calibration_dates = sorted(calibration_dates)
    previous_dt = price_data.index[0] - pd.tseries.offsets.Minute()
    voting_calibrations = pd.Series(index=calibration_dates)
    individual_params = pd.DataFrame(index=calibration_dates, columns=simulator.technical_indicator.technical_signals)
    calibrated_positions_raw = pd.Series(index=calibration_dates)
    calibrated_parameter_names = pd.Series(index=calibration_dates)
    parallel = False
    for d in calibration_dates:
        last_date = d - pd.tseries.frequencies.to_offset(lookback) if lookback else previous_dt
        (recommendation_block, raw_signals_block) = simulator.run_against_historical_data(d)
        all_recommendations = pd.concat([all_recommendations, recommendation_block])
        all_raw_signals = pd.concat([all_raw_signals, raw_signals_block])

        # Define optimizer here
        # Modifies object
        cut_returns_data = returns_data.ix[last_date:d]
        new_optimal_signals = []
        for signal in simulator.technical_indicator.technical_signals:
            name = signal.get_name()
            raw_signals = all_raw_signals.loc[last_date:d, name]
            optimizer = build_technical_indicator_optimizer(signal,
                                                            fit_func,
                                                            cut_returns_data,
                                                            raw_signals)
            best_position, best_fit = optimizer.optimize()
            ts_optimal_recommendation = pd.Series(optimizer.get_optimization_function().get_optimal_digitized_signals(),
                                                  index=raw_signals.index, name=name)
            new_optimal_signals.append(ts_optimal_recommendation)
            individual_params.loc[d, name] = str(tuple(optimizer.get_optimization_function().get_parameter_values_to_optimize()))
            print(name)

        new_optimal_signals = pd.concat(new_optimal_signals, axis=1)

        # use voting scheme util
        names = [k.get_name() for k in simulator.technical_indicator.technical_signals]
        optimizer = build_technical_cohort_optimizer(simulator.technical_indicator,
                                                     fit_func,
                                                     cut_returns_data,
                                                     new_optimal_signals)
        best_position, best_fit = optimizer.optimize()

        calibrated_positions_raw[d] = str(list(best_position))
        calibrated_parameter_names[d] = str(optimizer.get_optimization_function().get_names_of_parameter_values_to_optimize())
        voting_calibrations[d] = str(dict(zip(optimizer.get_optimization_function().get_names_of_parameter_values_to_optimize(),
                                          optimizer.get_optimization_function().get_parameter_values_to_optimize())))
        previous_dt = last_date

    recommendation_block = simulator.run_against_historical_data(price_data.index[-1])
    all_recommendations = pd.concat([all_recommendations, recommendation_block])
    simulated_holding_signal = all_recommendations[simulator.technical_indicator.get_name()]
    total_return = calculate_return_from_digitized_decisons(simulated_holding_signal, returns_data, fee)
    return (voting_calibrations, individual_params, total_return, all_recommendations, calibrated_parameter_names, calibrated_positions_raw)


def calibrate_ticker(ticker, tech_indicators_specification, fit_func, start_date, end_date, calibration_dates, filter_good=False,
                     lookback=None):
    price_data, returns_data = acquire_data_from_redshift(ticker, start_date, end_date)
    simulator = build_simulator(ticker, tech_indicators_specification, price_data)
    #optimizer = build_technical_indicator_optimizer(simulator.technical_indicator, fit_func)
    results = run_calibration_and_get_performance(simulator, calibration_dates, price_data, returns_data, fit_func, lookback)
    return results

def import_global_modules():
    global np
    global pd
    global partial
    global opt
    global partial
    global dill
    global parallel_brute

    import pandas as pd
    import scipy.optimize as opt
    import numpy as np
    from functools import partial
    import dill



def tech_trading_calibrator(ticker, config):
    import_global_modules()

    cohort_json = config["cohort_configuration"]
    tech_indicators_specification = read_technical_indicators_specification(cohort_json)
    suffix = cohort_json.split('.json')[0]

    date_json = config["date_file"]
    date_specs = read_calibration_simulation_dates(date_json)
    start_date = pd.Timestamp(date_specs["start_date"])
    end_date = pd.Timestamp(date_specs["end_date"])
    start_date_str = str(start_date).split(' ')[0]
    end_date_str = str(end_date).split(' ')[0]
    lookback = config["lookback"]

    #fit_func = eval(config["fit_func"])
    fit_func_str = "onc." + config["fit_func"] + "(" + str(config["pnl_fee"]) + ")"
    fit_func = eval(fit_func_str)
    date_list = date_specs["date_list"]
    calibration_dates = map(pd.Timestamp, date_list)

    try:
        results_dir = config["output_location"]
        results_file = results_dir + ticker + '_' + end_date_str + '_' + str(int(get_config()["TechnicalTradingCalibration"]["fee_amount"]*10000))\
            + '_' + suffix + '.h5'
        calibration_results = calibrate_ticker(ticker, tech_indicators_specification, fit_func, start_date,
                                               end_date, calibration_dates, lookback=lookback)

        calibrations = calibration_results[0]
        rets = calibration_results[1]
        recs = calibration_results[2]
        calibrated_indicators = calibration_results[3]
        calibrated_positions_raw = calibration_results[4]

        store = pd.HDFStore(results_file)
        store['rets'] = rets
        store['recs'] = recs
        store['indicators'] = calibrated_indicators
        store['weights'] = calibrated_positions_raw
        store.close()
    except Exception as e:
        error_filename = results_dir + ticker + '_' + end_date_str + '_' + str(int(get_config()["TechnicalTradingCalibration"]["fee_amount"]*10000))\
            + '_FAILURE' + '.txt'
        with file(error_filename, "w") as error_file:
            print("Calibration failure for {0}: {1}".format(ticker, str(e)), file=error_file)

def tech_trading_calibrator_remote(ticker, config):
    import_global_modules()

    cohort_json = config["cohort_configuration"]
    tech_indicators_specification = read_technical_indicators_specification(cohort_json)
    suffix = cohort_json.split('.json')[0]

    date_json = config["date_file"]
    date_specs = read_calibration_simulation_dates(date_json)
    start_date = pd.Timestamp(date_specs["start_date"])
    end_date = pd.Timestamp(date_specs["end_date"])
    start_date_str = str(start_date).split(' ')[0]
    end_date_str = str(end_date).split(' ')[0]
    lookback = config["lookback"]

    filter_good = config["filter_good"]
    if filter_good:
        suffix = suffix + '_filtered'
    suffix = suffix + '_' + config["additional_suffix"]

    fit_func = eval(config["fit_func"])
    date_list = date_specs["date_list"]
    calibration_dates = map(pd.Timestamp, date_list)

    s3 = boto3.client('s3')
    bucket = config["base_data_bucket"]
    key_prefix = config["key_prefix"]

    try:
        results_dir = config["output_location"]
        #results_file = results_dir + ticker + '_' + end_date_str + '_' + suffix + '.h5'
        filename = ticker + '_' + end_date_str + '_' + suffix + '.h5'
        results_file = os.path.join(tempfile.gettempdir(), filename)
        calibration_results = calibrate_ticker(ticker, tech_indicators_specification, fit_func, start_date,
                                               end_date, calibration_dates, filter_good=filter_good, lookback=lookback)

        calibrations = calibration_results[0]
        rets = calibration_results[1]
        recs = calibration_results[2]
        calibrated_indicators = calibration_results[3]
        calibrated_positions_raw = calibration_results[4]

        store = pd.HDFStore(results_file)
        store['rets'] = rets
        store['recs'] = recs
        store['indicators'] = calibrated_indicators
        store['weights'] = calibrated_positions_raw
        store.close()

        key_aws = key_prefix + "/{0}".format(filename)
        s3.upload_file(results_file, bucket, key_aws)
        os.remove(results_file)
        return 1

    except Exception as e:
        filename = ticker + '_' + end_date_str + '_' + 'ERROR' + '.txt'
        error_file = os.path.join(tempfile.gettempdir(), filename)
        text_file = open(error_file, "w")
        text_file.write("Calibration failure for {0}: {1}".format(ticker, str(e)))
        text_file.close()
        key_aws = key_prefix + "/{0}".format(filename)
        s3.upload_file(error_file, bucket, key_aws)
        os.remove(error_file)
        return 0

if __name__ == "__main__":

    config = load_python_conf(sys.argv[2])
    #tech_trading_calibrator(sys.argv[1], config)

    #TODO: So far we havce avoided to modify Intuition config on at Runtime.... FIGURE THIS OUT LATER!!!!!
    get_config()["TechnicalTradingCalibration"] = {"fee_amount": config["pnl_fee"]}

    print(os.environ)
    print(sys.path)

    tech_trading_calibrator(sys.argv[1], config)


