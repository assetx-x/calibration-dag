from collections import deque

#import online_indicators_cython_copy as onc
import offline

import numpy as np
import inspect
from functools import partial
import json
import pandas as pd



def prepare_dataframe_for_performance_analysis(df, holdings_columns, price_column = None,
                                               returns_columns = None, date_column= None, date_convention ="end",
                                               holdings_convention = "start", pricing_convention = "end"):
    if price_column and returns_columns:
        raise ValueError("Only on of price_column or returns_column should be provided")
    if holdings_convention not in ["start", "s", "e", "end"]:
        raise
    if holdings_convention not in ["start", "s", "e", "end"]:
        raise

    data = df.copy()
    if date_column:
        data.set_index(date_column, inplace = True)
    data.index = pd.DatetimeIndex(data.index)
    data = data[[price_column or returns_columns, holdings_columns]]
    data.columns = ["returns", "holdings"]
    data.sort_index(inplace=True)
    if price_column:
        data["returns"] = data["returns"].pct_change()
    holding_shift = 0 if holdings_convention[0]==date_convention[0] else (1 if date_convention[0]=="e" else -1)
    pricing_shift = 0 if pricing_convention[0]==date_convention[0] else (1 if date_convention[0]=="e" else -1)
    data["returns"] = data["returns"].shift(pricing_shift)
    data["holdings"] = data["holdings"].shift(holding_shift)
    return data




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

def process_data(n, data):
    aapl_data = data[0:n]
    #indicator = TechnicalTradingSignal("AAPL", "OnlineRSI", {"window": 390}, "generic_technical_digitization",
    #                                   {"exit_position": 0.5, "lower_threshold": 47.75342588727296, "orientation": -1,
    #                                    "upper_threshold": 53.300628732758234}, True, aapl_data )
    #cohort = TechnicalTradingSignalCohort("AAPL", "aapl_test")
    #voting_scheme = DummyTechnicalTradingSignalVotingSchemeForCalibration()
    #cohort.set_voting_scheme(voting_scheme)
    #cohort.add_technical_indicator(indicator)

    #cohort = build_technical_indicator("AAPL", "OnlineRSI", {"window": 390}, aapl_data)
    signals = []
    technical_specs = read_technical_indicators_specification("C:/DCM/dcm-intuition/processes/scanning/technical_trading/indicators_full_new_ali.json")
    all_indicators = []
    for technical_spec in technical_specs:
        signal = build_technical_signal("AAPL", *technical_spec, stock_history_for_cache=aapl_data)
        all_indicators.append(pd.Series(signal.indicator.get_cached_values(), name = signal.get_name(), index=aapl_data.index))
        signals.append(signal)

    #lenghts = range(1,23)
    #signals = [build_technical_signal("AAPL", "OnlineRSI", {"window": k}, "GenericTechnicalDigitizer",
    #                                {"exit_position": 0.5, "lower_threshold": 47.75342588727296, "orientation": -1,
    #                                 "upper_threshold": 53.300628732758234}, aapl_data) for k in lenghts]
    names = [signal.get_name() for signal in signals]

    voting_scheme = onc.CythonWeightedAverageVotingScheme(names, np.arange(len(names)) * 1.0
                                                          ,0.2,0.8,0.4, -1)

    cohort = onc.TechnicalTradingSignalCohort("AAPL", "test", voting_scheme, signals, False)

    indicator_values = []
    signal_values = []
    for dt,v in aapl_data.iteritems():
        cohort.update(v, dt)
        #indicator_values.append(cohort.get_recommendation(True))
        signal_values.append([k.get_metric() for k in cohort.technical_signals])
    #data = pd.DataFrame(indicator_values, index=aapl_data.index)

    signal_names = [k.get_name() for k in cohort.technical_signals]
    signals_df = pd.DataFrame(signal_values, index=aapl_data.index, columns=signal_names)

    digitizer, signal = (cohort.technical_signals[2].digitizer, signals_df.iloc[:,2])

    util_perf_func = onc.TechnicalStrategyPerformance(0.002)

    signal_no = 21
    optim_func = onc.SimpleTechnicalTradingSignalOptimizationFunction(cohort.technical_signals[signal_no].digitizer,
                                                                      aapl_data.pct_change().values,
                                                                      signals_df.iloc[:,signal_no].values,
                                                                      util_perf_func)

    print("Pre-optimization value: {0}; parameters: {1}".format(optim_func.evaluate(), optim_func.get_parameter_values_to_optimize()))

    indicator_min = signals_df.iloc[:,signal_no].min()*0.99
    indicator_max = signals_df.iloc[:,signal_no].max()*1.01

    bounds = (np.array([0.0, indicator_min, indicator_min]), np.array([1.0, indicator_max, indicator_max]))
    print("bounds={0}".format(bounds))
    pso = onc.ParticleSwarmOptimization(600, 10, 3, np.array([0.2, 0.5, 0.1, 0.05, 0.1, 1.0]), bounds[0], bounds[1],
                                        optim_func, True, "max", 1e-5, 50,
                                        np.asarray([[0.5, indicator_min, indicator_max]]))
    #import copy
    #import cPickle as pickle
    #pickle.loads(pickle.dumps(util_perf_func))
    #copy.deepcopy(util_perf_func)
    #copy.deepcopy(cohort.technical_signals[0].digitizer)
    best_position, best_fit = pso.optimize()

    print("Post-optimization value: {0}; parameters: {1}".format(optim_func.evaluate(), optim_func.get_parameter_values_to_optimize()))

    #particle = onc.Particle(util_func, np.asarray([0.0]*3), np.asarray([1000.0]*3))


    #util_func.evaluate()
    #util_func.set_optimized_parameter_values(np.asarray([10.0, 80.0, 0.5]))
    #util_func.evaluate()


    #all_inds = pd.concat(all_indicators, axis=1)



def load_data():
    import pandas as pd
    with pd.HDFStore(r"C:\DCM\temp\cython\aapl.h5", "r") as s:
        aapl_data = s["data"]
    aapl_data.index = aapl_data.index.asi8.astype(long)
    return aapl_data

def rastrigin_test():
    func = onc.testParticleOptimizationFunction()
    bounds = (np.array([0.0]*4), np.array([5.0]*4))

    pso = onc.ParticleSwarmOptimization(200, 600, 4, np.array([0.2, 0.5, 0.1, 0.05, 0.1, 1.0]), bounds[0], bounds[1],
                                        func, True, "min", 1e-5, 50)
    best_position, best_fit = pso.optimize()
    return best_position

def hit_ratio(input_array):
    number_of_pos = len(input_array[input_array>0])
    number_of_neg = len(input_array[input_array<=0])
    return np.float(number_of_pos)/(number_of_pos+number_of_neg)


if __name__=="__main__":
    #rastrigin_test()
    filter_to_calculate_measure = [1.0]
    measure_1 = onc.GenericPerformanceIndicatorPerSegments(0.0, onc.TechnicalStrategyPerformance(0.0), np.mean,
                                                           filter_to_calculate_measure)
    measure_2 = onc.TechnicalStrategyHitRatio(0.0)
    measure_3 = onc.TechnicalStrategyPerformance(0.00)
    measure_4 = onc.GenericPerformanceIndicatorPerSegments(0.0, onc.TechnicalStrategyHitRatio(0.0), np.mean,
                                                           filter_to_calculate_measure)
    measure_5 = onc.GenericPerformanceIndicatorPerSegments(0.0, onc.TechnicalStrategyPerformance(0.0), np.max,
                                                           filter_to_calculate_measure)
    measure_6 = onc.GenericPerformanceIndicatorPerSegments(0.0, onc.TechnicalStrategyPerformance(0.0), hit_ratio,
                                                           filter_to_calculate_measure)

    measure_dict = {"mean_segment_perf": measure_1, "hit_ratio": measure_2, "perf": measure_3,
                    "hit_ratio_per_segment": measure_4, "max_segment_perf": measure_5, "actual_hit_ratio": measure_6}
    #measure_dict = {"mean_segment_perf": measure_1}
    perf = onc.PerformanceSummary(measure_dict)
    #holdings = np.asarray([1]*5 + [-1]*5 + [1,-1]*10 + [1]*5 + [-1]*5 ).astype(float)
    #holdings = np.asanyarray([0.0]+[1.0]*3 + [0.0]*3 + [1.0]*2)
    holdings_input = np.asanyarray([1.0, 1.0,1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0])
    #returns = np.asarray([0.01] * 20 + [-0.01] * 20)
    #returns = np.asarray([-0.01] + [0.01] * 5+ [0.02] + [-0.01,-0.02])
    returns_input = np.asanyarray([0.01, -0.01, 0.02, -0.01, 0.01, 0.02, -0.01, 0.02, 0.03, 0.01, -0.01, 0.02, 0.01,
                              -0.02, 0.01, 0.02, 0.03, -0.01])

    lst=[list(holdings_input),list(returns_input)]
    df = pd.DataFrame([list(x) for x in zip(*lst)], columns=['holdings', 'return'], index= pd.bdate_range(start="2017-01-03",
                                                                                                         periods=18, freq="B"))
    res = prepare_dataframe_for_performance_analysis(df,"holdings",returns_columns= "return").dropna()

    holdings = np.asanyarray(res['holdings'].values)
    returns = np.asanyarray(res['returns'].values)


    values = perf.calculate_measure(holdings, returns, True)


    mm = load_data()
    process_data(100000, mm)
    print("Done")
