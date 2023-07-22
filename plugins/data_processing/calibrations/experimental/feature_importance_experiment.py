import gc
import pandas as pd
import os

from commonlib.util_functions import create_directory_if_does_not_exists
from etl_workflow_steps import StatusType, BaseETLStage, compose_main_flow_and_engine_for_task
from calibrations.common.config_functions import get_config

from calibrations.common.calibration_logging import calibration_logger

from calibrations.common.config_functions import get_config
from commonlib.market_timeline import marketTimeline

#from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask

#from calibrations.feature_builders.feature_builder import CalculateTaLibPPO, CalculateTaLibADOSC, CalculateTaLibADX, \
     #CalculateTaLibAROONOSC, CalculateTaLibCCI, CalculateTaLibCMO, CalculateTaLibMFI, CalculateTaLibNATR, \
     #ComputeIdioVol, CalculateVolatility, CalculateDollarVolume, CalculateOvernightReturn, CalculateTaLibRSI, \
     #CalculateTaLibSTOCH, CalculateTaLibSTOCHF, CalculateTaLibSTOCHRSI, CalculateTaLibTRIX, CalculateTaLibWILLR, \
     #CalculateTaLibULTOSC

#class CalculateTaLibPPOShort(CalculateTaLibPPO):
    #PROVIDES_FIELDS = ["talib_ppo_short_indicator_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

#class CalculateTaLibPPOLong(CalculateTaLibPPO):
    #PROVIDES_FIELDS = ["talib_ppo_long_indicator_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

#class CalculateTaLibADXShort(CalculateTaLibADX):
    #PROVIDES_FIELDS = ["talib_adx_short_indicator_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

#class CalculateTaLibADXLong(CalculateTaLibADX):
    #PROVIDES_FIELDS = ["talib_adx_long_indicator_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

#class CalculateTaLibWILLRShort(CalculateTaLibWILLR):
    #PROVIDES_FIELDS = ["talib_willr_short_indicator_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

#class CalculateTaLibWILLRLong(CalculateTaLibWILLR):
    #PROVIDES_FIELDS = ["talib_willr_long_indicator_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

#class CalculateTaLibAROONOSCShort(CalculateTaLibAROONOSC):
    #PROVIDES_FIELDS = ["talib_aroonosc_short_indicator_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

#class CalculateTaLibAROONOSCLong(CalculateTaLibAROONOSC):
    #PROVIDES_FIELDS = ["talib_aroonosc_long_indicator_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

#class CalculateVolatilityShort(CalculateVolatility):
    #PROVIDES_FIELDS = ["volatility_short_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]
    
#class CalculateVolatilityLong(CalculateVolatility):
    #PROVIDES_FIELDS = ["volatility_long_data"]
    #REQUIRES_FIELDS = ["daily_price_data"]

def pick_day_of_the_week(start_date, end_date, day):
    all_bdays = pd.bdate_range(start_date, end_date)
    ts = pd.Series(all_bdays)
    is_day = ts.apply(lambda x:x.dayofweek==day)
    selected_days = list(ts[is_day])
    final_days = map(marketTimeline.get_next_trading_day_if_holiday, selected_days)
    return final_days

def indicator_data_merge(data_dir):
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_1.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))

    correlation_data = correlation_data.drop(["SPY_correl_rank"], axis=1)
    correlation_data["date"] = pd.DatetimeIndex(correlation_data["date"]).normalize()
    dollar_volume_data = dollar_volume_data.drop(["index"], axis=1)
    dollar_volume_data["log_avg_dollar_volume"] = pd.np.log(dollar_volume_data["avg_dollar_volume"])
    past_return_data = past_return_data.drop(["index"], axis=1)
    past_return_data["date"] = pd.DatetimeIndex(past_return_data["date"]).normalize()
    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)
    overnight_return_data["date"] = pd.DatetimeIndex(overnight_return_data["date"]).normalize()
    overnight_return_data = overnight_return_data.drop(["index"], axis=1)
    macd_indicator_data["date"] = pd.DatetimeIndex(macd_indicator_data["date"]).normalize()
    
    
    merged_data = correlation_data.copy(deep=True)
    merged_data = pd.merge(merged_data, beta_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, dollar_volume_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, idiovol_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, macd_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, overnight_return_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, past_return_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stochf_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stochrsi_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stoch_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_trix_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ultosc_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_2.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))
    
    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)    
    
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])   
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_3.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))
    
    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)    
    
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])
    
    #target_returns = past_return_data.copy(deep=True)
    #target_returns["date"] = target_returns["date"].shift(1)
    #target_returns.columns = map(lambda x:x.replace("ret", "future_ret"), target_returns.columns)
    
    #merged_data = pd.merge(merged_data, target_returns, how="left", on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_4.h5"), "r") as store:
        future_return_data = store["/pandas/future_return_data"]
    
    #future_return_data = future_return_data.drop(["index"], axis=1)
    future_return_data["date"] = pd.DatetimeIndex(future_return_data["date"]).normalize()    
    merged_data = pd.merge(merged_data, future_return_data, how="left", on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "ConsolidatedData.h5"), "w") as store:
        store["merged_data"] = merged_data
        store["daily_price_data"] = daily_price_data
        store["etf_information"] = etf_information
        store["sector_industry_mapping"] = sector_industry_mapping
    
    print("done")

def indicator_data_merge_2(data_dir):
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_1.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))


    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)
    overnight_return_data["date"] = pd.DatetimeIndex(overnight_return_data["date"]).normalize()
    overnight_return_data = overnight_return_data.drop(["index"], axis=1)
    macd_indicator_data["date"] = pd.DatetimeIndex(macd_indicator_data["date"]).normalize()
    
    merged_data = beta_data.copy(deep=True)
    merged_data = pd.merge(merged_data, idiovol_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, macd_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, overnight_return_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stochf_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stochrsi_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stoch_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_trix_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ultosc_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_2.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))
    
    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)    
    
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])   
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_3.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))
    
    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)    
    
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_4.h5"), "r") as store:
        correlation_data = store["/pandas/correlation_data"]
        dollar_volume_data = store["/pandas/dollar_volume_data"]
        past_return_data = store["/pandas/past_return_data"]
        future_return_data = store["/pandas/future_return_data"]
    
    correlation_data = correlation_data.drop(["SPY_correl_rank"], axis=1)
    correlation_data = correlation_data.reset_index()
    correlation_data["date"] = pd.DatetimeIndex(correlation_data["date"]).normalize()
    dollar_volume_data["log_avg_dollar_volume"] = pd.np.log(dollar_volume_data["avg_dollar_volume"])
    past_return_data["date"] = pd.DatetimeIndex(past_return_data["date"]).normalize()        
    future_return_data["date"] = pd.DatetimeIndex(future_return_data["date"]).normalize()
    
    merged_data = pd.merge(merged_data, correlation_data, how="left", on=["date", "ticker"])
    merged_data = pd.merge(merged_data, dollar_volume_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, past_return_data, how="left", on=["date", "ticker"])
    merged_data = pd.merge(merged_data, future_return_data, how="left", on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "ConsolidatedData.h5"), "w") as store:
        store["merged_data"] = merged_data
        store["daily_price_data"] = daily_price_data
        store["etf_information"] = etf_information
        store["sector_industry_mapping"] = sector_industry_mapping
    
    print("done")
    
def indicator_data_merge_3(data_dir):
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_1.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))

    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)
    overnight_return_data["date"] = pd.DatetimeIndex(overnight_return_data["date"]).normalize()
    overnight_return_data = overnight_return_data.drop(["index"], axis=1)
    macd_indicator_data["date"] = pd.DatetimeIndex(macd_indicator_data["date"]).normalize()
    correlation_data = correlation_data.drop(["8554_correl_rank"], axis=1)
    correlation_data = correlation_data.reset_index()
    correlation_data["date"] = pd.DatetimeIndex(correlation_data["date"]).normalize()    
    
    merged_data = beta_data.copy(deep=True)
    merged_data = pd.merge(merged_data, macd_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, overnight_return_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stochf_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stochrsi_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_stoch_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_trix_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ultosc_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, correlation_data, how='left', on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_2.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))
    
    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)    
    
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])   
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_3.h5"), "r") as store:
        for key in store.keys():
            current_key = key.replace("/pandas/", "")
            print(key.replace("/pandas/", ""))
            exec("{0} = store['{1}']".format(current_key, key))
            exec("{0} = {0}.reset_index()".format(current_key))
    
    volatility_data["date"] = pd.DatetimeIndex(volatility_data["date"]).normalize()
    volatility_data = volatility_data.drop(["index"], axis=1)    
    
    merged_data = pd.merge(merged_data, talib_adx_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_ppo_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, talib_willr_indicator_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, volatility_data, how='left', on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_4.h5"), "r") as store:
        #correlation_data = store["/pandas/correlation_data"]
        dollar_volume_data = store["/pandas/dollar_volume_data"]
        past_return_data = store["/pandas/past_return_data"]
        future_return_data = store["/pandas/future_return_data"]
    
    #correlation_data = correlation_data.drop(["SPY_correl_rank"], axis=1)
    #correlation_data = correlation_data.reset_index()
    #correlation_data["date"] = pd.DatetimeIndex(correlation_data["date"]).normalize()
    dollar_volume_data["log_avg_dollar_volume"] = pd.np.log(dollar_volume_data["avg_dollar_volume"])
    past_return_data["date"] = pd.DatetimeIndex(past_return_data["date"]).normalize()        
    future_return_data["date"] = pd.DatetimeIndex(future_return_data["date"]).normalize()
    
    #merged_data = pd.merge(merged_data, correlation_data, how="left", on=["date", "ticker"])
    merged_data = pd.merge(merged_data, dollar_volume_data, how='left', on=["date", "ticker"])
    merged_data = pd.merge(merged_data, past_return_data, how="left", on=["date", "ticker"])
    merged_data = pd.merge(merged_data, future_return_data, how="left", on=["date", "ticker"])
    
    merged_data["volatility_63"] = pd.np.log(merged_data["volatility_63"])
    merged_data["volatility_21"] = pd.np.log(merged_data["volatility_21"])
    merged_data["volatility_126"] = pd.np.log(merged_data["volatility_126"])
    with pd.HDFStore(os.path.join(data_dir, "ConsolidatedData.h5"), "w") as store:
        store["merged_data"] = merged_data
        store["daily_price_data"] = daily_price_data

    
    print("done")

def filter_dates(data_dir, start_date, end_date, day):
    with pd.HDFStore(os.path.join(data_dir, "ConsolidatedData.h5"), "r") as store:
        merged_data = store["merged_data"]
    
    chosen_days = pick_day_of_the_week(start_date, end_date, day)
    filtered_data = merged_data[merged_data["date"].isin(chosen_days)].reset_index(drop=True)
    
    with pd.HDFStore(os.path.join(data_dir, "ConsolidatedDataWed.h5"), "w") as store:
        store["merged_data"] = filtered_data
    
    print("done")

def pick_bme_day(start_date, end_date):
    trading_days = marketTimeline.get_trading_days(start_date, end_date)
    dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).last()
    return dates

def filter_dates_monthly(data_dir, start_date, end_date):
    with pd.HDFStore(os.path.join(data_dir, "ConsolidatedData.h5"), "r") as store:
        merged_data = store["merged_data"]
    
    chosen_days = pick_bme_day(start_date, end_date)
    filtered_data = merged_data[merged_data["date"].isin(chosen_days)].reset_index(drop=True)
    
    with pd.HDFStore(os.path.join(data_dir, "ConsolidatedDataMonthly.h5"), "w") as store:
        store["merged_data"] = filtered_data
    
    print("done")

def merge_with_fundamentals(data_dir):
    with pd.HDFStore(os.path.join(data_dir, "ConsolidatedDataWed.h5"), "r") as store:
        merged_data = store["merged_data"]
    with pd.HDFStore(os.path.join(data_dir, "fundamental_features.h5"), "r") as store:
        fundamental_features = store["fundamental_features"]    
    
    merged_data = pd.merge(merged_data, fundamental_features, how='left', on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "merged_data_final.h5"), "w") as store:
        store["merged_data"] = merged_data    
    
    print("done")

def merge_with_suv(data_dir):
    with pd.HDFStore(os.path.join(data_dir, "merged_data_final.h5"), "r") as store:
        merged_data = store["merged_data"]
    with pd.HDFStore(os.path.join(data_dir, "suv.h5"), "r") as store:
        suv = store["suv"]
    
    suv["date"] = pd.DatetimeIndex(suv["date"]).normalize()
    merged_data = pd.merge(merged_data, suv, how="left", on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "merged_data_suv.h5"), "w") as store:
        store["merged_data"] = merged_data
    
    print("done")

def nonnull_lb(value):
    value = value if pd.notnull(value) else -999999999999999
    return value

def nonnull_ub(value):
    value = value if pd.notnull(value) else 999999999999999
    return value

def normalize_data(data_dir):
    with pd.HDFStore(os.path.join(data_dir, "merged_data_suv.h5"), "r") as store:
        df = store["merged_data"]
        
    X_cols = ['SUV', 'overnight_return', 'log_dollar_volume', 'log_avg_dollar_volume', 'log_mktcap', 'std_turn',
              'gp2assets', 'ret_1B', 'STOCHRSI_FASTK_14_5_3', 'fcf_yield', 'SPY_beta', 'TRIX_30', 'volatility_21',
              'ret_252B', 'ret_5B', 'ADX_5', 'WILLR_63', 'WILLR_5', 'FASTK_5_3', 'SPY_idiovol', 'ret_2B', 'divyield',
              'ULTOSC_7_14_28', 'WILLR_14', 'volatility_63', 'bm_ia', 'SLOWD_5_3_3', 'revenue_growth', 'bm',
              'SPY_correl', 'ret_126B', 'macd_diff', 'volatility_126', 'delta_inventory', 'WILLR_14_MA_3', 'ret_63B',
              'ret_21B', 'WILLR_63_MA_3', 'ULTOSC_7_14_28_MA_3', 'ADX_5_MA_3', 'WILLR_5_MA_3', 'shareswa_growth',
              'delta_capex', 'ret_10B', 'PPO_12_26']
    y_col = "future_ret_5B"
    
    df = df.set_index(['date', 'ticker'])[[y_col] + X_cols].sort_index()
    
    for col in df.columns:
        print(col)
        lb = df.groupby('date')[col].transform(pd.Series.quantile, 0.005)
        lb = lb.apply(nonnull_lb)
        ub = df.groupby('date')[col].transform(pd.Series.quantile, 0.995)
        ub = ub.apply(nonnull_ub)
        df[col] = df[col].clip(lb, ub)
        df[col] = df.groupby('date')[col].transform(lambda x: (x - x.mean()) / x.std())
        df[col] = df[col].fillna(0)    
    
    with pd.HDFStore(os.path.join(data_dir, "merged_data_suv_normalized.h5"), "w") as store:
        store["df"] = df
    
    
    print("done")
    
def prepare_final_data(data_dir, start_date):
    with pd.HDFStore(os.path.join(data_dir, "merged_data_suv.h5"), "r") as store:
        df = store["merged_data"]    
    remove_cols = ['delta_capex', 'shareswa_growth', 'delta_inventory', 'ADX_5_MA_3', 'ADX_5', 'revenue_growth',
                   'bm', 'ULTOSC_7_14_28_MA_3', 'ULTOSC_7_14_28', 'volatility_126', 'SPY_idiovol', 'SLOWD_5_3_3',
                   'WILLR_5', 'WILLR_14', 'WILLR_63', 'PPO_12_26']
        
    X_cols = ['SUV', 'overnight_return', 'log_dollar_volume', 'log_avg_dollar_volume', 'log_mktcap', 'std_turn',
              'gp2assets', 'ret_1B', 'STOCHRSI_FASTK_14_5_3', 'fcf_yield', 'SPY_beta', 'TRIX_30', 'volatility_21',
              'ret_252B', 'ret_5B', 'ADX_5', 'WILLR_63', 'WILLR_5', 'FASTK_5_3', 'SPY_idiovol', 'ret_2B', 'divyield',
              'ULTOSC_7_14_28', 'WILLR_14', 'volatility_63', 'bm_ia', 'SLOWD_5_3_3', 'revenue_growth', 'bm',
              'SPY_correl', 'ret_126B', 'macd_diff', 'volatility_126', 'delta_inventory', 'WILLR_14_MA_3', 'ret_63B',
              'ret_21B', 'WILLR_63_MA_3', 'ULTOSC_7_14_28_MA_3', 'ADX_5_MA_3', 'WILLR_5_MA_3', 'shareswa_growth',
              'delta_capex', 'ret_10B', 'PPO_12_26']
    y_col = "future_ret_5B"
    X_cols = list(set(X_cols) - set(remove_cols))
    r1000 = list(pd.read_csv(os.path.join(data_dir, "r1000_2.csv"))["Ticker"])
    df = df[(df["ticker"].isin(r1000)) & (df["date"]>=start_date)].reset_index(drop=True)
    
    df = df.set_index(['date', 'ticker'])[[y_col] + X_cols].sort_index()
    
    for col in df.columns:
        print(col)
        lb = df.groupby('date')[col].transform(pd.Series.quantile, 0.005)
        lb = lb.apply(nonnull_lb)
        ub = df.groupby('date')[col].transform(pd.Series.quantile, 0.995)
        ub = ub.apply(nonnull_ub)
        df[col] = df[col].clip(lb, ub)
        df[col] = df.groupby('date')[col].transform(lambda x: (x - x.mean()) / x.std())
        df[col] = df[col].fillna(0)    
    
    with pd.HDFStore(os.path.join(data_dir, "r1000_data_normalized_2.h5"), "w") as store:
        store["df"] = df
    
    print("done")    

def return_cut_dates(dt, dt_range):
    val = 0*(dt<=dt_range[0]) + 1*((dt>dt_range[0]) & (dt<=dt_range[1])) + \
        2*((dt>dt_range[1]) & (dt<=dt_range[2])) + 3*((dt>dt_range[2]) & (dt<=dt_range[3])) + \
        4*((dt>dt_range[3]) & (dt<=dt_range[4])) + 5*(dt>dt_range[4])
    return int(val)

def add_cv_column(data_dir):
    with pd.HDFStore(os.path.join(data_dir, "r1000_data_normalized_2.h5"), "r") as store:
        df = store["df"]
    df = df.reset_index()
    dates = pd.DatetimeIndex(df["date"].unique())
    dates.sort_values()
    dt_range = [dates[89], dates[179], dates[269], dates[359], dates[449]]
    df["fold_id"] = df["date"].apply(lambda x:return_cut_dates(x, dt_range))
    
    with pd.HDFStore(os.path.join(data_dir, "r1000_data_normalized_fold.h5"), "w") as store:
        store["df"] = df        
    print("done")

def get_market_cap_distribution(data_dir):
    r1000 = list(pd.read_csv(os.path.join(data_dir, "r1000_2.csv"))["Ticker"])
    marketcap = pd.read_csv(os.path.join(data_dir, "marketcap.csv"))
    print("done")

def merge_with_raw_vwap(data_dir, start_date):
    with pd.HDFStore(os.path.join(data_dir, "merged_data_suv.h5"), "r") as store:
        df = store["merged_data"]    
    remove_cols = ['delta_capex', 'shareswa_growth', 'delta_inventory', 'ADX_5_MA_3', 'ADX_5', 'revenue_growth',
                   'bm', 'ULTOSC_7_14_28_MA_3', 'ULTOSC_7_14_28', 'volatility_126', 'SPY_idiovol', 'SLOWD_5_3_3',
                   'WILLR_5', 'WILLR_14', 'WILLR_63', 'PPO_12_26']
        
    X_cols = ['SUV', 'overnight_return', 'log_dollar_volume', 'log_avg_dollar_volume', 'log_mktcap', 'std_turn',
              'gp2assets', 'ret_1B', 'STOCHRSI_FASTK_14_5_3', 'fcf_yield', 'SPY_beta', 'TRIX_30', 'volatility_21',
              'ret_252B', 'ret_5B', 'ADX_5', 'WILLR_63', 'WILLR_5', 'FASTK_5_3', 'SPY_idiovol', 'ret_2B', 'divyield',
              'ULTOSC_7_14_28', 'WILLR_14', 'volatility_63', 'bm_ia', 'SLOWD_5_3_3', 'revenue_growth', 'bm',
              'SPY_correl', 'ret_126B', 'macd_diff', 'volatility_126', 'delta_inventory', 'WILLR_14_MA_3', 'ret_63B',
              'ret_21B', 'WILLR_63_MA_3', 'ULTOSC_7_14_28_MA_3', 'ADX_5_MA_3', 'WILLR_5_MA_3', 'shareswa_growth',
              'delta_capex', 'ret_10B', 'PPO_12_26']
    y_col = "future_ret_5B"
    X_cols = list(set(X_cols) - set(remove_cols))
    r1000 = list(pd.read_csv(os.path.join(data_dir, "r1000_2.csv"))["Ticker"])
    df = df[(df["ticker"].isin(r1000)) & (df["date"]>=start_date)].reset_index(drop=True)
    df = df.set_index(['date', 'ticker'])[[y_col] + X_cols].sort_index()
    df = df.reset_index()
    
    ret = pd.read_hdf(os.path.join(data_dir, 'r1000_data_normalized_fold_vwap.h5'))
    
    df = pd.merge(df, ret[["date", "ticker", "raw_ret"]], how="left", on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "merged_data_with_vwap.h5"), "w") as store:
        store["df"] = df        
    
    print("done")
    

def merge_normalized_vwap(data_dir):
    with pd.HDFStore(os.path.join(data_dir, "r1000_data_normalized_fold.h5"), "r") as store:
        df = store["df"]
    ret = pd.read_hdf(os.path.join(data_dir, 'r1000_data_normalized_fold_vwap.h5'))
    
    df = pd.merge(df, ret[["date", "ticker", "raw_ret", "win_ret", "std_ret"]], how="left", on=["date", "ticker"])
    
    with pd.HDFStore(os.path.join(data_dir, "r1000_data_normalized_fold_vwap_2.h5"), "w") as store:
        store["df"] = df         
    
    print("done")
    




if __name__=="__main__":
    # timeline test
    start_date = pd.Timestamp("2009-03-20")
    end_date = pd.Timestamp("2019-04-30")
    day = 2 # WED
    #days = pick_day_of_the_week(start_date, end_date, day)

    data_dir = r"C:\DCM\temp\pipeline_tests\tiaa_2"
    
    #indicator_data_merge_3(data_dir)
    
    
    filter_dates_monthly(data_dir, start_date, end_date)
    print("done")
    
    #with pd.HDFStore(os.path.join(data_dir, "DerivedDataProcessing_1.h5"), "r") as store:
        #print(store.keys())
        #print("done")
    
    #indicator_data_merge_2(data_dir)
    #filter_dates(data_dir, start_date, end_date, day)
    #merge_with_fundamentals(data_dir)
    
    #merge_with_suv(data_dir)
    
    #normalize_data(data_dir)
    
    start_date = pd.Timestamp("2009-04-01")
    #prepare_final_data(data_dir, start_date)
    #add_cv_column(data_dir)
    
    #merge_with_raw_vwap(data_dir, start_date)
    merge_normalized_vwap(data_dir)
    
    ##get_market_cap_distribution(data_dir)
    
    ##end_date = pd.Timestamp("2019-04-11")    
    
    
    
    tech_vars = ['SPY_correl', 'SPY_beta', 'log_avg_dollar_volume', 'SPY_idiovol', 'macd', 'macd_centerline', 'macd_diff', 'overnight_return', 'ret_1B', 'ret_2B', 'ret_5B', 'ret_10B', 'ret_21B', 'ret_63B', 'ret_126B', 'ret_252B', 'ADX_14', 'ADX_14_MA_3', 'PPO_12_26', 'FASTK_5_3', 'FASTD_5_3', 'STOCHRSI_FASTK_14_5_3', 'STOCHRSI_FASTD_14_5_3', 'SLOWK_5_3_3', 'SLOWD_5_3_3', 'TRIX_30', 'ULTOSC_7_14_28', 'ULTOSC_7_14_28_MA_3', 'WILLR_14', 'WILLR_14_MA_3', 'volatility_63', 'ADX_5', 'ADX_5_MA_3', 'PPO_3_14', 'WILLR_5', 'WILLR_5_MA_3', 'volatility_21', 'ADX_63', 'ADX_63_MA_3', 'PPO_21_126', 'WILLR_63', 'WILLR_63_MA_3', 'volatility_126']
    fund_vars = ['log_mktcap', 'bm', 'bm_ia', 'gp2assets', 'divyield', 'shareswa_growth', 'pe', 'log_dollar_volume', 'debt2equity', 'delta_inventory', 'delta_capex', 'fcf_yield', 'asset_growth', 'revenue_growth', 'std_turn']

    
    y_col = "future_ret_5B"
    
    
    print("done")