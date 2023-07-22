from __future__ import division

import pandas as pd
import numpy as np
from scipy.signal import lfilter, lfiltic

import talib
import warnings

BIPOWER_VAR_DENOM = 0.63664441
TRUNCATED_POWER_VAR_OMEGA = 0.47
_SMALL_EPSILON = np.finfo(np.float64).eps
# TODO (Jack): Investigate further later, increase precision
_PRECISION = 5

def nonnull_lb(value):
    value = value if pd.notnull(value) else -999999999999999
    return value

def nonnull_ub(value):
    value = value if pd.notnull(value) else 999999999999999
    return value

def winsorize(df, cols, by=['date'], alpha=0.01):
    for col in cols:
        lb = df.groupby(by)[col].transform(pd.Series.quantile, alpha / 2.0)
        ub = df.groupby(by)[col].transform(pd.Series.quantile, 1 - alpha / 2.0)
        lb = lb.apply(nonnull_lb)
        ub = ub.apply(nonnull_ub)
        idx = df[col].notnull()
        df.loc[idx, col] = df.loc[idx, col].clip(lb, ub)
    return df

def MA(ts, window_size, type='simple', adjust=True):
    if type=='simple':
        return ts.rolling(window_size, min_periods=window_size).mean()
    else:
        return ts.ewm(span=window_size, min_periods=window_size, adjust=adjust).mean()

def SMA(ts, window_size, type='simple', adjust=True):
    if type=='simple':
        return ts.rolling(window_size, min_periods=window_size).mean()
    else:
        return ts.ewm(span=window_size, min_periods=window_size, adjust=adjust).mean()

def EWMA(ts, window_size, type='simple', adjust=True):
    if type=='simple':
        return ts.rolling(window_size, min_periods=window_size).mean()
    else:
        return ts.ewm(span=window_size, min_periods=window_size, adjust=adjust).mean()

def rsi(ts, window=14):
    if isinstance(ts, pd.DataFrame):
        ts = ts.iloc[:, 0]
    ts = ts.fillna(method='ffill')
    delta = ts.diff()
    u = delta * 0
    d = u.copy()
    cond = (delta>0)
    u[cond] = delta[cond]
    cond = (delta<0)
    d[cond] = -delta[cond]
    u[u.index[window - 1]] = np.mean(u[:window]) #first value is sum of avg gains
    d[d.index[window - 1]] = np.mean(d[:window]) #first value is sum of avg losses
    #rs = MA(u, window, 'ewma', False) / MA(d, window, 'ewma', False)
    rs = u.ewm(com=window-1, adjust=False, min_periods=window-1).mean()\
        /(d.ewm(com=window-1, adjust=False, min_periods=window-1).mean()*1.0)
    x = 100.0 - 100.0 / (1. + rs)
    return x

def macd(ts, nslow=26, nfast=12):
    ts = ts.fillna(method='ffill')
    emaslow = MA(ts, nslow, type='ewma')
    emafast = MA(ts, nfast, type='ewma')
    return emafast - emaslow

def KaufmanMA(ts, offset=10, lambda1=2, lambda2=30):
    ts = ts.fillna(method='ffill')
    pdelta = abs(ts - ts.shift(1))
    ER = (ts - ts.shift(offset)) / (pdelta.rolling(offset).sum() + _SMALL_EPSILON)
    sc = (ER*(2.0/(lambda1+1)-2.0/(lambda2+1.0))+2/(lambda2+1.0))**2.0
    answer = pd.Series(index = ts.index, name=ts.name)
    answer[0:offset] = np.nan
    answer[offset] = ts[offset]
    for i in range(offset+1, len(ts)):
        answer[i] = answer[i-1] + sc[i] * (ts[i] - answer[i-1])
    return answer

def price_oscillator(ts, nslow=26, nfast=12):
    ts = ts.fillna(method='ffill')
    emaslow = MA(ts, nslow, type='ewma')*1.0
    emafast = MA(ts, nfast, type='ewma')*1.0
    return (emafast - emaslow) / (emafast + _SMALL_EPSILON)

def rate_of_change(ts, offset=10):
    ts = ts.fillna(method='ffill')

    ts_shifted = ts.shift(offset)
    result = ((ts - ts_shifted) / (ts_shifted + _SMALL_EPSILON))* 100.

    return result.fillna(value=0.)

def disparity(ts, offset=10):
    ts = ts.fillna(method='ffill')
    ema = MA(ts, offset, type='ewma')*1.0
    return ts / (ema + _SMALL_EPSILON)

def momentum(ts, offset=10):
    ts = ts.fillna(method='ffill')
    ts = ts.diff(offset)
    return ts

def stoch_k(ts, offset=14):
    ts = ts.fillna(method='ffill')
    hi = ts.rolling(window=offset).max()*1.0
    lo = ts.rolling(window=offset).min()*1.0

    return 100. * (ts - lo) / (hi - lo + _SMALL_EPSILON)

def stoch_d(ts, offset=14, window=3):
    ts = ts.fillna(method='ffill')
    hi = ts.rolling(window=offset).max()*1.0
    lo = ts.rolling(window=offset).min()*1.0
    k = 100. * (ts - lo) / (hi - lo + _SMALL_EPSILON)
    d = MA(k, window, type='ewma')
    return d

def slow_stoch_d(ts, offset=14, window1=3, window2=3):
    ts = ts.fillna(method='ffill')
    hi = ts.rolling(window=offset).max()*1.0
    lo = ts.rolling(window=offset).min()*1.0
    k = 100.0 * (ts - lo) / (hi - lo + _SMALL_EPSILON)
    d = MA(k, window1, type='ewma')
    s = MA(d, window2, type='ewma')
    return s

def stoch_k_d(ts, offset, window):
    stochK = stoch_k(ts, offset=offset)
    stochD = stoch_d(ts, offset=offset, window=window)
    return stochK - stochD

def tsi(ts, nslow=25, nfast=13, offset=1):
    ts = ts.fillna(method='ffill')
    ts = ts.diff(offset)
    a = np.abs(ts)
    ema = MA(ts, nslow, type='ewma')
    ema = MA(ema, nfast, type='ewma')*1.0
    ema_a = MA(a, nslow, type='ewma')
    ema_a = MA(ema_a, nfast, type='ewma')*1.0

    return 100. * ema / (ema_a + _SMALL_EPSILON)

# variance estimators
def realized_bipower_var(ts, delta_t, window, fillna=True):
    #NOTE ts here is assumed to be log-returns
    if fillna:
        ts[ts.first_valid_index():] = ts[ts.first_valid_index():].fillna(value=0.0)
    running = np.abs(ts)*np.abs(ts.shift(1))
    var = running.rolling(window=window-2, min_periods=window-2).mean()/(BIPOWER_VAR_DENOM*delta_t)
    return var

def truncated_power_var(ts, delta_t, window, g, fillna=True):
    #NOTE ts here is assumed to be log-returns
    if fillna:
        ts[ts.first_valid_index():] = ts[ts.first_valid_index():].fillna(value=0.0)
    threshold = g*np.power(delta_t, TRUNCATED_POWER_VAR_OMEGA)
    running = ts**2
    running = np.multiply(running, ts<=threshold)
    var = running.rolling(window=window, min_periods=window).mean()/delta_t
    return var

#Jump detection test described on "Detecting Jumps from Levy Jump Diffusion processes, by Suzanne S. Lee and Jan Hannig"

def lee_jump_detection(ts, delta_t, window, g, jump_return_criteria="either", alpha=0.05, n_max=252, fillna=True, recording=None,
                       variance_decimals=6, stat_decimals=3):
    if jump_return_criteria not in ["realized_bipower", "truncated_power", "either", "both"]:
        raise RuntimeError("lee_jump_detection method must be one of 'realized_bipower', 'truncated_power', "
                           "'either' or 'both'")
    if window>n_max:
        warnings.warn("estimation window should not be greater than n_max. window is {0} n_max is {1}"\
                      .format(window, n_max))
    ts[abs(ts)<0.0000001] = 0
    var_rb = pd.np.round(realized_bipower_var(ts, delta_t, window, fillna), variance_decimals)
    var_tp = pd.np.round(truncated_power_var(ts, delta_t, window, g, fillna), variance_decimals)
    lee_test_stat_rb = pd.np.round(ts/np.sqrt(var_rb)/np.sqrt(delta_t), stat_decimals)
    lee_test_stat_tp = pd.np.round(ts/np.sqrt(var_tp)/np.sqrt(delta_t), stat_decimals)

    #n_observations = pd.np.arange(len(ts)) + 1
    # Need to truncate to max observations
    n_observations = n_max
    q = -np.log(-np.log(1.0-alpha))
    log_n = np.log(n_observations)
    log_n_factor = (2.0*log_n)**0.5
    C = log_n_factor - (np.log(np.pi) + np.log(log_n))/(2.0*(log_n_factor))
    S = 1.0/log_n_factor
    lower_lee_test_region = pd.Series(-q*S-C, index=ts.index)
    upper_lee_test_region = pd.Series(q*S+C, index=ts.index)

    jump_indicator_rb = -1*(lee_test_stat_rb<lower_lee_test_region).astype(int) \
        + (lee_test_stat_rb>upper_lee_test_region).astype(int)
    jump_indicator_tp = -1*(lee_test_stat_tp<lower_lee_test_region).astype(int) \
        + (lee_test_stat_tp>upper_lee_test_region).astype(int)
    if jump_return_criteria == "realized_bipower":
        jump_indicator = jump_indicator_rb
    elif jump_return_criteria == "truncated_power":
        jump_indicator = jump_indicator_tp
    elif jump_return_criteria == "both":
        jump_indicator = ((jump_indicator_rb + jump_indicator_tp) / 2.0).astype(int)
    else:
        jump_indicator = pd.np.sign((jump_indicator_rb + jump_indicator_tp) / 2.0)
    if recording is not None:
        recording["criteria"] = jump_return_criteria
        recording["var_rb"] = var_rb.iloc[-1]
        recording["var_tp"] = var_tp.iloc[-1]
        recording["lee_test_stat_rb"] = lee_test_stat_rb.iloc[-1]
        recording["lee_test_stat_tp"] = lee_test_stat_tp.iloc[-1]
        recording["lower_lee_test_region"] = lower_lee_test_region.iloc[-1]
        recording["upper_lee_test_region"] = upper_lee_test_region.iloc[-1]
    return jump_indicator

def adx(dataframe, date_column=None, high_col="High", low_col="Low", close_col="Close", window=14):
    data = dataframe.copy()
    if date_column is not None:
        data.set_index(date_column, inplace=True)
    data = data.sort_index()
    data["Open"] = data[close_col].shift(1)
    data["TR"] = pd.np.maximum(pd.np.maximum((data[high_col] - data[low_col]).values,
                               pd.np.abs(data[high_col] - data["Open"].combine_first(data[high_col])).values),
                               pd.np.abs(data[low_col] - data["Open"].combine_first(data[low_col])).values)

    data["H-H_prev"] = data[high_col] - data[high_col].shift(1)
    data["L_prev-L"] = data[low_col].shift(1) - data[low_col]

    data["+DM"] = pd.np.maximum(data["H-H_prev"], 0)*(data["H-H_prev"] > data["L_prev-L"])
    data["-DM"] = pd.np.maximum(data["L_prev-L"], 0)*(data["L_prev-L"] > data["H-H_prev"])

    lfilt_b = pd.np.asarray([1.0])
    lfilt_a = pd.np.asarray([1.0, -(1.0-1.0/window)])

    for k in ["TR", "+DM", "-DM"]:
        col_label = "{0}{1}".format(k,window)
        data[col_label] = pd.np.NaN
        initial_filt_val = sum(data[k].iloc[1:window+1])
        series_to_filt = data[k].iloc[window:].shift(-1).shift(1).fillna(initial_filt_val)
        data[col_label].iloc[window:] = lfilter(lfilt_b, lfilt_a, series_to_filt)

    plus_di_label = "+DI{0}".format(window)
    minus_di_label = "-DI{0}".format(window)
    tr_agg_label = "TR{0}".format(window)
    di_diff_label = "DI{0} Diff".format(window)
    di_sum_label = "DI{0} Sum".format(window)

    data[plus_di_label] = data["+DM{0}".format(window)] / data[tr_agg_label] * 100
    data[minus_di_label] = data["-DM{0}".format(window)] / data[tr_agg_label] * 100

    data[di_diff_label] = abs(data[plus_di_label] - data[minus_di_label])
    data[di_sum_label] = data[plus_di_label] + data[minus_di_label]

    data["DX"] = data[di_diff_label] / data[di_sum_label] * 100

    data["ADX"] = np.nan

    initial_adx_value = data["DX"].iloc[window:2*window].mean()
    dx_as = pd.np.asarray([1.0, -(window-1.0)/window])
    dx_bs = pd.np.asarray([1.0/window])
    dx_to_filt = data["DX"].iloc[2*window:]
    zi = lfiltic(dx_bs, dx_as, [initial_adx_value], [initial_adx_value])

    data["ADX"].iloc[2*window-1] = initial_adx_value
    data["ADX"].iloc[2*window:] = lfilter(dx_bs, dx_as, dx_to_filt, zi=zi)[0]

    return data[["ADX",plus_di_label,minus_di_label]]

def get_bolinger_bands(ts, bolinger_band_length = 22, bollinger_band_sdev_multiplier = 1.05,
                       lookback_period_percentile_high = 22, highest_percentile = .90):
    sDev = bollinger_band_sdev_multiplier * ts.rolling(bolinger_band_length, bolinger_band_length).std()
    midLine = ts.rolling(bolinger_band_length, bolinger_band_length).mean()
    lowerBand = midLine - sDev
    upperBand = midLine + sDev
    rangeHigh = ts.rolling(lookback_period_percentile_high, lookback_period_percentile_high).max() * highest_percentile        
    return (lowerBand, upperBand, rangeHigh)

def get_smoothed_WVF(WVF_ts, ticker_wvf_param, smoothing = 'Fast'):
    WVF_smoothed = WVF_ts.copy()
    LengthEMA = ticker_wvf_param.get("wvf_smooth_window_fast", 10) if smoothing =='Fast' \
                        else ticker_wvf_param.get("wvf_smooth_window_slow", 30)
    WVF_smoothed = EWMA(WVF_smoothed, 1 + 2 * LengthEMA, type='ewma')
    WVF_smoothed.name = 'WVF {0} Smooth'.format(smoothing)
    return WVF_smoothed

def compute_WVF(ts, LengthWVF=100, low_col="low", close_col="close"):
    ts = ts.fillna(method='ffill')
    WVF_df = ts.copy()
    rolling_max = ts[close_col].rolling(LengthWVF).max()
    WVF_df['WVF'] = 100 * ((rolling_max - ts[low_col]) / rolling_max)
    return WVF_df['WVF']

LARGE_LOOKBACK = 100000

def getWeights(d, size):
    w = [1.]
    for k in range(1, size):
        w_ = -w[-1]/k*(d-k+1)
        w.append(w_)
    w=np.array(w[::-1]).reshape(-1, 1)
    return w

def getWeights_FFD(d, thresh):
    w = [1.]
    for k in range(1, LARGE_LOOKBACK):
        w_ = -w[-1]/k*(d-k+1)
        if abs(w_)>=thresh:
            w.append(w_)
        else:
            break
    w=np.array(w[::-1]).reshape(-1, 1)
    return w

def plotWeights(dRange, nPlots, size):
    w = pd.DataFrame()
    for d in np.linspace(dRange[0], dRange[1], nPlots):
        w_ = getWeights(d, size=size)
        w_ = pd.DataFrame(w_, index=range(w_.shape[0])[::-1], columns=[d])
        w = w.join(w_, how='outer')
    ax = w.plot()
    ax.legend(loc='upper left')
    plt.show()

def fracDiff(series, d, thres=.01):
    #1) Compute weights for the longest series
    w = getWeights(d,series.shape[0])
    #2) Determine initial calcs to be skipped based on weight-loss threshold
    w_= np.cumsum(abs(w))
    w_ /= w[-1]
    skip = w_[w_>thres].shape[0]
    #3) Apply weights to values
    df={}
    for name in series.columns:
        seriesF = series[[name]].fillna(method='ffill').dropna()
        df_ = pd.Series()
        for iloc in range(skip, seriesF.shape[0]):
            loc = seriesF.index[iloc]
            if not np.isfinite(series.loc[locename]):
                continue
            df_[loc] = np.dot(w[-(iloc+1):,:].T, seriesF.loc[:loc])[0, 0]
            df[name]=df_.copy(deep=True)
        df=pd.concat(df, axis=1)
    return df

def fracDiff_FFD(series, differencing=0.6, threshold=1e-3):
    w = getWeights_FFD(differencing, threshold)
    width = len(w) - 1
    df={}
    if isinstance(series, pd.Series):
        series = series.to_frame(series.name)
    for name in series.columns:
        seriesF, df_ = series[[name]].fillna(method='ffill').dropna(), pd.Series()
        for iloc1 in range(width, seriesF.shape[0]):
            loc0, loc1 = seriesF.index[iloc1-width], seriesF.index[iloc1]
            if not np.isfinite(series.loc[loc1,name]):
                continue
            df_[loc1] = np.dot(w.T, seriesF.loc[loc0:loc1])[0,0]
        df[name] = df_.copy (deep=True)
    df = pd.concat(df, axis=1)
    if isinstance(series, pd.Series):
        df = df[df.columns[0]]
    return df

# pandas wrappers for talib functions
def talib_ADX(high, low, close, timeperiod=14):
    return pd.Series(talib.ADX(high.values, low.values, close.values, timeperiod), index=high.index)

def talib_RSI(close, timeperiod=14):
    return pd.Series(talib.RSI(close.values, timeperiod), index=close.index)

def talib_ADXR(high, low, close, timeperiod=14):
    return pd.Series(talib.ADXR(high.values, low.values, close.values, timeperiod=timeperiod), index=high.index)

def talib_APO(close, fastperiod=12, slowperiod=26, matype=0):
    return pd.Series(talib.APO(close.values, fastperiod, slowperiod, matype), index=close.index)

def talib_AROON(high, low, timeperiod=14, side="both"):
    index = high.index
    aroondown, aroonup = talib.AROON(high.values, low.values, timeperiod)
    down = pd.Series(aroondown, index=index)
    up = pd.Series(aroonup, index=index)
    if side=="up":
        return up
    elif side=="down":
        return down
    else:
        return up, down

def talib_AROONOSC(high, low, timeperiod=14):
    return pd.Series(talib.AROONOSC(high.values, low.values, timeperiod), index=high.index)

def talib_BOP(open, high, low, close):
    return pd.Series(talib.BOP(open.values, high.values, low.values, close.values), index=close.index)

def talib_CCI(high, low, close, timeperiod=14):
    return pd.Series(talib.CCI(high.values, low.values, close.values, timeperiod), index=close.index)

def talib_CMO(close, timeperiod=14):
    return pd.Series(talib.CMO(close.values, timeperiod), index=close.index)

def talib_DX(high, low, close, timeperiod=14):
    return pd.Series(talib.DX(high.values, low.values, close.values, timeperiod), index=high.index)

def talib_MACD(close, fastperiod=12, slowperiod=26, signalperiod=9):
    index = close.index
    macd, macdsignal, macdhist = talib.MACD(close.values, fastperiod, slowperiod, signalperiod)
    return pd.Series(macd, index=index), pd.Series(macdsignal, index=index), pd.Series(macdhist, index=index)

def talib_MACDEXT(close, fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0):
    index = close.index
    macd, macdsignal, macdhist = MACDEXT(close.values, fastperiod, fastmatype, slowperiod, slowmatype, signalperiod,
                                         signalmatype)
    return pd.Series(macd, index=index), pd.Series(macdsignal, index=index), pd.Series(macdhist, index=index)

def talib_MFI(high, low, close, volume, timeperiod=14):
    return pd.Series(talib.MFI(high.values, low.values, close.values, volume.values, timeperiod), index=high.index)

def talib_MINUS_DI(high, low, close, timeperiod=14):
    return pd.Series(talib.MINUS_DI(high.values, low.values, close.values, timeperiod), index=high.index)

def talib_MINUS_DM(high, low, timeperiod=14):
    return pd.Series(talib.MINUS_DM(high.values, low.values, timeperiod), index=high.index)

def talib_MOM(close, timeperiod=10):
    return pd.Series(talib.MOM(close.values, timeperiod), index=close.index)

def talib_PLUS_DI(high, low, close, timeperiod=14):
    return pd.Series(talib.PLUS_DI(high.values, low.values, close.values, timeperiod), index=high.index)

def talib_PLUS_DM(high, low, timeperiod=14):
    return pd.Series(talib.PLUS_DM(high.values, low.values, timeperiod), index=high.index)

def talib_PPO(close, fastperiod=12, slowperiod=26, matype=0):
    return pd.Series(talib.PPO(close.values, fastperiod=fastperiod, slowperiod=slowperiod, matype=matype), index=close.index)

def talib_ROC(close, timeperiod=10):
    return pd.Series(talib.ROC(close.values, timeperiod=timeperiod), index=close.index)

def talib_ROCR(close, timeperiod=10):
    return pd.Series(talib.ROCR(close.values, timeperiod=timeperiod), index=close.index)

def talib_ROCR100(close, timeperiod=10):
    return pd.Series(talib.ROCR100(close.values, timeperiod=timeperiod), index=close.index)

def talib_STOCH(high, low, close, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0,
                k_or_d="both"):
    index = close.index
    slowk, slowd = pd.Series(talib.STOCH(high.values, low.values, close.values, fastk_period, slowk_period,
                                         slowk_matype, slowd_period, slowd_matype))
    k = pd.Series(slowk, index=index)
    d = pd.Series(slowd, index=index)
    if k_or_d=="k":
        return k
    elif k_or_d=="d":
        return d
    else:
        return k, d

def talib_STOCHF(high, low, close, fastk_period=5, fastd_period=3, fastd_matype=0, k_or_d="both"):
    index = close.index
    fastk, fastd = pd.Series(talib.STOCHF(high.values, low.values, close.values, fastk_period, fastd_period,
                                          fastd_matype))
    k = pd.Series(fastk, index=index)
    d = pd.Series(fastd, index=index)
    if k_or_d=="k":
        return k
    elif k_or_d=="d":
        return d
    else:
        return k, d

def talib_STOCHRSI(close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0, k_or_d="both"):
    index = close.index
    fastk, fastd = pd.Series(talib.STOCHRSI(close.values, timeperiod, fastk_period, fastd_period, fastd_matype))
    k = pd.Series(fastk, index=index)
    d = pd.Series(fastd, index=index)
    if k_or_d=="k":
        return k
    elif k_or_d=="d":
        return d
    else:
        return k, d

def talib_TRIX(close, timeperiod=30):
    return pd.Series(talib.TRIX(close, timeperiod), index=close.index)

def talib_ULTOSC(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28):
    return pd.Series(talib.ULTOSC(high.values, low.values, close.values, timeperiod1, timeperiod2, timeperiod3),
                     index=high.index)

def talib_WILLR(high, low, close, timeperiod=14):
    return pd.Series(talib.WILLR(high.values, low.values, close.values, timeperiod), index=close.index)

def talib_AD(high, low, close, volume):
    return pd.Series(talib.AD(high.values, low.values, close.values, volume.values), index=high.index)

def talib_ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10):
    return pd.Series(talib.ADOSC(high.values, low.values, close.values, volume.values, fastperiod, slowperiod),
                     index=high.index)

def talib_OBV(close, volume):
    return pd.Series(talib.OBV(close.values, volume.values), index=close.index)

def talib_ATR(high, low, close, timeperiod=14):
    return pd.Series(talib.ATR(high.values, low.values, close.values, timeperiod), index=high.index)

def talib_NATR(high, low, close, timeperiod=14):
    return pd.Series(talib.NATR(high.values, low.values, close.values, timeperiod), index=high.index)

def talib_TRANGE(high, low, close):
    return pd.Series(talib.TRANGE(high.values, low.values, close.values), index=high.index)


if __name__=="__main__":
    # This section is for testng
    from pipelines.market_view_calibration.snapshot import *

    filename = r"D:\DCM\engine_results\views_experiment\enhanced_data.h5"
    with pd.HDFStore(r"D:\DCM\engine_results\views_experiment\enhanced_data.h5") as store:
        merged_data = store["merged_data"]

    data = merged_data.set_index("date")
    ts = data["VXMT_prev_close"]

    res = talib_RSI(ts, 14)
    res = fracDiff_FFD(ts, 0.6, 1e-3)
    res = talib_ADX(ts+0.5, ts-0.5, ts, 14)
    print("done")
