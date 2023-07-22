import random
import pandas as pd

from commonlib.market_timeline import marketTimeline
from commonlib.commonStatTools.technical_indicators.offline import price_oscillator, stoch_k, stoch_d, MA
from commonlib.commonStatTools.clustering_tools import *

from etl_workflow_steps import StatusType

from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask, CalibrationFileTaskflowTask
from calibrations.common.taskflow_params import TaskflowPipelineRunMode
from calibrations.common.calibration_logging import calibration_logger
from calibrations.common.calibrations_utils import store_pandas_object

RANDOM_STATE = 12345

class GenerateKMeansClusters(CalibrationTaskflowTask):
    '''

    Performs Kmeans cluster on a given data matrix and feature coordinate system. Returns clusters per date.

    '''
    #TODO: remove prices later this is just for splitting the flows
    PROVIDES_FIELDS = ["kmeans_cluster_data"]
    REQUIRES_FIELDS = ["liquidity_list", "beta_data", "pca_data", "calibration_dates", "daily_price_data",
                       "daily_index_data"]

    def __init__(self, n_clusters, method, feature_list=None):
        self.data = None
        self.n_clusters = n_clusters or ''
        self.method = method or 'auto'
        self.feature_list = feature_list
        if feature_list:
            self.feature_list.extend(["date", "ticker"])
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        liquidity_list = kwargs[self.__class__.REQUIRES_FIELDS[0]].set_index('date')
        beta_data = kwargs[self.__class__.REQUIRES_FIELDS[1]].reset_index()
        pca_data = kwargs[self.__class__.REQUIRES_FIELDS[2]].reset_index()

        all_features = pd.merge(beta_data, pca_data, how='left', on=['date', 'ticker'])
        non_rank_columns = all_features.columns[map(lambda x:'_rank' not in x, list(all_features.columns))]
        non_rank_columns = self.feature_list or non_rank_columns
        all_features = all_features[non_rank_columns]
        dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[3]]['date']))
        dates_to_compute.sort()
        kmeans_cluster_data = pd.DataFrame(index=dates_to_compute, columns=range(self.n_clusters))
        for dt in dates_to_compute:
            features = all_features[all_features['date']==dt].drop('date', axis=1).dropna().set_index('ticker')
            if len(features):
                # Apply liquidity filter
                liquid_names = liquidity_list.loc[dt, "tradeable_tickers"]
                features = features.loc[features.index[features.index.isin(liquid_names)]]
                results = get_kmeans(features, self.n_clusters, self.method)
                for c in range(self.n_clusters):
                    kmeans_cluster_data.loc[dt, c] = list(results[results["cluster_id"]==c].index)
            else:
                continue
        self.data =  kmeans_cluster_data.reset_index().rename(columns={"index": "date"})
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"n_clusters": 13, "feature_list": ["SPY_beta", "VWO_beta", "TLT_beta", "GLD_beta", "GSG_beta", "PC_1_beta",
                                                 "PC_2_beta", "PC_3_beta", "date", "ticker"],
                "method": "auto"}


class GenerateGraphicalLassoGroups(CalibrationTaskflowTask):
    '''

    Performs graphical lasso analysis and returns the lasso groups per date, and cluster.

    '''
    PROVIDES_FIELDS = ["glasso_group_data", "strength_index_data"]
    REQUIRES_FIELDS = ["daily_price_data", "kmeans_cluster_data", "lasso_calculate_dates", "daily_index_data"]

    def __init__(self, lookback, offset_unit, price_column, normalize, dropna_pctg, min_group_size, max_group_size,
                 shrinkage, use_cross_validation, alpha):
        self.data = None
        self.strength_data = None
        self.lookback = lookback or 126
        self.offset_unit = offset_unit or "B"
        self.offset = "{0}{1}".format(self.lookback, self.offset_unit)
        self.price_column = price_column or "close"
        self.normalize = normalize or True
        self.dropna_pctg = dropna_pctg or 0.15
        self.required_records = int(self.lookback*(1-self.dropna_pctg))
        self.min_group_size = min_group_size or 4
        self.max_group_size = max_group_size or 25
        self.shrinkage = shrinkage or 0.1
        self.use_cross_validation = use_cross_validation or False
        self.alpha = alpha or 0.01
        CalibrationTaskflowTask.__init__(self)

    def _glasso_group(self, all_returns, all_clusters):
        glasso_groups = pd.DataFrame(index=all_clusters.index, columns=['baskets'])
        strength_index = pd.DataFrame(index=all_clusters.index, columns=['strength_index'])
        for dt in self.dates_to_compute:
            start_date = dt - pd.tseries.frequencies.to_offset(self.offset)
            current_data = all_returns.loc[start_date:dt]
            return_data = current_data.dropna(thresh=self.required_records, axis=1).fillna(value=0.0)
            clusters = all_clusters.loc[:dt].tail(1).dropna()
            if len(clusters):
                clusters = clusters.loc[clusters.index[0]]
            all_baskets = []
            if len(clusters):
                for c in clusters.index:
                    current_cluster = clusters.loc[c]
                    if len(current_cluster)>=self.min_group_size:
                        current_returns = return_data[current_cluster]
                        try:
                            baskets, _, alpha = get_glasso_groups(current_returns, alpha=self.alpha, min_group_size=self.min_group_size,
                                                                  max_group_size=self.max_group_size, take_return=False,
                                                                  normalize=self.normalize, shrinkage=self.shrinkage,
                                                                  use_cross_validation=self.use_cross_validation, max_iter=150)

                        except:
                            baskets = [] # If matrix is ill-conditioned
                    else:
                        baskets = []
                    all_baskets.extend(baskets)
            else:
                all_baskets = []
            glasso_groups.loc[dt, "baskets"] = all_baskets
            calibration_logger.info("{0}: {1}".format(dt, len(all_baskets)))
            strength_index.loc[dt, "strength_index"] = len(all_baskets)
        self.strength_data = strength_index
        return glasso_groups

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        kmeans_cluster_data = kwargs[self.__class__.REQUIRES_FIELDS[1]].set_index('date')
        self.dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[2]]['date']))
        self.dates_to_compute.sort()
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values = self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = pd.np.log(pivot_data).diff()
        return_data.index = return_data.index.normalize()
        glasso_groups = self._glasso_group(return_data, kmeans_cluster_data)
        self.data = glasso_groups.reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.strength_data}

    @classmethod
    def get_default_config(cls):
        return {"lookback": 189, "offset_unit": "B", "price_column": "close", "normalize": True,
                "dropna_pctg": 0.15, "min_group_size": 6, "max_group_size": 20, "shrinkage": 0.15,
                "use_cross_validation": False, "alpha": 0.01}

class GenerateGraphicalLassoBenchmark(CalibrationTaskflowTask):
    '''

    Performs graphical lasso analysis and returns the lasso groups per date, and cluster using cross-validation
    for determining the alpha parameter.

    '''
    PROVIDES_FIELDS = ["cross_validation_data", "alpha_data", "cluster_size_data", "strength_benchmark_data"]
    REQUIRES_FIELDS = ["daily_price_data", "kmeans_cluster_data", "lasso_calculate_dates", "daily_index_data",
                       "glasso_group_data", "strength_index_data"]

    def __init__(self, lookback, offset_unit, price_column, normalize, dropna_pctg, min_group_size, max_group_size,
                 shrinkage, use_cross_validation, alpha):
        self.data = None
        self.alpha_data = None
        self.cluster_size_data = None
        self.strength_bmk = None
        self.lookback = lookback or 126
        self.offset_unit = offset_unit or "B"
        self.offset = "{0}{1}".format(self.lookback, self.offset_unit)
        self.price_column = price_column or "close"
        self.normalize = normalize or True
        self.dropna_pctg = dropna_pctg or 0.15
        self.required_records = int(self.lookback*(1-self.dropna_pctg))
        self.min_group_size = min_group_size or 4
        self.max_group_size = max_group_size or 25
        self.shrinkage = shrinkage or 0.1
        self.use_cross_validation = use_cross_validation
        self.alpha = alpha or 0.01
        CalibrationTaskflowTask.__init__(self)

    def _glasso_group(self, all_returns, all_clusters):
        glasso_groups = pd.DataFrame(index=all_clusters.index, columns=['baskets'])
        alpha_index = pd.DataFrame(index=all_clusters.index, columns=all_clusters.columns)
        cluster_size = pd.DataFrame(index=all_clusters.index, columns=all_clusters.columns)
        strength_index = pd.DataFrame(index=all_clusters.index, columns=['strength_index'])
        for dt in self.dates_to_compute:
            start_date = dt - pd.tseries.frequencies.to_offset(self.offset)
            current_data = all_returns.loc[start_date:dt]
            return_data = current_data.dropna(thresh=self.required_records, axis=1).fillna(value=0.0)
            clusters = all_clusters.loc[:dt].tail(1).dropna()
            if len(clusters):
                clusters = clusters.loc[clusters.index[0]]
            all_baskets = []
            if len(clusters):
                for c in clusters.index:
                    current_cluster = clusters.loc[c]
                    if len(current_cluster)>=self.min_group_size:
                        current_returns = return_data[current_cluster]
                        try:
                            baskets, _, alpha = get_glasso_groups(current_returns, alpha=self.alpha, min_group_size=self.min_group_size,
                                                                  max_group_size=self.max_group_size, take_return=False,
                                                                  normalize=self.normalize, shrinkage=self.shrinkage,
                                                                  use_cross_validation=self.use_cross_validation, max_iter=150)

                            alpha_index.loc[dt, c] = alpha
                            cluster_size.loc[dt, c] = len(current_cluster)
                        except:
                            baskets = [] # If matrix is ill-conditioned
                    else:
                        baskets = []
                    all_baskets.extend(baskets)
            else:
                all_baskets = []
            glasso_groups.loc[dt, "baskets"] = all_baskets
            calibration_logger.info("{0}: {1}".format(dt, len(all_baskets)))
            strength_index.loc[dt, "strength_index"] = len(all_baskets)
        self.alpha_data = alpha_index
        self.cluster_size_data = cluster_size
        self.strength_bmk = strength_index
        return glasso_groups

    def do_step_action(self, **kwargs):
        daily_prices = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        kmeans_cluster_data = kwargs[self.__class__.REQUIRES_FIELDS[1]].set_index('date')
        self.dates_to_compute = list(set(kwargs[self.__class__.REQUIRES_FIELDS[2]]['date']))
        self.dates_to_compute.sort()
        pivot_data = daily_prices[["date", "ticker", self.price_column]] \
            .pivot_table(index="date", values = self.price_column, columns="ticker", dropna=False).sort_index()
        return_data = pd.np.log(pivot_data).diff()
        return_data.index = return_data.index.normalize()
        glasso_groups = self._glasso_group(return_data, kmeans_cluster_data)
        self.data = glasso_groups.reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.alpha_data,
                self.__class__.PROVIDES_FIELDS[2] : self.cluster_size_data,
                self.__class__.PROVIDES_FIELDS[3] : self.strength_bmk}

    @classmethod
    def get_default_config(cls):
        return {"lookback": 189, "offset_unit": "B", "price_column": "close", "normalize": True,
                "dropna_pctg": 0.15, "min_group_size": 6, "max_group_size": 20, "shrinkage": 0.15, "alpha": 0.01,
                "use_cross_validation": True}

class GenerateLongShortBaskets(CalibrationTaskflowTask):
    '''

    Within the graphical lasso groups formulates long short baskets using technical indicators

    '''
    PROVIDES_FIELDS = ["glasso_group_data"]
    REQUIRES_FIELDS = ["glasso_group_data", "technical_indicator_data", "calibration_dates"]

    def __init__(self, pctg_cutoff, rounding):
        self.data = None
        self.pctg_cutoff = pctg_cutoff or 1.0/3
        self.rounding = rounding or False
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        glasso_groups = kwargs[self.__class__.REQUIRES_FIELDS[0]].set_inde('date')
        technical_indicator = kwargs[self.__class__.REQUIRES_FIELDS[0]].set_inde('date')

        calibration_logger.info("done")

    @classmethod
    def get_default_config(cls):
        return {"pctg_cutoff": 1.0/3, "rounding": False}

class PortfolioMeanReversionTraderCalibration(CalibrationFileTaskflowTask):
    '''

    Creates basket pairs calibration files with parameters taken from the long short baskets and the config.
    Deposits file to a local location or s3

    '''
    PROVIDES_FIELDS = ["trader_calibration"]
    REQUIRES_FIELDS = ["glasso_group_data", "technical_indicator_data", "lasso_calculate_dates",
                       "strength_index_data", "cross_validation_data",
                       "alpha_data", "cluster_size_data", "strength_benchmark_data"]
    TRADER_FIELDS = ["date", "actorID", "role", "ranking", "first_daily_check_up_time", "valid_until_date",
                     "long_basket", "short_basket", "stoch_d_params", "entry_threshold", "exit_threshold",
                     "time_limit", "indicator_check_frequency", "stop_limits_pct", "total_funding",
                     "fundingRequestsAmount", "funding_buffer", "override_filter", "instructions_overrides"]

    def __init__(self, first_daily_check_up_time, stoch_d_params, entry_threshold, exit_threshold, time_limit,
                 indicator_check_frequency, stop_limits_pct, total_funding, fundingRequestsAmount, funding_buffer,
                 proportion, valid_until_offset, output_dir, file_suffix, drop_duplicates, add_supervisor,
                 ignore_calibration_timeline, calibration_date_offset, calibration_anchor_date):
        self.data = None
        self.dates_to_compute = None
        self.first_daily_check_up_time = first_daily_check_up_time or "09:30:00"
        self.stoch_d_params =  str(dict(stoch_d_params)).replace("'", '"') or '{"offset": 14, "window": 3}'
        self.entry_threshold = entry_threshold or -50
        self.exit_threshold = exit_threshold or 0.0 #10
        self.time_limit = time_limit or "21B"
        self.indicator_check_frequency = indicator_check_frequency or "00:30:00"
        self.stop_limits_pct = stop_limits_pct or "(-0.08, 10.0)"
        self.total_funding = total_funding or 15000000.0
        self.fundingRequestsAmount = fundingRequestsAmount or 150000.0
        self.funding_buffer = funding_buffer or 100000.0
        self.proportion = eval(proportion) or 1.0/3
        self.valid_until_offset = valid_until_offset or "21B"
        self.drop_duplicates = drop_duplicates or True
        self.add_supervisor = add_supervisor or False
        self.calibration_date_offset = calibration_date_offset or "21B"
        self.calibration_anchor_date = calibration_anchor_date or None
        CalibrationFileTaskflowTask.__init__(self, output_dir, file_suffix, ignore_calibration_timeline)

    def do_step_action(self, **kwargs):
        glasso_group_data = kwargs[self.__class__.REQUIRES_FIELDS[0]].dropna().set_index('date')
        technical_indicator_data = kwargs[self.__class__.REQUIRES_FIELDS[1]].reset_index()
        lasso_calculate_dates = list(kwargs[self.__class__.REQUIRES_FIELDS[2]]['date'])
        self.dates_to_compute = lasso_calculate_dates

        oscillator_data = technical_indicator_data[["date", "ticker", "indicator"]] \
            .pivot_table(index="date", values = "indicator", columns="ticker").sort_index()
        oscillator_data.index = oscillator_data.index.normalize()

        all_cal = []
        for dt in self.dates_to_compute:
            if not marketTimeline.isTradingDay(dt):
                continue
            date_str = str(dt.date()).replace("-", "")
            original_groups = glasso_group_data.loc[dt, "baskets"]
            # Introduced to shuffle order 05/24/2018
            current_groups = list(original_groups)
            random.seed(RANDOM_STATE)
            random.shuffle(current_groups)
            num_groups = len(current_groups)
            cal_df = pd.DataFrame(index=range(num_groups),
                                  columns=PortfolioMeanReversionTraderCalibration.TRADER_FIELDS)
            cal_df["first_daily_check_up_time"] = self.first_daily_check_up_time
            cal_df["role"] = int(2)
            cal_df["ranking"] = range(len(current_groups))
            cal_df["stoch_d_params"] = self.stoch_d_params
            cal_df["entry_threshold"] = self.entry_threshold
            cal_df["exit_threshold"] = self.exit_threshold
            cal_df["time_limit"] = self.time_limit
            cal_df["indicator_check_frequency"] = self.indicator_check_frequency
            cal_df["stop_limits_pct"] = self.stop_limits_pct
            cal_df["date"] = dt.date()
            cal_df["valid_until_date"] = \
                str((dt.date()+pd.tseries.frequencies.to_offset(self.valid_until_offset)).date())
            for ind in range(num_groups):
                basket = current_groups[ind]
                num_per_leg = int(len(basket)*self.proportion)
                current_oscillator = oscillator_data.loc[dt, current_groups[ind]]
                current_oscillator.sort_values(inplace=True)
                longs = list(current_oscillator[:num_per_leg].index)
                shorts = list(current_oscillator[-num_per_leg:].index)
                cal_df.loc[ind, "long_basket"] = str(map(Ticker, longs))
                cal_df.loc[ind, "short_basket"] = str(map(Ticker, shorts))
                cal_df.loc[ind, "actorID"] = "bt_trader_{0}_{1}".format(date_str, ind)
                calibration_logger.info(cal_df.loc[ind, "actorID"])
            if self.add_supervisor:
                cal_df.loc[num_groups, "date"] = dt.date()
                cal_df.loc[num_groups, "actorID"] = "SUP"
                cal_df.loc[num_groups, "role"] = int(1)
                cal_df.loc[num_groups, 'total_funding'] = self.total_funding
                cal_df.loc[num_groups, "fundingRequestsAmount"] = self.fundingRequestsAmount
                cal_df.loc[num_groups, "funding_buffer"] = self.funding_buffer
            if self.drop_duplicates:
                cal_df = cal_df.drop_duplicates(['date', 'long_basket', 'short_basket']).reset_index(drop=True)
            all_cal.append(cal_df)

        cal_df = pd.concat(all_cal).reset_index(drop=True)
        self.data = cal_df
        # Only output the end_dt
        mode = self.task_params.run_mode
        calibration_mode = mode is TaskflowPipelineRunMode.Calibration
        if calibration_mode:
            output_df = cal_df[cal_df['date']==self.task_params.end_dt.normalize().date()].reset_index(drop=True)
        else:
            output_df = cal_df
        filename = self.get_file_targets()[0]
        if mode != TaskflowPipelineRunMode.Test:
            store_pandas_object(output_df, filename, "Basket Pairs Calibration")
        return StatusType.Success

    def get_calibration_dates(self, task_params):
        return self.get_calibration_dates_with_offset_anchor(task_params, self.calibration_date_offset,
                                                             self.calibration_anchor_date)

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"first_daily_check_up_time": "09:33:00", "stoch_d_params": {"offset": 14, "window": 3},
                "entry_threshold": -50.0, "exit_threshold": 0.0, "time_limit": "20B",
                "indicator_check_frequency": "00:05:00", "stop_limits_pct": "(-0.14, 10.0)",
                "total_funding": 18000000.0, "fundingRequestsAmount": 450000.0,
                "funding_buffer": 150000.0, "proportion": "1.0/3", "valid_until_offset": "20B",
                "output_dir": "D:/DCM/engine_results/pairs_trading_live_20181218",
                "file_suffix": "basket_pair_trader.csv",
                "drop_duplicates": True, "add_supervisor": True,
                "ignore_calibration_timeline":True, "calibration_date_offset": "21B", "calibration_anchor_date": None}



class FilterCalibrationConcentrationLimits(CalibrationFileTaskflowTask):
    '''
    
    Filters names for approximate concentration control
    
    '''
    
    REQUIRES_FIELDS = ["security_master", "daily_price_data", "trader_calibration"]
    PROVIDES_FIELDS = ["filtered_trader_calibration", "singlename_concentration", "sector_concentration"]
    
    def __init__(self, output_dir, file_suffix, ignore_calibration_timeline, calibration_date_offset, calibration_anchor_date,
                 entry_tol=0.0, exit_tol=0.0, filter_start_dt=None, filter_end_dt=None, singlename_limit=None, sector_limit=None,
                 weighting=None):
        # Sector limits are removed from v1.0
        self.calibration_date_offset = calibration_date_offset or "21B"
        self.calibration_anchor_date = calibration_anchor_date
        self.entry_tol = entry_tol
        self.exit_tol = exit_tol
        self.filter_start_dt = pd.Timestamp(filter_start_dt) if filter_start_dt else None
        self.filter_end_dt = pd.Timestamp(filter_end_dt) if filter_end_dt else None
        self.singlename_limit = singlename_limit
        self.sector_limit = sector_limit
        self.weighting = weighting or {"entry_signal_delta": 0.5, "signal_coherence": 0.5/3., 
                                       "return_coherence": 0.5/3., "basket_size": 0.5/3.}
        self.data = None
        self.singlename_concentration = None
        self.sector_concentration = None
        CalibrationFileTaskflowTask.__init__(self, output_dir, file_suffix, ignore_calibration_timeline)
    
    def diversity_score(self, basket, price_data, lookback=126):
        prices = price_data[basket]
        rets = prices/prices.shift() - 1.
        rets = rets[-lookback:].fillna(0.0)
        numer = rets.std().mean()
        n_names = len(basket)
        n_obs = len(rets)
        Sigma = rets.cov()
        weights = np.ones((1, n_names))
        denom = np.matmul(np.matmul(weights, Sigma), weights.T)[0][0]
        diversity_score = 1.0*numer/denom
        return diversity_score
    
    def get_score(self, trader_results, weighting):
        columns = list(weighting.keys())
        score = pd.Series(np.zeros(len(trader_results)), index=trader_results.index)
        for col in columns:
            weight = 1.0*weighting[col]
            score += pd.Series(weight*zscore(trader_results[col]), index=trader_results.index).fillna(0.0)
        return score
    
    def check_entries_and_rank(self, cal_df, price_data, entry_tol=0.0, exit_tol=0.0, lookback_div=126,
                               lookback_sig=63):
        traders = cal_df[cal_df["role"]==2.0]
        sup = cal_df[cal_df["role"]==1.0]
        capital_per_leg = sup["fundingRequestsAmount"].values[0]*2./3
    
        trader_results = pd.DataFrame(index=traders.index,
                                      columns=["actorID", "long_basket", "short_basket", "signal", "basket", "isActive",
                                               "leg_size_long", "leg_size_short", "exit", "signal_coherence",
                                               "return_coherence", "basket_size", "entry_signal_delta", "total_score",
                                               "ranking"])
        
        for ind in traders.index:
            current_row = traders.loc[ind]
            actorID = current_row["actorID"]
            long_basket = map(lambda x:x.symbol, eval(current_row["long_basket"]))
            short_basket = map(lambda x:x.symbol, eval(current_row["short_basket"]))
            basket = long_basket + short_basket
            entry_thresh = current_row["entry_threshold"]
            exit_thresh = current_row["exit_threshold"]
    
            long_prices = price_data[long_basket]
            short_prices = price_data[short_basket]
            
            long_signal_ts = long_prices.apply(stoch_d)
            short_signal_ts = short_prices.apply(stoch_d)
            
            long_indicator = long_signal_ts.iloc[-1,:].mean()
            short_indicator = short_signal_ts.iloc[-1,:].mean()
            signal = long_indicator - short_indicator
            isActive = int(signal<=entry_thresh+entry_tol)
            leg_size_long = isActive*capital_per_leg/len(long_basket)
            leg_size_short = isActive*capital_per_leg/len(short_basket)
            exit = -int(signal>=exit_thresh-exit_tol)
    
            trader_results.loc[ind, "actorID"] = actorID
            trader_results.loc[ind, "long_basket"] = str(dict(zip(long_basket, [leg_size_long]*len(long_basket))))
            trader_results.loc[ind, "short_basket"] = str(dict(zip(short_basket, [leg_size_short]*len(short_basket))))
            trader_results.loc[ind, "basket"] = basket
            trader_results.loc[ind, "signal"] = signal
            trader_results.loc[ind, "isActive"] = isActive
            trader_results.loc[ind, "leg_size_long"] = leg_size_long
            trader_results.loc[ind, "leg_size_short"] = leg_size_short
            trader_results.loc[ind, "exit"] = exit
            
            # Ranking Elements
            basket = list(set(long_basket + short_basket))
            n_names = len(long_basket)
            trader_results.loc[ind, "return_coherence"] = 1.0/self.diversity_score(basket, price_data, lookback_div)
            trader_results.loc[ind, "entry_signal_delta"] = entry_thresh+entry_tol - signal
            trader_results.loc[ind, "basket_size"] = n_names
            if n_names==1:
                trader_results.loc[ind, "signal_coherence"] = 1.0
            else:
                Sigma = long_signal_ts[-lookback_sig:].corr()
                weights = np.ones((1, n_names))
                long_coherence = 1.0/np.matmul(np.matmul(weights, Sigma), weights.T)[0][0]
                Sigma = short_signal_ts[-lookback_sig:].corr()
                short_coherence = 1.0/np.matmul(np.matmul(weights, Sigma), weights.T)[0][0]/n_names
                trader_results.loc[ind, "signal_coherence"] = 0.5*(long_coherence + short_coherence)
    
        trader_results["total_score"] = self.get_score(trader_results, self.weighting)
        trader_results["ranking"] = trader_results["total_score"].rank()
            
        return trader_results
    
    def check_concentration(self, trader_results):
        all_tickers = set()
        for ind in trader_results.index:
            current_row = trader_results.loc[ind]
            longs = eval(current_row["long_basket"]).keys()
            shorts = eval(current_row["short_basket"]).keys()
            all_tickers = all_tickers.union(set(longs)).union(set(shorts))
    
        all_tickers = list(all_tickers)
        all_tickers.sort()
        concentration = pd.DataFrame(index=all_tickers, columns=["capital", "score_sum", "score_count"])
        concentration["capital"] = 0.0
        concentration["score_sum"] = 0.0
        concentration["score_count"] = 0
        for ind in trader_results.index:
            current_row = trader_results.loc[ind]
            longs = eval(current_row["long_basket"])
            for l in longs:
                concentration.loc[l, "capital"] += longs[l]
                concentration.loc[l, "score_sum"] += current_row["total_score"]
                concentration.loc[l, "score_count"] += 1
            shorts = eval(current_row["short_basket"])
            for s in shorts:
                concentration.loc[s, "capital"] -= shorts[s]
                concentration.loc[s, "score_sum"] += current_row["total_score"]
                concentration.loc[s, "score_count"] += 1                
        
        concentration["average_score"] = concentration["score_sum"]/concentration["score_count"]        
        concentration["absolute_capital"] = abs(concentration["capital"])
        concentration["excess"] = concentration["absolute_capital"] - self.singlename_limit
        concentration = concentration.sort_values(["absolute_capital"], ascending=False)
        return concentration
    
    def check_sector_concentration(self, trader_results, security_master, concentration):
        unique_sectors = list(security_master["Sector"].unique())
        unique_sectors.sort()
        results = pd.merge(concentration, security_master[["ticker", "Sector"]], how="left", on=["ticker"])
        sector_concentration = results.groupby("Sector").sum()["capital"].to_frame()
        
        sector_concentration["absolute_capital"] = abs(sector_concentration["capital"])
        sector_concentration["excess"] = sector_concentration["absolute_capital"] - self.sector_limit
        sector_concentration = sector_concentration.sort_values(["absolute_capital"], ascending=False)
        return sector_concentration    
    
    def block_traders(self, cal_df, trader_results, concentration, bad_names):
        concentration["negative_average_score"] = -1.*concentration["average_score"]
        trader_results["over_limit"] = trader_results["basket"].apply(lambda x:len(set(x).intersection(set(bad_names)))>0)
        current_results = trader_results[trader_results["over_limit"]]#.sort_values(["ranking"], ascending=False)
        current_concentration = concentration[concentration["ticker"].isin(bad_names)].reset_index(drop=True)\
            .sort_values(["excess", "average_score"], ascending=False)
        max_violation = current_concentration["excess"].max()
        
        bad_traders = []
        while max_violation>0.0:
            worst_name = current_concentration.loc[0, "ticker"]
            trader_to_remove = current_results[current_results["basket"].apply(lambda x:worst_name in x)]\
                .sort_values(["total_score"]).reset_index(drop=True).loc[0, "actorID"]
            bad_traders.append(trader_to_remove)
            current_results = current_results[~current_results["actorID"].isin(bad_traders)].reset_index(drop=True)
            current_concentration = self.check_concentration(current_results)\
                .sort_values(["excess", "average_score"], ascending=False).reset_index().rename(columns={"index": "ticker"})
            max_violation = current_concentration["excess"].max()
            print("{0}     REMOVED".format(trader_to_remove))
            
        return bad_traders
    
    def do_step_action(self, **kwargs):
        security_master = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        daily_price_data = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        price_data = daily_price_data[["date", "ticker", "close"]]\
            .pivot_table(index="date", values = "close", columns="ticker").sort_index()
        trader_calibration = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy()
        trader_calibration["date"] = trader_calibration["date"].apply(pd.Timestamp)
        trader_calibration["block"] = 0
        
        dates = list(trader_calibration["date"].unique())
        dates = list(map(pd.Timestamp, dates))
        dates.sort()
        
        if self.filter_start_dt:
            dates = list(filter(lambda x:x>=self.filter_start_dt, dates))
        if self.filter_end_dt:
            dates = list(filter(lambda x:x<=self.filter_end_dt, dates))
        
        all_concentrations = []
        all_sector_concentrations = []
        if len(dates):
            for dt in dates:
                cal_df = trader_calibration[trader_calibration["date"]==dt]
                index_orig = cal_df.index
                trader_results = self.check_entries_and_rank(cal_df, price_data)
                concentration = self.check_concentration(trader_results).reset_index().rename(columns={"index": "ticker"})
                sector_concentration = self.check_sector_concentration(trader_results, security_master, concentration)\
                    .reset_index().rename(columns={"index": "Sector"})
                concentration["date"] = dt
                sector_concentration["date"] = dt                
                all_concentrations.append(concentration)
                all_sector_concentrations.append(sector_concentration)
                bad_names = list(concentration.loc[abs(concentration["capital"])>self.singlename_limit,
                                                   "ticker"])
                if len(bad_names):
                    print("{0} **********".format(dt))
                    traders_to_block = self.block_traders(cal_df, trader_results, concentration, bad_names)
                    trader_calibration["block"] = trader_calibration["actorID"]\
                        .apply(lambda x:x in traders_to_block).astype(int)
            
            trader_calibration = trader_calibration[trader_calibration["block"]==0].reset_index(drop=True)
            all_concentrations = pd.concat(all_concentrations, ignore_index=True)
            all_sector_concentrations = pd.concat(all_sector_concentrations, ignore_index=True)
        
        trader_calibration = trader_calibration.drop("block", axis=1)
        self.data = trader_calibration
        self.singlename_concentration = all_concentrations
        self.sector_concentration = all_sector_concentrations
        
        # Only output the end_dt
        mode = self.task_params.run_mode
        calibration_mode = mode is TaskflowPipelineRunMode.Calibration
        if calibration_mode:
            output_df = cal_df[cal_df['date']==self.task_params.end_dt.normalize().date()].reset_index(drop=True)
        else:
            output_df = cal_df
        filename = self.get_file_targets()[0]
        if mode != TaskflowPipelineRunMode.Test:
            store_pandas_object(output_df, filename, "Basket Pairs Calibration")
        
        return StatusType.Success
    
    def get_calibration_dates(self, task_params):
        return self.get_calibration_dates_with_offset_anchor(task_params, self.calibration_date_offset,
                                                             self.calibration_anchor_date)

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.singlename_concentration,
                self.__class__.PROVIDES_FIELDS[2] : self.sector_concentration}

    @classmethod
    def get_default_config(cls):
        return  {"output_dir": "D:/DCM/engine_results/pairs_trading_live_20181218",
                 "file_suffix": "basket_pair_trader_filtered.csv", "ignore_calibration_timeline":True,
                 "calibration_date_offset": "21B", "calibration_anchor_date": None, "entry_tol": 0.0, "exit_tol": 0.0,
                 "filter_start_dt": "2018-03-01", "filter_end_dt": "2018-04-15", "singlename_limit": 250000, 
                 "sector_limit": 10000000, "weighting": None}