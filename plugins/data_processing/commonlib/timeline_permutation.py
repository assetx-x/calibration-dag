from collections import OrderedDict
import pandas as pd
from commonlib.data_requirements import DRDataTypes
from enum import Enum
from commonlib.commonStatTools.ou_calibrate import ou_calibrate, ou_calibrate_jackknife
from commonlib.subjects import Ticker, Index, Future, Option
from commonlib.util_classes import ReturnTypeForPermutation

class BusinessDatePermutatator(object):
    def __init__(self, original_timeline, timeline_segmenter_functions, random_seed=None, anchor_first_date=True,
                 permute_day_at_lowest_segment=True):
        if not isinstance(original_timeline, pd.DatetimeIndex):
            raise ValueError("TimelinePermutatator.__init__ - The parameter original_timeline must be of type "
                             "pandas.DatetimeIndex")
        if not isinstance(timeline_segmenter_functions, OrderedDict):
            raise ValueError("TimelinePermutatator.__init__ - The parameter timeline_segmenter_functions must be an "
                             "OrderedDict, with ordering expressing priority. Received {0}"
                             .format(type(timeline_segmenter_functions)))
        non_functions = [k for k in timeline_segmenter_functions
                         if not hasattr(timeline_segmenter_functions[k], "__call__")]
        if non_functions:
            raise ValueError("TimelinePermutatator.__init__ - all values of timeline_segmenter_functions must be "
                             "callable; these keys are not functions: {0}".format(non_functions))

        if permute_day_at_lowest_segment:
            timeline_segmenter_functions["_default_perm"] = lambda x:pd.np.asarray(xrange(len(x)))
        self.function_hierarchy = timeline_segmenter_functions.keys()
        self.random_seed = random_seed

        self.permutation_frame = pd.DataFrame(index=range(len(original_timeline)))
        self.permutation_frame["original_datetime"] = original_timeline
        self.permutation_frame["target_datetime"] = original_timeline
        for k in timeline_segmenter_functions:
            self.permutation_frame[k] = timeline_segmenter_functions[k](original_timeline)
        self.permutation_frame.sort_values(["original_datetime"], inplace=True)
        if anchor_first_date:
            first_date = self.permutation_frame.iloc[0,:]
            self.permutation_frame = self.permutation_frame.iloc[1:,:]
        self.create_hierarchical_permutation()
        if anchor_first_date:
            self.permutation_frame.loc[first_date.name,:] = first_date
        self.permutation_frame.rename(columns=dict(map(lambda x:(x, "target_{0}".format(x)),
                                                       self.function_hierarchy[0:-1])), inplace=True)
        if permute_day_at_lowest_segment:
            del timeline_segmenter_functions["_default_perm"]
            self.permutation_frame.drop("_default_perm", axis=1, inplace=True)
        self.permutation_frame.sort_values(["original_datetime"], inplace=True)
        for k in timeline_segmenter_functions:
            self.permutation_frame["original_{0}".format(k)] = timeline_segmenter_functions[k](original_timeline)

    def __permute_data_based_on_columns_preserving_order(self, df, column_to_use_for_permutation):
        original_datetimes = df["original_datetime"].copy(deep=True).values
        column_unique_values = df[column_to_use_for_permutation].unique()
        pd.np.random.shuffle(column_unique_values)
        reordered_segments = []
        for k in column_unique_values:
            reordered_segments.append(df[df[column_to_use_for_permutation].isin([k])])
        reordered_segments = pd.concat(reordered_segments)
        reordered_segments["original_datetime"] = original_datetimes
        return reordered_segments

    def get_date_permutation(self):
        return self.permutation_frame[["original_datetime", "target_datetime"]]

    def create_hierarchical_permutation(self):
        current_state = pd.np.random.get_state()
        pd.np.random.set_state(pd.np.random.RandomState(self.random_seed).get_state())
        for hierarchy in range(len(self.function_hierarchy)):
            perm_col = self.function_hierarchy[hierarchy]
            columns_to_groupby = self.function_hierarchy[0:hierarchy]
            if columns_to_groupby:
                new_data = []
                for _, data in self.permutation_frame.groupby(columns_to_groupby):
                    new_data.append(self.__permute_data_based_on_columns_preserving_order(data, perm_col))
                self.permutation_frame = pd.concat(new_data)
            else:
                self.permutation_frame = self.__permute_data_based_on_columns_preserving_order(self.permutation_frame,
                                                                                               perm_col)
        pd.np.random.set_state(current_state)

    def permute_data(self, data, datetime_column, permutation_timezone, datetime_column_timezone=None):
        data_to_permute = data.copy()
        dtimes_to_permute = pd.DatetimeIndex(data_to_permute[datetime_column] if datetime_column
                                             else data_to_permute.index)
        original_tzone = dtimes_to_permute.tzinfo
        effective_tzone = original_tzone or datetime_column_timezone
        if not effective_tzone:
            raise RuntimeError("The datetime column to permute must either be lcoalized, or the parameter "
                               "datetime_column_timezone has to be provided")
        dtimes_to_permute = dtimes_to_permute.tz_localize(datetime_column_timezone) if not original_tzone \
            else dtimes_to_permute
        dtimes_to_permute = dtimes_to_permute.tz_convert(permutation_timezone)
        times_of_day = pd.TimedeltaIndex(dtimes_to_permute.values - dtimes_to_permute.normalize().values)
        data_to_permute["_original_dates_perm_tz"] = dtimes_to_permute.tz_localize(None).normalize()
        data_to_permute["_original_times_perm_tz"] = times_of_day
        permutation_dates = self.get_date_permutation().rename(columns={"original_datetime":"_original_datetime",
                                                                        "target_datetime":"_target_dates_perm_tz"})
        data_to_permute = pd.merge(data_to_permute, permutation_dates, left_on=["_original_dates_perm_tz"],
                                   right_on=["_original_datetime"], how="left")
        new_dtimes = pd.DatetimeIndex(data_to_permute["_target_dates_perm_tz"]) \
            + pd.TimedeltaIndex(data_to_permute["_original_times_perm_tz"])
        new_dtimes = new_dtimes.tz_localize(permutation_timezone).tz_convert(effective_tzone)
        data_to_permute.drop(["_original_dates_perm_tz", "_original_times_perm_tz", "_original_datetime",
                              "_target_dates_perm_tz"], axis=1, inplace=True)
        # converting the new_dtimes here before sorting has a bug -- the time associated with tz_localize(None) is wrong
        # for some reason. Need to insert first, sort, and the de-localize if need be
        if datetime_column:
            data_to_permute[datetime_column] = new_dtimes
            data_to_permute.sort_values(datetime_column, inplace=True)
            if not original_tzone:
                data_to_permute[datetime_column] = pd.DatetimeIndex(data_to_permute[datetime_column]).tz_localize(None)
        else:
            data_to_permute.index = new_dtimes
            data_to_permute.sort_index(inplace=True)
            if not original_tzone:
                data_to_permute.index = data_to_permute.index.tz_localize(None)
        return data_to_permute

class MarketDataPermutator(object):
    def __init__(self, business_day_permutator):
        self.business_day_permutator = business_day_permutator
        #TODO: this is a horrible hack
        self.calibration_method = "mle"
        self.delta_t = 1./252.
        #TODO: these should be estimated on the fly
        self.ou_params = {ReturnTypeForPermutation.LogOU_VIX: (9.321, 2.74, 1.246),
                          ReturnTypeForPermutation.LogOU_VXST: (16.87, 2.79, 0.913),
                          ReturnTypeForPermutation.LogOU_VX1: (6.562, 2.79, 0.901),
                          ReturnTypeForPermutation.LogOU_VX2: (3.9788, 2.846, 0.6215)}
        #TODO: undo the clipping
        self.clipping_params = {ReturnTypeForPermutation.LogReturn: (-0.0297, 0.0278),
                                ReturnTypeForPermutation.LogOU_VIX: (-0.1321, 0.1694),
                                ReturnTypeForPermutation.LogOU_VXST: (-0.2014, 0.257),
                                ReturnTypeForPermutation.LogOU_VX1: (-0.0948, 0.1384),
                                ReturnTypeForPermutation.LogOU_VX2: (-0.0714, 0.0936)}
        self.clipping_params = {}


    def __compute_return(self, ts, return_type):
        if return_type is ReturnTypeForPermutation.LogReturn:
            return_data = (pd.np.log(ts) - pd.np.log(ts.shift())).fillna(0)
        elif return_type is ReturnTypeForPermutation.AbsReturn:
            return_data = (ts - ts.shift()).fillna(0)
        elif return_type is ReturnTypeForPermutation.Level:
            return_data = ts
        elif return_type in [ReturnTypeForPermutation.LogOU_VIX, ReturnTypeForPermutation.LogOU_VXST,
                             ReturnTypeForPermutation.LogOU_VX1, ReturnTypeForPermutation.LogOU_VX2]:
            return_data = self.__compute_OU_residual(pd.np.log(ts), return_type)
        if return_type in self.clipping_params:
            lb = self.clipping_params[return_type][0]
            ub = self.clipping_params[return_type][1]
            return_data = return_data.clip(lb, ub)
        return return_data

    def __compute_level(self, ts, return_type, initial_level):
        if return_type is ReturnTypeForPermutation.LogReturn:
            level_data = pd.np.exp(ts.cumsum())*initial_level
        elif return_type is ReturnTypeForPermutation.AbsReturn:
            level_data = ts.cumsum() + initial_level
        elif return_type is ReturnTypeForPermutation.Level:
            level_data = ts
        elif return_type in [ReturnTypeForPermutation.LogOU_VIX, ReturnTypeForPermutation.LogOU_VXST,
                             ReturnTypeForPermutation.LogOU_VX1, ReturnTypeForPermutation.LogOU_VX2]:
            level_data = self.__compute_OU_level(ts, return_type, initial_level)
        return level_data

    def __compute_OU_residual(self, ts, return_type):
        ts = ts.fillna(method="ffill")
        kappa, theta, sigma = self.ou_params[return_type]
        # Conversion back to regression coefficients
        b = pd.np.exp(-kappa*self.delta_t)
        a = (1-b)*theta
        resid = (ts - a - b*ts.shift()).fillna(0.0)
        return resid

    def __compute_OU_level(self, ts, return_type, initial_level):
        kappa, theta, sigma = self.ou_params[return_type]
        initial_level = pd.np.log(initial_level) if pd.notnull(initial_level) else theta
        # Conversion back to regression coefficients
        b = pd.np.exp(-kappa*self.delta_t)
        a = (1-b)*theta
        log_level_data = pd.Series(index=ts.index)
        for ind in range(len(log_level_data)):
            dt = log_level_data.index[ind]
            log_level_data[dt] = initial_level if (ind==0) else \
                a + b*log_level_data[log_level_data.index[ind-1]] + ts[dt]
        return pd.np.exp(log_level_data)

    def get_return(self, original_df, datetime_col, return_mapping):
        df = original_df.copy()
        original_index_name=None
        if datetime_col:
            original_index_name = df.index.name or "index"
            df = df.reset_index().set_index(datetime_col)
        sorted_data = df.sort_index()
        return_data = pd.DataFrame(index=sorted_data.index)
        for k in return_mapping:
            return_data[k] = self.__compute_return(sorted_data[k], return_mapping[k])
        for k in original_df.columns:
            if k not in return_data.columns and k!=datetime_col:
                return_data[k] = df[k]
        if datetime_col:
            return_data[original_index_name] = sorted_data[original_index_name]
            return_data = return_data.reset_index().set_index(original_index_name)
        return_data = return_data[original_df.columns]
        #print "Processed {0}".format(original_df["ticker"].unique())
        return return_data

    def reconstruct_levels(self, original_df, datetime_col, return_mapping, initial_level):
        df = original_df.copy()
        original_index_name=None
        if datetime_col:
            original_index_name = df.index.name or "index"
            df = df.reset_index().set_index(datetime_col)
        sorted_data = df.sort_index()
        level_data = pd.DataFrame(index=sorted_data.index)
        for k in return_mapping:
            level_data[k] = self.__compute_level(sorted_data[k], return_mapping[k], initial_level[k])
        for k in original_df.columns:
            if k not in level_data.columns and k!=datetime_col:
                level_data[k] = df[k]
        if datetime_col:
            level_data[original_index_name] = sorted_data[original_index_name]
            level_data = level_data.reset_index().set_index(original_index_name)
        level_data = level_data[original_df.columns]
        #print "Processed {0}".format(original_df["ticker"].unique())
        return level_data

    def get_initial_levels(self, original_df, datetime_col):
        df = original_df.copy()
        original_index_name=None
        if datetime_col:
            original_index_name = df.index.name or "index"
            df = df.reset_index().set_index(datetime_col)
        sorted_data = df.sort_index()
        initial_levels = sorted_data.iloc[0,:]
        if datetime_col:
            initial_levels = initial_levels.reset_index().set_index(original_index_name)
        return initial_levels

    def permute_data(self, data, return_mapping, datetime_column, permutation_timezone,
                     groupby_columns=[], datetime_column_timezone=None):
        unknown_columns = [k for k in data.columns if k not in return_mapping and k not in groupby_columns
                           and k!=datetime_column]
        if unknown_columns:
            raise ValueError("The mapping does not specify how to operate on columns {0}".format(unknown_columns))
        if groupby_columns:
            data_groups = data.groupby(groupby_columns)
            returns_data = data_groups.apply(lambda x:self.get_return(x, datetime_column, return_mapping)).reset_index(level=0, drop=True)
            initial_levels = data_groups.apply(lambda x:self.get_initial_levels(x, datetime_column))
            permuted_data = self.business_day_permutator.permute_data(returns_data, datetime_column, permutation_timezone,
                                                                  datetime_column_timezone)
            reconstructed_levels = permuted_data.groupby(groupby_columns).apply(lambda x:self.reconstruct_levels(x, datetime_column, return_mapping,
                                                                                                             initial_levels.loc[x.name,:].iloc[:,0]))
        else:
            returns_data = self.get_return(data, datetime_column, return_mapping)
            initial_levels = self.get_initial_levels(data, datetime_column)
            permuted_data = self.business_day_permutator.permute_data(returns_data, datetime_column, permutation_timezone,
                                                                              datetime_column_timezone)
            reconstructed_levels = self.reconstruct_levels(permuted_data, datetime_column,return_mapping,
                                                        initial_levels)

        return reconstructed_levels

def get_return_types(data_type, fields):
    DEFAULT_DATATYPE_TO_RETURN = {DRDataTypes.Equities: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.DailyEquities: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.DailyFutures: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.DailyIndices: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.Indices: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.Sentiment: ReturnTypeForPermutation.AbsReturn}

    DEFAULT_RETURN_EXCEPTIONS = {DRDataTypes.Equities: {ReturnTypeForPermutation.Level: ["volume"]},
                                 DRDataTypes.DailyEquities: {ReturnTypeForPermutation.Level: ["volume"]},
                                 DRDataTypes.DailyFutures: {ReturnTypeForPermutation.Level: ["volume"]}
                                 }
    data_type_default_return_type = DEFAULT_DATATYPE_TO_RETURN[data_type]


    return_types = {}
    for column in fields:
        if column == "volume":
            return_types.update({column : ReturnTypeForPermutation.Level})
        else:
            return_types.update({column : data_type_default_return_type})

    return return_types

def get_return_types_special(data_type, fields, replacement_dict):
    DEFAULT_DATATYPE_TO_RETURN = {DRDataTypes.Equities: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.DailyEquities: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.DailyFutures: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.DailyIndices: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.Indices: ReturnTypeForPermutation.LogReturn,
                                  DRDataTypes.Sentiment: ReturnTypeForPermutation.AbsReturn}

    DEFAULT_DATATYPE_TO_RETURN.update(replacement_dict)

    DEFAULT_RETURN_EXCEPTIONS = {DRDataTypes.Equities: {ReturnTypeForPermutation.Level: ["volume"]},
                                 DRDataTypes.DailyEquities: {ReturnTypeForPermutation.Level: ["volume"]},
                                 DRDataTypes.DailyFutures: {ReturnTypeForPermutation.Level: ["volume"]}
                                 }
    data_type_default_return_type = DEFAULT_DATATYPE_TO_RETURN[data_type]


    return_types = {}
    for column in fields:
        if column == "volume":
            return_types.update({column : ReturnTypeForPermutation.Level})
        else:
            return_types.update({column : data_type_default_return_type})

    return return_types


def permute_market(market, mkt_permutator):
    for data_type in market.data_groups.keys():
        for subject in market.data_groups[data_type].sub_groups.keys():
            price_data = market.data_groups[data_type].sub_groups[subject]
            price_data.index = price_data.index.astype("M8[ns]")
            return_types = get_return_types(data_type, price_data.columns)

            permuted_mkt_data = mkt_permutator.permute_data(price_data, return_types, None,
                                                            "US/Eastern", [], "UTC")
            permuted_mkt_data.index = permuted_mkt_data.index.asi8.astype(long)
            market.data_groups[data_type].sub_groups[subject] = permuted_mkt_data
    return market

def _fill_data_columns_using_close(row, reference_column, target_columns):
    for col in target_columns:
        row[col] = row[reference_column] if pd.isnull(row[col]) else row[col]
    return row

def permute_market_special(market, mkt_permutator, fill_missing_index_cols=True):
    special_instruments = [Index("VIX"), Index("VXST"), Future("VX1"), Future("VX2")]
    reference_column = "close"
    target_columns = ["high", "low", "open"]

    for data_type in market.data_groups.keys():
        for subject in market.data_groups[data_type].sub_groups.keys():
            price_data = market.data_groups[data_type].sub_groups[subject]
            price_data.index = price_data.index.astype("M8[ns]")
            if fill_missing_index_cols and (data_type in [DRDataTypes.Indices, DRDataTypes.DailyIndices]):
                price_data = price_data.apply(lambda x:_fill_data_columns_using_close(x, reference_column,
                                                                                      target_columns), axis=1)
            if subject in special_instruments:
                if subject==special_instruments[0]:
                    replacement_dict = {data_type: ReturnTypeForPermutation.LogOU_VIX}
                elif subject==special_instruments[1]:
                    replacement_dict = {data_type: ReturnTypeForPermutation.LogOU_VXST}
                elif subject==special_instruments[2]:
                    replacement_dict = {data_type: ReturnTypeForPermutation.LogOU_VX1}
                elif subject==special_instruments[3]:
                    replacement_dict = {data_type: ReturnTypeForPermutation.LogOU_VX2}
                return_types = get_return_types_special(data_type, price_data.columns, replacement_dict)
            else:
                return_types = get_return_types(data_type, price_data.columns)
            permuted_mkt_data = mkt_permutator.permute_data(price_data, return_types, None,
                                                            "US/Eastern", [], "UTC")
            permuted_mkt_data.index = permuted_mkt_data.index.asi8.astype(long)
            market.data_groups[data_type].sub_groups[subject] = permuted_mkt_data
    return market

def pre_process_index_data(market):
    special_instruments = [Index("VIX"), Index("VXST")]
    for data_type in market.data_groups.keys():
        for subject in market.data_groups[data_type].sub_groups.keys():
            price_data = market.data_groups[data_type].sub_groups[subject]
            if subject in special_instruments:
                if subject==special_instruments[0]:
                    replacement_dict = {data_type: ReturnTypeForPermutation.LogOU_VIX}
                elif subject==special_instruments[1]:
                    replacement_dict = {data_type: ReturnTypeForPermutation.LogOU_VXST}
                elif subject==special_instruments[2]:
                    replacement_dict = {data_type: ReturnTypeForPermutation.LogOU_VX1}
                elif subject==special_instruments[3]:
                    replacement_dict = {data_type: ReturnTypeForPermutation.LogOU_VX2}
                return_types = get_return_types_special(data_type, price_data.columns, replacement_dict)
            else:
                return_types = get_return_types(data_type, price_data.columns)
            permuted_mkt_data = mkt_permutator.permute_data(price_data, return_types, None,
                                                            "US/Eastern", [], "UTC")
            permuted_mkt_data.index = permuted_mkt_data.index.asi8.astype(long)
            market.data_groups[data_type].sub_groups[subject] = permuted_mkt_data
    return market

def main():
    import sys
    import os
    sys.path.append(os.path.join("..", "pipelines", "market_view_calibration"))
    from market_view_analysis_input_generator import read_taskflow_engine_snapshot

    location = r"C:\DCM\vistools\Data\market_view_analysis_input_generator.h5"
    daily_price_data = read_taskflow_engine_snapshot(location, False, ["daily_price_data"])["daily_price_data"]
    daily_price_data.drop("this_quarter", axis=1, inplace=True)
    daily_price_data.dropna(how="any", inplace=True)

    timeline_to_permute = pd.DatetimeIndex(daily_price_data["date"].unique()).sort_values()
    dates_per_ticker = daily_price_data.groupby("ticker")["ticker"].agg(lambda x:len(x))
    valid_tickers = dates_per_ticker.index[dates_per_ticker==len(timeline_to_permute)][0:20]
    daily_price_data = daily_price_data[daily_price_data["ticker"].isin(valid_tickers)]

    time_segmenters = OrderedDict()
    time_segmenters["year"] = lambda x:x.year
    time_segmenters["month"] = lambda x:x.month
    time_segmenters["week"] = lambda x:x.week
    bd_permutator = BusinessDatePermutatator(timeline_to_permute, time_segmenters, 100)

    mkt_permutator = MarketDataPermutator(bd_permutator)
    return_types = {"close":ReturnTypeForPermutation.LogReturn,
                    "high":ReturnTypeForPermutation.LogReturn,
                    "low":ReturnTypeForPermutation.LogReturn,
                    "open":ReturnTypeForPermutation.LogReturn,
                    "volume":ReturnTypeForPermutation.Level}
    permuted_mkt_data = mkt_permutator.permute_data(daily_price_data, return_types, "date",
                                                    "US/Eastern", ["ticker"], "US/Eastern")

    print("Done")


def main_special():
    import os, sys
    sys.path = sys.path[2:]

    from quick_simulator import QuickSimulatorMarketStoreHandler, QuickSimulationMarket
    from commonlib.subjects import Ticker
    import random
    h5_file_location = r"C:\Temp\market\market_new_vol_2011_2016.h5"
    h5_file_location = r"C:\Temp\market\market_new_vol_2011_2016_svxy.h5"
    market_data_loader = QuickSimulatorMarketStoreHandler(h5_file_location, None)
    market = QuickSimulationMarket(None, market_data_loader, None)
    org_market = QuickSimulationMarket(None, market_data_loader, None)

    root_seed = 20180802
    random.seed(root_seed)
    MAX = 10000
    num_runs = 5000
    seeds = random.sample(range(MAX), num_runs)
    seed = seeds[1278]

    timeline_to_permute = market.data_groups[DRDataTypes.Equities].sub_groups[Ticker("SPY")].index.astype("M8[ns]").normalize().unique().sort_values()
    time_segmenters = OrderedDict()
    time_segmenters["year"] = lambda x:x.year
    time_segmenters["month"] = lambda x:x.month
    time_segmenters["week"] = lambda x:x.week
    bd_permutator = BusinessDatePermutatator(timeline_to_permute, time_segmenters, random_seed=seed,
                                             permute_day_at_lowest_segment=False)

    mkt_permutator = MarketDataPermutator(bd_permutator)
    market = permute_market_special(market, mkt_permutator)

    print("done")


if __name__=="__main__":
    import os
    main_special()
    print("done")


    '''
    data_dir = r"C:\DCM\temp\h5Files"
    h5_file_location = os.path.join(data_dir, "market_view_5_regimes_full_out_of_sample.h5")
    market = QuickSimulationMarket(h5_file_location, remove_duplicates_by_date=True)
    org_market = QuickSimulationMarket(h5_file_location, remove_duplicates_by_date=True)

    timeline_to_permute = market.data_groups[DRDataTypes.Equities].sub_groups[Ticker("SPY")].index.astype("M8[ns]").normalize().unique().sort_values()
    time_segmenters = OrderedDict()
    time_segmenters["year"] = lambda x:x.year
    time_segmenters["month"] = lambda x:x.month
    time_segmenters["week"] = lambda x:x.week
    bd_permutator = BusinessDatePermutatator(timeline_to_permute, time_segmenters, 100)

    mkt_permutator = MarketDataPermutator(bd_permutator)

    market = permute_market(market, mkt_permutator)


    print " Done"
    '''


