import tempfile
import os
from glob import glob
from abc import abstractmethod

import pandas as pd

from commonlib.data_requirements import LookbackField, DataRequirementsCollection, DRDataTypes
from commonlib.util_functions import load_module_by_full_path, load_obj_from_module, get_max_of_pandas_frequency
from commonlib.market_timeline import marketTimeline
from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask
from quick_simulator_market_builder import expand_daily_to_minute_data
from status_objects import StatusType
from calibrations.common.config_functions import get_config
from calibrations.common.calibrations_utils import store_pandas_object
from calibrations.common.calibration_logging import calibration_logger
from calibrations.common.taskflow_params import TaskflowPipelineRunMode
from calibrations.core.core_classes import CalibrationDates

INTUITION_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../"))

class MarketViewInstaceHandler(object):
    def __init__(self, view_module_locations, quick_simulator_module_location, time_in_day_to_evaluate_signal,
                 views_definition,
                 quick_simulator_class_name = "create_market_from_data_requirements",
                 views_filter=None, market_start_date=pd.Timestamp("2010-01-15"),
                 market_end_date=pd.Timestamp("2016-12-31")):
        views_definition = views_definition
        if isinstance(view_module_locations, str):
            view_module_locations = [view_module_locations]
        self.market_output_file = os.path.join(tempfile.mkdtemp(), self.__class__.__name__+".h5")

        view_modules = []
        for p in view_module_locations:
            view_modules.append(load_module_by_full_path(p))
        self.view_instances = self.__create_view_instances(view_modules, views_definition, views_filter)

        self.qs_class = load_obj_from_module(quick_simulator_module_location, quick_simulator_class_name)
        if not self.qs_class:
            raise RuntimeError("The file {0} does not contain a quick_simulator class named {1} "
                               .format(quick_simulator_module_location, quick_simulator_class_name))
        self.time_in_day_to_evaluate_signal = time_in_day_to_evaluate_signal
        self.individual_signal_intervals = {}
        self.market_start_date = pd.Timestamp(market_start_date)
        self.market_end_date = pd.Timestamp(market_end_date)

    def __create_view_instances(self, view_modules, views_to_instantiate, views_filter):
        #view_instances = {}
        #views_filter = views_filter or views_to_instantiate.keys()
        #views_filter = list(set(views_filter).intersection(set(views_to_instantiate.keys())))
        #for view in views_filter:
        #    class_name = views_to_instantiate[view]["view_signal_class_name"]
        #    class_params = views_to_instantiate[view]["view_function_params"]
        #    class_definition = [getattr(mdl, class_name,None) for mdl in view_modules if hasattr(mdl, class_name)]
        #    if not class_definition:
        #        raise ValueError("The View {0} of type {1} cannot be located on any of the provided modules"
        #                         .format(view, class_params))
        #    if len(class_definition)>1:
        #        raise ValueError("The class {0} is located on 2 different modules")
        #    class_definition = class_definition[0]
        #    view_instances[view] = class_definition(**class_params)
        #return view_instances
        return None

    def __prepare_interval_data(self):
        intervals =pd.concat([self.individual_signal_intervals[view] for view in self.individual_signal_intervals])
        return intervals

    def _get_all_views_requirements(self):
        views_requirements = []
        all_lookbacks = []
        for view in self.view_instances.values():
            view_reqs = view._get_data_requirements_aux()
            requirements = view_reqs.get_all_requirements()
            all_lookbacks.extend([requirements[k].as_raw_dict()[LookbackField] for k in range(len(requirements))])
            view_reqs.update_all_requirements_with_dict({LookbackField:None})
            view_reqs.update_all_requirements_with_dict({"mkt_data_req_topic": "dummy_topic"})
            views_requirements.append(view_reqs)
        full_requirements = reduce(lambda a,b:a+b, views_requirements, DataRequirementsCollection())
        full_requirements.reduce_requirements()
        return full_requirements, all_lookbacks

    def __calculate_periods(self, view, signal_values_original):
        signal_values = pd.np.sign(signal_values_original)
        periods_starts = signal_values.index[signal_values.diff().fillna(value=0) != 0].insert(0, signal_values.index[0])
        return_summary = signal_values[periods_starts].to_frame(name="signal_value")
        return_summary.index.name = "entry_time"
        return_summary.reset_index(inplace=True)
        return_summary["exit_time"] = return_summary["entry_time"].shift(-1).fillna(signal_values.index[-1])
        return_summary["view_name"] = view
        return_summary["signal_value"] = signal_values_original.reindex(return_summary["entry_time"]).values
        return_summary["signal_asset"] = return_summary["signal_value"].apply(lambda x:"Long" if x>0
                                                                              else ("Short" if x<0 else ""))
        return return_summary

    def __setup_market_object(self):
        timeline = [self.market_start_date, self.market_end_date]

        data_requirement, all_lookbacks = self._get_all_views_requirements()
        daily_to_minute_func = lambda x: expand_daily_to_minute_data(x, [0,1,2,3,387,388,389,390])

        market = self.qs_class(self.market_output_file, data_requirements_collection = data_requirement,
                                   force_data_retrieval=True, timeline=timeline,
                                   post_retrieval_transformation={DRDataTypes.Indices:daily_to_minute_func})
        return (data_requirement, all_lookbacks, market)

    def __build_signal_timeline(self, all_lookbacks):
        time_in_day = self.time_in_day_to_evaluate_signal
        time_delta  = get_max_of_pandas_frequency(all_lookbacks,self.market_start_date,False)
        signal_timeline = marketTimeline.get_trading_days(self.market_start_date + pd.tseries.frequencies.to_offset(time_delta),
                                                          pd.Timestamp(self.market_end_date)) + pd.Timedelta(time_in_day)
        signal_timeline = signal_timeline.tz_localize("US/Eastern")
        return signal_timeline

    def run_signals_with_details(self):
        _, all_lookbacks, market = self.__setup_market_object(timeline)
        signal_timeline = self.__build_signal_timeline(all_lookbacks)

        all_data = []
        for view in self.view_instances:
            signal_values = market.apply_function(lambda x:self.view_instances[view].get_signal_details(market=x, revaluate_signal=True), signal_timeline, reduce=True)
            views_periods = self.__calculate_periods(view, signal_values)
            self.individual_signal_intervals[view] = views_periods
            data = pd.DataFrame.from_records(signal_values.values).set_index("current_date").rename(columns={col : col+"_"+ view for col in ["signal", "signal_value"]})
            all_data.append(data)
        data = pd.concat(all_data, axis=1)
        # hacky way to remove duplicated columns, not cool
        data = data.T.groupby(level=0).first().T
        good_columns = [col for col in list(data.columns) if "self" not in col]
        return data[good_columns].apply(pd.to_numeric, errors='ignore').reset_index()

    def run_signals(self):
        _, all_lookbacks, market = self.__setup_market_object()
        signal_timeline = self.__build_signal_timeline(all_lookbacks)

        for view in self.view_instances:
            signal_values = market.apply_function(self.view_instances[view].evaluate_signal, signal_timeline, reduce=True)
            views_periods = self.__calculate_periods(view, signal_values)
            self.individual_signal_intervals[view] = views_periods
        intervals = self.__prepare_interval_data()
        return intervals


class CalibrationDatesMarketViews(CalibrationDates):
    PROVIDES_FIELDS = ["intervals_data", "regime_data"]

    @classmethod
    def default_spy_view(cls):
        return {
          "spy_view": {
            "view_signal_class_name": "ViewInMarketSignal_RiskOnOff5Regimes",
            "wealth_percentage": 1.0,
            "jump_parameters_for_filtering_per_view_value": {
              "1": [
                -0.015,
                -0.002
              ],
              "2": [
                -0.015,
                -0.002
              ]
            },
            "view_assets": "market_assets",
            "view_function_params": {
              "numerator_security": "VIX",
              "epsilon": 0.015,
              "ivts_median_thresh": 1.12,
              "denominator_security": "VXST",
              "look_back_for_mean_in_days": 10
            }
          }
        }



    def __init__(self, intervals_view_filter=None, cache_file="../configuration/intervals_as_of_20181218.csv",
                 signal_asset_ticker_map=None, target_dt=None, force_recalculation=False, save_intervals_to=None,
                 view_module_locations= "views_in_market_trading.py",
                 quick_simulator_module_location="quick_simulator_market_builder.py",
                 quick_simulator_class_name="create_market_from_data_requirements",
                 time_in_day_to_evaluate_signal="09:31:00", views_definition=None,
                 views_filter=None, market_start_date=pd.Timestamp("2010-01-15"),
                 market_end_date=pd.Timestamp("2018-12-31")):
        self.intervals_view_filter = intervals_view_filter or ["spy_view"]
        self.signal_asset_ticker_map = signal_asset_ticker_map or {"spy_view": {"Long": "SPY", "Short": "TLT"}}
        self.target_dt = pd.Timestamp(target_dt) if target_dt else None

        self.data = None
        self.intervals_data = None
        read_csv_kwargs = dict(index_col= 0, parse_dates=["entry_time", "exit_time"])
        
        view_base_dir = os.path.abspath(os.path.join(INTUITION_DIR, "strategies/cointegration/"))
        quick_sim_base_dir = os.path.abspath(os.path.join(INTUITION_DIR, "toolsets/quick_simulator/"))

        views_definition = views_definition or self.default_spy_view()
        if isinstance(view_module_locations, str):
            view_module_locations = [view_module_locations]
        view_module_locations = list(map(lambda x:os.path.abspath(os.path.join(view_base_dir, x)), view_module_locations))
        quick_simulator_module_location = os.path.abspath(os.path.join(view_base_dir, quick_simulator_module_location))

        self.view_instances_params = (view_module_locations, quick_simulator_module_location,
                                      time_in_day_to_evaluate_signal, views_definition, quick_simulator_class_name,
                                      views_filter, pd.Timestamp(market_start_date), pd.Timestamp(market_end_date))

        self.view_instances = None
        CalibrationDates.__init__(self, cache_file, read_csv_kwargs, force_recalculation, save_intervals_to)

    def _split_interval_regime_data(self, data):
        data.rename(columns={"entry_time": "entry_date", "exit_time": "exit_date"}, inplace=True)
        data['entry_date'] = pd.DatetimeIndex(data['entry_date']).normalize()
        data['exit_date'] = pd.DatetimeIndex(data['exit_date']).normalize()
        data = data.sort_values(['view_name', 'entry_date']).reset_index(drop=True)
        data["signal_asset"].fillna(value=" ", inplace=True)
        map_func = lambda x:self.signal_asset_ticker_map[x['view_name']].get(x['signal_asset'], pd.np.NaN)
        data['signal_ticker'] = data[['view_name', 'signal_asset']].apply(map_func, axis=1)
        data["signal_asset"].replace({" ":pd.np.NaN}, inplace=True)
        intervals_data = data[data['view_name'].isin(self.intervals_view_filter)].reset_index(drop=True) \
            if self.intervals_view_filter else data.copy()
        quarter_func = lambda x:str(x-pd.tseries.frequencies.QuarterEnd(normalize=True)\
                                    +pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        intervals_data['quarter_date'] = \
            intervals_data['entry_date'].apply(quarter_func)
        intervals_data["this_quarter"] = intervals_data["entry_date"].apply(lambda x: str(x.year) +"-Q"+str((x.month-1)/3+1))
        self.intervals_data = intervals_data
        return data

    def calculate_intervals(self):
        self.view_instances = MarketViewInstaceHandler(*self.view_instances_params)
        intervals = self.view_instances.run_signals()
        intervals["entry_time"] = pd.DatetimeIndex(intervals["entry_time"]).tz_localize(None)
        intervals["exit_time"] = pd.DatetimeIndex(intervals["exit_time"]).tz_localize(None)
        return intervals

    def calculate_intervals_for_calibration(self):
        self.target_dt = self.target_dt or self.task_params.end_dt.normalize()
        quarter_end = str(self.target_dt-pd.tseries.frequencies.QuarterEnd(normalize=True)\
                          +pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        entry_date = self.target_dt.normalize()
        data = [[entry_date, entry_date, "", quarter_end]]
        intervals = pd.DataFrame(data, columns=['entry_date', 'exit_date', 'signal_ticker', 'quarter_date'])
        return intervals

    def post_process_intervals(self, intervals):
        calibration_mode = self.task_params.run_mode is TaskflowPipelineRunMode.Calibration
        if calibration_mode:
            intervals["this_quarter"] = intervals["entry_date"].apply(lambda x: str(x.year) +"-Q"+str((x.month-1)/3+1))
            self.intervals_data = intervals
            return intervals
        else:
            new_intervals = self._split_interval_regime_data(intervals)
            new_intervals["this_quarter"] = new_intervals["entry_date"].apply(lambda x: str(x.year) +"-Q"+str((x.month-1)/3+1))
            return new_intervals

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.intervals_data,
                self.__class__.PROVIDES_FIELDS[1] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"intervals_view_filter": ["spy_view"], "cache_file": "../configuration/intervals_as_of_20181218.csv",
                "signal_asset_ticker_map": {"spy_view": {"Long": "SPY", "Short": "TLT"}},
                "target_dt": pd.Timestamp("2018-01-05"), "force_recalculation": False, "save_intervals_to": None,
                "view_module_locations": "views_in_market_trading.py",
                "quick_simulator_module_location": "quick_simulator_market_builder.py",        
                "quick_simulator_class_name": "create_market_from_data_requirements",
                "views_definition": cls.default_spy_view(),
                "time_in_day_to_evaluate_signal": "09:31:00", "market_start_date": pd.Timestamp("2010-01-15"),
                "market_end_date": pd.Timestamp("2018-12-31"), "views_filter": None}

class CreateMarketViewsDerivedData(CalibrationTaskflowTask):
    '''

    Creates a dataframe summarizing details recorded inside a view of market views

    '''
    PROVIDES_FIELDS = ["market_views_signal_details"]

    def __init__(self, views_definition,
                 view_module_locations= "views_in_market_trading.py",
                 quick_simulator_module_location="quick_simulator_market_builder.py",
                 quick_simulator_class_name="create_market_from_data_requirements",
                 time_in_day_to_evaluate_signal="09:31:00",
                 views_filter=None, market_start_date=pd.Timestamp("2010-01-15"),
                 market_end_date=pd.Timestamp("2018-12-31")):
        self.data = None
        self.intervals_data = None

        view_base_dir = os.path.abspath(os.path.join(INTUITION_DIR, "strategies/cointegration/"))
        quick_sim_base_dir = os.path.abspath(os.path.join(INTUITION_DIR, "toolsets/quick_simulator/"))

        views_definition = views_definition or self.default_spy_view()
        if isinstance(view_module_locations, str):
            view_module_locations = [view_module_locations]
        view_module_locations = list(map(lambda x:os.path.abspath(os.path.join(view_base_dir, x)), view_module_locations))
        quick_simulator_module_location = os.path.abspath(os.path.join(view_base_dir, quick_simulator_module_location))

        self.view_instances = MarketViewInstaceHandler(view_module_locations, quick_simulator_module_location,
                                                       time_in_day_to_evaluate_signal, views_definition,
                                                       quick_simulator_class_name, views_filter, market_start_date,
                                                       market_end_date)


        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self , **kwargs):
        self.data = self.view_instances.run_signals_with_details()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"view_module_locations": "views_in_market_trading.py",
                "quick_simulator_module_location": "quick_simulator_market_builder.py",
                "quick_simulator_class_name": "create_market_from_data_requirements",
                "views_definition": CalibrationDatesMarketViews.default_spy_view(),
                "time_in_day_to_evaluate_signal": "09:31:00", "market_start_date": pd.Timestamp("2010-01-15"),
                "market_end_date": pd.Timestamp("2018-12-31"), "views_filter": None}


class CalibrationDatesBasketPairs(CalibrationDates):
    PROVIDES_FIELDS = ["calibration_dates", "lasso_calculate_dates"]

    def __init__(self, cache_file, target_dt=None, intervals_start_dt=None, intervals_end_dt=None, force_recalculation=False,
                 save_intervals_to=None):
        self.data = None
        self.lasso_dates = None
        target_dt = pd.Timestamp(target_dt) if target_dt else None
        intervals_start_dt = pd.Timestamp(intervals_start_dt) if intervals_start_dt else None
        intervals_end_dt = pd.Timestamp(intervals_end_dt) if intervals_end_dt else None
        self.target_dt = target_dt
        self.intervals_start_dt = intervals_start_dt
        self.intervals_end_dt = intervals_end_dt
        cache_file = cache_file or "../configuration/calibration_dates_compare.csv"
        read_csv_kwargs = dict(index_col= 0, parse_dates=["date"])
        CalibrationDates.__init__(self, cache_file, read_csv_kwargs, force_recalculation, save_intervals_to)


    def calculate_intervals(self):
        # By default perform other calibrations on every trading day but lasso only on BMS
        self.intervals_start_dt = self.intervals_start_dt or self.task_params.start_dt.normalize()
        self.intervals_end_dt = self.intervals_end_dt or self.task_params.end_dt.normalize()
        trading_days = marketTimeline.get_trading_days(self.intervals_start_dt, self.intervals_end_dt)
        bms_days = trading_days[pd.Series(trading_days.month).diff()!=0]
        calibration = pd.DataFrame(index=range(len(trading_days)), columns=['date', 'regime'])
        calibration['date'] = trading_days
        calibration['regime'] = 0
        lasso = pd.DataFrame(index=range(len(bms_days)), columns=['date', 'regime'])
        lasso['date'] = list(bms_days)
        lasso['regime'] = 1
        intervals = pd.concat([calibration, lasso], ignore_index=True)
        return intervals

    def calculate_intervals_for_calibration(self):
        self.target_dt = self.target_dt or self.task_params.end_dt.normalize()
        trading_days = marketTimeline.get_trading_days(self.target_dt.normalize()-pd.tseries.frequencies.to_offset("5B"),
                                                       self.target_dt.normalize()).normalize()
        lasso_days = trading_days[1:]
        calibration = pd.DataFrame(index=range(len(trading_days)), columns=['date', 'regime'])
        calibration['date'] = trading_days
        calibration['regime'] = 0
        lasso = pd.DataFrame(index=range(len(lasso_days)), columns=['date', 'regime'])
        lasso['date'] = list(lasso_days)
        lasso['regime'] = 1
        intervals = pd.concat([calibration, lasso], ignore_index=True)
        return intervals

    def post_process_intervals(self, intervals):
        data = intervals[intervals["regime"]==0].reset_index(drop=True)
        self.lasso_dates = intervals[intervals["regime"]==1].reset_index(drop=True)
        return data

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.lasso_dates}

    @classmethod
    def get_default_config(cls):
        return {"cache_file": "../configuration/calibration_dates_compare.csv", "target_dt": pd.Timestamp("2019-01-04"),
                "intervals_start_dt": pd.Timestamp("2009-06-01"), "intervals_end_dt": pd.Timestamp("2018-12-13"),
                "force_recalculation": False,
                "save_intervals_to": None}

class CalibrationDatesFundamental(CalibrationDates):
    PROVIDES_FIELDS = ["intervals_data", "regime_data"]

    def __init__(self, cache_file="../configuration/intervals_bms.csv", target_dt=None, intervals_start_dt=None,
                 intervals_end_dt=None, force_recalculation=False, save_intervals_to=None):
        target_dt = pd.Timestamp(target_dt) if target_dt else None
        intervals_start_dt = pd.Timestamp(intervals_start_dt) if intervals_start_dt else None
        intervals_end_dt = pd.Timestamp(intervals_end_dt) if intervals_end_dt else None
        self.data = None
        self.intervals_data = None
        self.target_dt = target_dt
        self.intervals_start_dt = intervals_start_dt
        self.intervals_end_dt = intervals_end_dt
        read_csv_kwargs = dict(index_col= 0, parse_dates=["entry_time", "exit_time"])
        CalibrationDates.__init__(self, cache_file, read_csv_kwargs, force_recalculation, save_intervals_to)

    def _split_interval_regime_data(self, data):
        data.rename(columns={"entry_time": "entry_date", "exit_time": "exit_date"}, inplace=True)
        data['entry_date'] = pd.DatetimeIndex(data['entry_date']).normalize()
        data['exit_date'] = pd.DatetimeIndex(data['exit_date']).normalize()
        data = data.sort_values(['view_name', 'entry_date']).reset_index(drop=True)
        data["signal_asset"].fillna(value=" ", inplace=True)
        data['signal_ticker'] = "SPY"
        data["signal_asset"].replace({" ":pd.np.NaN}, inplace=True)
        intervals_data = data.copy()
        quarter_func = lambda x:str(x-pd.tseries.offsets.QuarterEnd(normalize=True)\
                                    +pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        intervals_data['quarter_date'] = \
            intervals_data['entry_date'].apply(quarter_func)
        intervals_data["this_quarter"] = intervals_data["entry_date"].apply(lambda x: str(x.year) +"-Q"+str((x.month-1)//3+1))
        self.intervals_data = intervals_data
        return data

    def calculate_intervals(self):
        # BMS
        self.intervals_start_dt = self.intervals_start_dt or self.task_params.start_dt.normalize()
        self.intervals_end_dt = self.intervals_end_dt or self.task_params.end_dt.normalize()
        trading_days = marketTimeline.get_trading_days(self.intervals_start_dt, self.intervals_end_dt)
        bms_days = trading_days[pd.Series(trading_days.month).diff()!=0]
        bms_days.append(pd.DatetimeIndex([bms_days[-1] + pd.tseries.frequencies.to_offset("BMS")]))
        intervals = pd.concat([pd.Series(bms_days[:-1].values), pd.Series(bms_days[1:].values)], axis=1, keys=["entry_time", "exit_time"])
        intervals["view_name"] = "spy_view"
        intervals["signal_asset"] = "Long"
        return intervals

    def calculate_intervals_for_calibration(self):
        self.target = self.target_dt or self.task_params.end_dt.normalize()
        quarter_end = str(self.target_dt-pd.tseries.frequencies.QuarterEnd(normalize=True)\
                          +pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        entry_date = self.target_dt.normalize()
        data = [[entry_date, entry_date, "", quarter_end]]
        intervals = pd.DataFrame(data, columns=['entry_date', 'exit_date', 'signal_ticker', 'quarter_date'])
        return intervals

    def post_process_intervals(self, intervals):
        calibration_mode = self.task_params.run_mode is TaskflowPipelineRunMode.Calibration
        if calibration_mode:
            intervals["this_quarter"] = intervals["entry_date"].apply(lambda x: str(x.year) +"-Q"+str((x.month-1)//3+1))
            self.intervals_data = intervals
            return intervals
        else:
            new_intervals = self._split_interval_regime_data(intervals)
            new_intervals["this_quarter"] = new_intervals["entry_date"].apply(lambda x: str(x.year) +"-Q"+str((x.month-1)//3+1))
            return new_intervals

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.intervals_data,
                self.__class__.PROVIDES_FIELDS[1] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"cache_file": "../configuration/intervals_bms.csv", "target_dt": pd.Timestamp("2018-01-05"),
                "intervals_start_dt": pd.Timestamp("2017-06-01"), "intervals_end_dt": pd.Timestamp("2017-12-31"),
                "force_recalculation": False, "save_intervals_to": None}


class CalibrationDatesJump(CalibrationDates):
    PROVIDES_FIELDS = ["intervals_data"]

    def __init__(self, cache_file="../configuration/intervals_for_jump.csv", target_dt=None, intervals_start_dt=None,
                 intervals_end_dt=None, holding_period_in_trading_days=1, force_recalculation=False, save_intervals_to=None):
        target_dt = pd.Timestamp(target_dt) if target_dt else None
        intervals_start_dt = pd.Timestamp(intervals_start_dt) if intervals_start_dt else None
        intervals_end_dt = pd.Timestamp(intervals_end_dt) if intervals_end_dt else None
        self.data = None
        self.target_dt = target_dt
        self.intervals_start_dt = intervals_start_dt
        self.intervals_end_dt = intervals_end_dt
        self.holding_period_in_trading_days = holding_period_in_trading_days
        read_csv_kwargs = dict(index_col= 0, parse_dates=["entry_date", "exit_date"])
        CalibrationDates.__init__(self, cache_file, read_csv_kwargs, force_recalculation, save_intervals_to)

    def calculate_intervals(self):
        self.intervals_start_dt = self.intervals_start_dt or self.task_params.start_dt.normalize()
        self.intervals_end_dt = self.intervals_end_dt or self.task_params.end_dt.normalize()
        entry_dates = list(marketTimeline.get_trading_days(self.intervals_start_dt, self.intervals_end_dt).normalize())
        exit_dates = map(lambda x:marketTimeline.get_trading_day_using_offset(x, self.holding_period_in_trading_days), entry_dates)
        intervals = pd.concat([pd.Series(entry_dates), pd.Series(exit_dates)], axis=1, keys=["entry_date", "exit_date"])
        quarter_func = lambda x:str(x-pd.tseries.frequencies.QuarterEnd(normalize=True)\
                                    +pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        intervals['quarter_date'] = intervals['entry_date'].apply(quarter_func)
        return intervals

    def calculate_intervals_for_calibration(self):
        self.target_dt = self.target_dt or self.task_params.end_dt.normalize()
        quarter_end = str(self.target_dt-pd.tseries.frequencies.QuarterEnd(normalize=True)\
                          +pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        entry_date = self.target_dt.normalize()
        data = [[entry_date, entry_date, quarter_end]]
        intervals = pd.DataFrame(data, columns=['entry_date', 'exit_date', 'quarter_date'])
        return intervals

    def post_process_intervals(self, intervals):
        intervals["this_quarter"] = intervals["entry_date"].apply(lambda x: str(x.year) +"-Q"+str((x.month-1)/3+1))
        return intervals

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"cache_file": "../configuration/intervals_for_jump.csv", "target_dt": pd.Timestamp("2018-01-05"),
                "intervals_start_dt": pd.Timestamp("2017-06-01"), "intervals_end_dt": pd.Timestamp("2017-12-31"),
                "holding_period_in_trading_days": 1, "force_recalculation": False, "save_intervals_to": None}

class CalibrationAllPossibleDates(CalibrationDates):
    PROVIDES_FIELDS = ["intervals_data"]

    def __init__(self, cache_file="../configuration/intervals_for_base_strategy.csv", target_dt=None, intervals_start_dt=None,
                 intervals_end_dt=None, holding_period_in_trading_days=1, force_recalculation=False, save_intervals_to=None):
        target_dt = pd.Timestamp(target_dt) if target_dt else None
        intervals_start_dt = pd.Timestamp(intervals_start_dt) if intervals_start_dt else None
        intervals_end_dt = pd.Timestamp(intervals_end_dt) if intervals_end_dt else None
        self.data = None
        self.target_dt = target_dt
        self.intervals_start_dt = intervals_start_dt
        self.intervals_end_dt = intervals_end_dt
        self.holding_period_in_trading_days = holding_period_in_trading_days
        read_csv_kwargs = dict(index_col= 0, parse_dates=["entry_date", "exit_date"])
        CalibrationDates.__init__(self, cache_file, read_csv_kwargs, force_recalculation, save_intervals_to)

    def calculate_intervals(self):
        self.intervals_start_dt = self.intervals_start_dt or self.task_params.start_dt.normalize()
        self.intervals_end_dt = self.intervals_end_dt or self.task_params.end_dt.normalize()
        entry_dates = list(marketTimeline.get_trading_days(self.intervals_start_dt, self.intervals_end_dt).normalize())
        exit_dates = map(lambda x:marketTimeline.get_trading_day_using_offset(x, self.holding_period_in_trading_days), entry_dates)
        intervals = pd.concat([pd.Series(entry_dates), pd.Series(exit_dates)], axis=1, keys=["entry_date", "exit_date"])
        quarter_func = lambda x:str(x-pd.tseries.frequencies.QuarterEnd(normalize=True)\
                                    +pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        intervals['quarter_date'] = intervals['entry_date'].apply(quarter_func)
        return intervals

    def calculate_intervals_for_calibration(self):
        self.target_dt = self.target_dt or self.task_params.end_dt.normalize()
        quarter_end = str(self.target_dt-pd.tseries.frequencies.QuarterEnd(normalize=True)\
                          +pd.tseries.frequencies.to_offset('1D')).split(" ")[0]
        entry_date = self.target_dt.normalize()
        data = [[entry_date, entry_date, quarter_end]]
        intervals = pd.DataFrame(data, columns=['entry_date', 'exit_date', 'quarter_date'])
        return intervals

    def post_process_intervals(self, intervals):
        intervals["this_quarter"] = intervals["entry_date"].apply(lambda x: str(x.year) +"-Q"+str((x.month-1)/3+1))
        return intervals

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"cache_file": None, "target_dt": pd.Timestamp("2018-01-05"),
                "intervals_start_dt": pd.Timestamp("2017-06-01"), "intervals_end_dt": pd.Timestamp("2017-12-31"),
                "holding_period_in_trading_days": 1, "force_recalculation": False, "save_intervals_to": None}
