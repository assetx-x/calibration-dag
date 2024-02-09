from commonlib import talib_STOCHRSI, MA,talib_PPO
import talib
import os

from core_classes import construct_required_path,construct_destination_path, DataFormatter
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(plugins_folder, "data_processing")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(data_processing_folder,'dcm-prod.json')
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
JUMP_DATES_CSV = os.path.join(data_processing_folder,'intervals_for_jump.csv')


RUN_DATE = '2023-11-01'

###################### DataPull ######################

from data_pull_step import (
    CalibrationDatesJump, S3SecurityMasterReader,
    S3GANUniverseReader, S3IndustryMappingReader, S3EconTransformationReader,
    YahooDailyPriceReader, S3RussellComponentReader, S3RawQuandlDataReader, SQLMinuteToDailyEquityPrices
)

calibration_jump_params = DataFormatter(class_=CalibrationDatesJump,
             class_parameters={'intervals_start_dt':'2016-06-01','cache_file':JUMP_DATES_CSV,
                       'holding_period_in_trading_days':1,'force_recalculation':True,
                       'target_dt':"2018-11-30"},
             provided_data={'DataPull':['interval_data']},
             required_data={})

s3_security_master_reader = DataFormatter(class_=S3SecurityMasterReader,
             class_parameters={
                "bucket": os.environ['GCS_BUCKET'],
                "key": "alex/security_master_20230603.csv",
            },
             provided_data={'DataPull':['security_master']},
             required_data={})



s3_gan_universe_reader = DataFormatter(class_=S3GANUniverseReader,
             class_parameters={"bucket": os.environ['GCS_BUCKET'],
                              "key": "calibration_data/current_gan_training_universe.csv"},
             provided_data={'DataPull':['current_gan_universe']},
             required_data={'DataPull':['security_master']})



s3_industry_map_reader = DataFormatter(class_=S3IndustryMappingReader,
             class_parameters={
                "bucket": os.environ['GCS_BUCKET'],
                "key": "alex/industry_map.csv",
            },
             provided_data={'DataPull':['industry_mapper']},
             required_data={'DataPull':['security_master']})


s3_econ_transformation = DataFormatter(class_=S3EconTransformationReader,
             class_parameters={"sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
                              "bucket": "dcm-prod-ba2f-us-dcm-data-test",
                              "key": "alex/econ_transform_definitions.csv"},
             provided_data={'DataPull':['econ_transformation']},
             required_data={'DataPull':['security_master']})


yahoo_daily_price_reader = DataFormatter(class_=YahooDailyPriceReader,
             class_parameters={"sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
                              "start_date": "1997-01-01",
                              "end_date": RUN_DATE},
             provided_data={'DataPull':['etf_prices']},
             required_data={'DataPull':['security_master']})


s3_russull_component_reader = DataFormatter(class_=S3RussellComponentReader,
             class_parameters={"bucket": "dcm-prod-ba2f-us-dcm-data-test", 'key': 'N/A',
                              "r3k_key": "alex/r3k.csv", "r1k_key": "alex/r1k.csv"},
             provided_data={'DataPull':['russell_components']},
             required_data={'DataPull':['security_master']})


s3_russell_component_reader = DataFormatter(class_=S3RussellComponentReader,
             class_parameters={"bucket": "dcm-prod-ba2f-us-dcm-data-test", 'key': 'N/A',
                              "r3k_key": "alex/r3k.csv", "r1k_key": "alex/r1k.csv"},
             provided_data={'DataPull':['russell_components']},
             required_data={'DataPull':['security_master']})



s3_raw_quandl_reader = DataFormatter(class_=S3RawQuandlDataReader,
             class_parameters={"bucket": "dcm-prod-ba2f-us-dcm-data-temp",
                              "key": "jack/SHARADAR_SF1.csv", 'index_col': False,
                                       "start_date" : "2000-01-03","end_date" : RUN_DATE
                                      },
             provided_data={'DataPull':['raw_quandl_data']},
             required_data={'DataPull':['security_master']})


sql_minute_to_daily_equity_prices = DataFormatter(class_=SQLMinuteToDailyEquityPrices,
             class_parameters={"start_date" : "2000-01-03","end_date" : RUN_DATE},
             provided_data={'DataPull':['daily_price_data']},
             required_data={'DataPull':['security_master']})




###################### EconData ######################
from econ_data_step import  DownloadEconomicData


download_daily_economic_data = DataFormatter(class_=DownloadEconomicData,
             class_parameters={"sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
                                   "start_date": "1997-01-01",
                                   "end_date": RUN_DATE,
                                   },
             provided_data={'EconData':['econ_data']},
             required_data={'DataPull':['econ_transformation']})


###################### FundamentalCleanup ######################
from fundamental_cleanup_step import QuandlDataCleanup


quandl_data_cleanup = DataFormatter(class_=QuandlDataCleanup,
             class_parameters={},
             provided_data={'FundamentalCleanup':['quandl_daily','quandl_monthly','quandl_quarterly']},
             required_data={'DataPull':['daily_price_data','raw_quandl_data']})


###################### Targets ######################
from targets_step import CalculateTargetReturns


targets = DataFormatter(class_=CalculateTargetReturns,
             class_parameters={'return_column':'close',
                                                      'periods':[1, 5, 10, 21],
                                                      'winsorize_alpha':0.01},
             provided_data={'Targets':['target_returns']},
             required_data={'DataPull':['daily_price_data']})


###################### DerivedFundamentalDataProcessing ######################
from derived_fundamental_data_process_step import CalculateDerivedQuandlFeatures



calculate_derived_quandl_features = DataFormatter(class_=CalculateDerivedQuandlFeatures,
             class_parameters={},
             provided_data={'DerivedFundamentalDataProcessing':['fundamental_features']},
             required_data={'DataPull':['industry_mapper','daily_price_data','security_master'],
                            'FundamentalCleanup':['quandl_daily','quandl_quarterly'],
                            })


###################### DerivedTechnicalDataProcessing ######################

from derived_technical_data_processing_step import (CalculateTaLibSTOCHRSIMultiParam,
                                                    CalculateVolatilityMultiParam,
                                                    CalculateTaLibWILLRMultiParam,
                                                    CalculateTaLibPPOMultiParam,
                                                    CalculateTaLibADXMultiParam)




calculate_talib_stochrsimulti_param = DataFormatter(class_=CalculateTaLibSTOCHRSIMultiParam,
             class_parameters=[{"technical_indicator_params": {"timeperiod": 14, "fastk_period": 5,
                                                                   "fastd_period": 3, "fastd_matype": 0},
                                    "price_column": "close"},
        {"technical_indicator_params": {"timeperiod": 30, "fastk_period": 10,
                                                                   "fastd_period": 5, "fastd_matype": 0},
                                    "price_column": "close"},
        {"technical_indicator_params": {"timeperiod": 63, "fastk_period": 15,
                                                                   "fastd_period": 10, "fastd_matype": 0},
                                    "price_column": "close"},
       ],
             provided_data={'DerivedTechnicalDataProcessing':['talib_stochrsi_indicator_data']},
             required_data={'DataPull':['daily_price_data'],
                            })




calculate_volatility_multi_param = DataFormatter(class_=CalculateVolatilityMultiParam,
             class_parameters=[{"volatility_lookback": 63, "price_column": "close"},
                                        {"volatility_lookback": 21, "price_column": "close"},
                                        {"volatility_lookback": 126, "price_column": "close"}],
             provided_data={'DerivedTechnicalDataProcessing':['volatility_data']},
             required_data={'DataPull':['daily_price_data'],
                            })

######


calculate_talib_willr_multi_param = DataFormatter(class_=CalculateTaLibWILLRMultiParam,
             class_parameters=[{"technical_indicator_params": {"timeperiod": 5}, "smoothing_period": 3},
                                         {"technical_indicator_params": {"timeperiod": 14}, "smoothing_period": 3},
                                         {"technical_indicator_params": {"timeperiod": 63}, "smoothing_period": 3},
                                        ],
             provided_data={'DerivedTechnicalDataProcessing':['talib_willr_indicator_data']},
             required_data={'DataPull':['daily_price_data'],
                            })




##############


calculate_talib_ppo_multi_param = DataFormatter(class_=CalculateTaLibPPOMultiParam,
             class_parameters=[{"technical_indicator_params": {"fastperiod": 12, "slowperiod": 26, "matype": 0},
                                    "price_column": "close", "invert_sign": True},
                                      {"technical_indicator_params": {"fastperiod": 3, "slowperiod": 14, "matype": 0},
                                    "price_column": "close", "invert_sign": True},
                                      {"technical_indicator_params": {"fastperiod": 21, "slowperiod": 126, "matype": 0},
                                    "price_column": "close", "invert_sign": True},
                                     ],
             provided_data={'DerivedTechnicalDataProcessing':['talib_ppo_indicator_data']},
             required_data={'DataPull':['daily_price_data'],
                            })


##########

calculate_talib_adx_mult_param = DataFormatter(class_=CalculateTaLibADXMultiParam,
             class_parameters=[{"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 5},
                                     "smoothing_period": 3},
                              {"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 14},
                                     "smoothing_period": 3},
                               {"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 63},
                                     "smoothing_period": 3}
                              ],
             provided_data={'DerivedTechnicalDataProcessing':['talib_adx_indicator_data']},
             required_data={'DataPull':['daily_price_data'],
                            })



###################### DerivedSimplePriceFeatureProcessing ######################


from derived_simple_price_step import (
    ComputeBetaQuantamental,
    CalculateMACD,
    CalcualteCorrelation,
    CalculateDollarVolume,
    CalculateOvernightReturn,
    CalculatePastReturnEquity,
    CalculateTaLibSTOCH,
    CalculateTaLibSTOCHF,
    CalculateTaLibTRIX,
    CalculateTaLibULTOSC,
)


compute_beta_quantamental = DataFormatter(class_=ComputeBetaQuantamental,
             class_parameters={"benchmark_names": [8554], "beta_lookback": 63,
                "offset_unit": "B", "price_column": "close", "dropna_pctg": 0.15,
                "use_robust": False, "epsilon": 1.35, "alpha": 0.0001, "fit_intercept": False},
             provided_data={'DerivedSimplePriceFeatureProcessing':['beta_data']},
             required_data={'DataPull':['daily_price_data','intervals_data']})



calculate_macd = DataFormatter(class_=CalculateMACD,
             class_parameters={"technical_indicator": "macd", "technical_indicator_params": {"nslow":26, "nfast":12},
                "smoothing_period": 3, "price_column": "close"},
             provided_data={'DerivedSimplePriceFeatureProcessing':['macd_indicator_data']},
             required_data={'DataPull':['daily_price_data']})


calculate_correlation = DataFormatter(class_=CalcualteCorrelation,
             class_parameters={"bucket": os.environ['GCS_BUCKET'],
                              "key": "calibration_data/current_gan_training_universe.csv"},
             provided_data={'DerivedSimplePriceFeatureProcessing':['correlation_data']},
             required_data={'DataPull':['daily_price_data']})

calculate_dollar_volume = DataFormatter(class_=CalculateDollarVolume,
             class_parameters={"lookback_periods": 21},
             provided_data={'DerivedSimplePriceFeatureProcessing':['dollar_volume_data']},
             required_data={'DataPull':['daily_price_data']})



calculate_overnight_return = DataFormatter(class_=CalculateOvernightReturn,
             class_parameters={},
             provided_data={'DerivedSimplePriceFeatureProcessing':['overnight_return_data']},
             required_data={'DataPull':['daily_price_data']})


calculate_past_returns_equity = DataFormatter(class_=CalculatePastReturnEquity,
             class_parameters={"column": "close","lookback_list": [1, 2, 5, 10, 21, 63, 126, 252]},
             provided_data={'DerivedSimplePriceFeatureProcessing':['past_return_data']},
             required_data={'DataPull':['daily_price_data']})


calculate_talib_stoch = DataFormatter(class_=CalculateTaLibSTOCH,
             class_parameters={'technical_indicator_params':{"fastk_period": 5,
                                                                      "slowk_period": 3,
                                                                      "slowk_matype": 0,
                                                                      "slowd_period": 3,
                                                                      "slowd_matype": 0}},
             provided_data={'DerivedSimplePriceFeatureProcessing':['talib_stoch_indicator_data']},
             required_data={'DataPull':['daily_price_data']})


calculate_talib_stochf = DataFormatter(class_=CalculateTaLibSTOCHF,
             class_parameters={'technical_indicator_params':{"fastk_period": 5,
                                                             "fastd_period": 3,
                                                             "fastd_matype": 0}},
             provided_data={'DerivedSimplePriceFeatureProcessing':['talib_stochf_indicator_data']},
             required_data={'DataPull':['daily_price_data']})



calculate_talib_trix = DataFormatter(class_=CalculateTaLibTRIX,
             class_parameters={'technical_indicator_params':
                                                  {"timeperiod": 30},
                                                 'price_column':'close'},
             provided_data={'DerivedSimplePriceFeatureProcessing':['talib_trix_indicator_data']},
             required_data={'DataPull':['daily_price_data']})




calculate_talib_ultosc = DataFormatter(class_=CalculateTaLibULTOSC,
             class_parameters={'technical_indicator_params':{"timeperiod1": 7,
                                                                       "timeperiod2": 14,
                                                                       "timeperiod3": 28},
                                         'smoothing_period':3},
             provided_data={'DerivedSimplePriceFeatureProcessing':['talib_ultosc_indicator_data']},
             required_data={'DataPull':['daily_price_data']})





###################### MergeStep ######################
from merge_step import QuantamentalMerge



QM_datasets = {'DerivedSimplePriceFeatureProcessing':['beta_data',
                                                      'macd_indicator_data',
                                                      'correlation_data',
                                                      'talib_ultosc_indicator_data',
                                                      'dollar_volume_data',
                                                      'overnight_return_data',
                                                      'past_return_data',
                                                      'talib_stoch_indicator_data',
                                                      'talib_stochf_indicator_data',
                                                      'talib_trix_indicator_data'],
               'DerivedTechnicalDataProcessing':['talib_stochrsi_indicator_data',
                                                 'volatility_data',
                                                 'talib_willr_indicator_data',
                                                 'talib_adx_indicator_data',
                                                 'talib_ppo_indicator_data'],
               'DerivedFundamentalDataProcessing':['fundamental_features'],
               'DataPull':['security_master','daily_price_data'],
               'Targets':['target_returns']}



quantamental_merge = DataFormatter(class_=QuantamentalMerge,
             class_parameters={'apply_log_vol': True,
              'start_date': "2000-03-15",
              'end_date': RUN_DATE
              },
             provided_data={'QuantamentalMerge':['merged_data']},
             required_data=QM_datasets)


###################### FilterDatesSingleNames ######################
from filter_dates_single_names import FilterMonthlyDatesFullPopulationWeekly,CreateMonthlyDataSingleNamesWeekly



filter_monthly_dates_full_pop = DataFormatter(class_=FilterMonthlyDatesFullPopulationWeekly,
             class_parameters={'monthly_mode':"bme",
        'weekly_mode':"w-mon",
        'start_date':"2000-03-15",
        'end_date':RUN_DATE},
             provided_data={'FilterDatesSingleNames':['monthly_merged_data','weekly_merged_data']},
             required_data={'QuantamentalMerge':['merge_step']})


create_monthly_data_single_names = DataFormatter(class_=CreateMonthlyDataSingleNamesWeekly,
             class_parameters={},
             provided_data={'FilterDatesSingleNames':['monthly_merged_data_single_names','weekly_merged_data_single_names']},
             required_data={'FilterDatesSingleNames':['monthly_merged_data','weekly_merged_data'],
                            'DataPull':['security_master']})



###################### Transformation ######################
from transformation_step import (
    CreateYahooDailyPriceRolling,
    TransformEconomicDataWeekly,
    CreateIndustryAverageWeekly,
)



create_yahoo_daily_price_rolling =DataFormatter(class_=CreateYahooDailyPriceRolling,
             class_parameters={'rolling_interval':5},
             provided_data={'Transformation':['yahoo_daily_price_rolling']},
             required_data={'DataPull':['yahoo_daily_price_data']})


transform_economic_data_weekly =DataFormatter(class_=TransformEconomicDataWeekly,
             class_parameters={"monthly_mode" : "bme",
                "weekly_mode" : "w-mon",
                "start_date" : "1997-01-01",
                "end_date" : "2023-06-28",
                "shift_increment": "month"},
             provided_data={'Transformation':['transformed_econ_data','transformed_econ_data_weekly']},
             required_data={'DataPull':['econ_transformation'],
                            'Transformation':['yahoo_daily_price_rolling'],
                            'EconData':['econ_data']})


CIA_params = {'industry_cols':["volatility_126", "PPO_12_26", "PPO_21_126", "netmargin",
                               "macd_diff", "pe", "debt2equity", "bm", "ret_63B", "ebitda_to_ev", "divyield"],
        'sector_cols':["volatility_126", "PPO_21_126", "macd_diff", "divyield", "bm"]}

create_industry_average_weekly = DataFormatter(class_=CreateIndustryAverageWeekly,
             class_parameters=CIA_params,
             provided_data={'Transformation':['industry_average',
                                              'sector_average',
                                              'industry_average_weekly',
                                              'sector_average_weekly']},
             required_data={'DataPull':['security_master'],
                            'FilterDatesSingleNames':['monthly_merged_data_single_names',
                                                      'weekly_merged_data_single_names']})




###################### MergeEcon ######################
from merge_econ_step import QuantamentalMergeEconIndustryWeekly

WMEIW_params = {'industry_cols':["volatility_126", "PPO_12_26", "PPO_21_126", "netmargin", "macd_diff", "pe", "debt2equity", "bm", "ret_63B", "ebitda_to_ev", "divyield"],
 'security_master_cols':["dcm_security_id", "Sector", "IndustryGroup"],
 'sector_cols':["volatility_126", "PPO_21_126", "macd_diff", "divyield", "bm"],
 'key_sectors':["Energy", "Information Technology", "Financials", "Utilities", "Consumer Discretionary", "Industrials", "Consumer Staples"],
 'econ_cols':["RETAILx", "USTRADE", "SPY_close", "bm_Financials", "T10YFFM", "T5YFFM", "CPITRNSL", "DCOILWTICO", "EWJ_volume", "HWI",
              "CUSR0000SA0L2", "CUSR0000SA0L5", "T1YFFM", "DNDGRG3M086SBEA", "AAAFFM", "RPI", "macd_diff_ConsumerStaples", "DEXUSUK",
              "CPFF", "PPO_21_126_Industrials", "PPO_21_126_Financials", "CP3Mx", "divyield_ConsumerStaples", "VIXCLS", "GS10", "bm_Utilities",
              "EWG_close", "CUSR0000SAC", "GS5", "divyield_Industrials", "WPSID62", "IPDCONGD", "PPO_21_126_InformationTechnology", "PPO_21_126_Energy",
              "PPO_21_126_ConsumerDiscretionary"],
 'start_date': "1997-12-15",
 'end_date': RUN_DATE,
 'normalize_econ':False
    }


quantamental_merge_econ_industry_weekly = DataFormatter(class_=QuantamentalMergeEconIndustryWeekly,
             class_parameters=WMEIW_params,
             provided_data={'MergeEcon':['merged_data_econ_industry',
                                              'merged_data_econ_industry_weekly',
                                              'econ_data_final',
                                              'econ_data_final_weekly']},
             required_data={'DataPull':['security_master'],
                            'FilterDatesSingleNames':['monthly_merged_data_single_names',
                                                      'weekly_merged_data_single_names'],
                            'Transformation':['industry_average',
                                              'sector_average',
                                              'transformed_econ_data',
                                              'industry_average_weekly',
                                              'sector_average_weekly',
                                              'transformed_econ_data_weekly']})



###################### Standarization ######################
from standarization_step import FactorStandardizationFullPopulationWeekly

FSFPW_params = {'all_features':True,
                'exclude_from_standardization':["fq", "divyield_Industrials", "PPO_21_126_ConsumerDiscretionary", "DNDGRG3M086SBEA", "DEXUSUK", "GS10", "IPDCONGD", "T5YFFM",
                                        "USTRADE", "CUSR0000SA0L2", "RETAILx", "bm_Financials", "DCOILWTICO", "T10YFFM", "CPITRNSL", "CP3Mx", "CUSR0000SAC", "EWJ_volume",
                                        "SPY_close", "VIXCLS", "PPO_21_126_InformationTechnology", "WPSID62", "GS5", "CPFF", "CUSR0000SA0L5", "T1YFFM", "PPO_21_126_Energy",
                                        "bm_Utilities", "PPO_21_126_Financials", "HWI", "RPI", "PPO_21_126_Industrials", "divyield_ConsumerStaples", "EWG_close", "macd_diff_ConsumerStaples",
                                        "AAAFFM", "fold_id", "Sector", "IndustryGroup"],
                'target_columns':["future_asset_growth_qoq", "future_ret_10B", "future_ret_1B", "future_ret_21B", "future_ret_42B", "future_ret_5B", "future_revenue_growth_qoq"],
                'suffixes_to_exclude':["_std"]
    }



factor_standardization_full_pop_weekly = DataFormatter(class_=FactorStandardizationFullPopulationWeekly,
             class_parameters=FSFPW_params,
             provided_data={'Standarization':['normalized_data_full_population',
                                              'normalized_data_full_population_weekly']},
             required_data={'MergeEcon':['merged_data_econ_industry',
                                         'merged_data_econ_industry_weekly']})


###################### ActiveMatrix ######################
from active_matrix_step import GenerateActiveMatrixWeekly

generate_active_matrix_weekly = DataFormatter(class_=GenerateActiveMatrixWeekly,
             class_parameters={'start_date': "2000-01-01",
                    'end_date': RUN_DATE,
                    'ref_col':'ret_5B'
                },
             provided_data={'ActiveMatrix':['active_matrix',
                                              'active_matrix_weekly']},
             required_data={'FilterDatesSingleNames':['monthly_merged_data_single_names',
                                         'weekly_merged_data_single_names'],
                            'DataPull':['current_gan_universe']})



###################### AdditionalGANFeatures ######################


from additional_gan_features_step import GenerateBMEReturnsWeekly


generate_bme_returns_weekly = DataFormatter(class_=GenerateBMEReturnsWeekly,
             class_parameters={},
             provided_data={'AdditionalGANFeatures':['past_returns_bme',
                                              'future_returns_bme',
                                                     'future_returns_weekly']},
             required_data={'ActiveMatrix':['active_matrix',
                                         'active_matrix_weekly'],
                            'DataPull':['daily_price_data']})


###################### SaveGANInputs ######################

from save_gan_inputs_step import GenerateDataGANWeekly

GDGW_params = {'data_dir':construct_destination_path('SaveGANInputs').format(os.environ['GCS_BUCKET'],
                                                                              'holding').split('/holding')[0]}

generate_data_gan_weekly = DataFormatter(class_=GenerateDataGANWeekly,
             class_parameters=GDGW_params,
             provided_data={'SaveGANInputs':['company_data',
                                              'company_data_weekly',
                                                     'gan_data_info']},
             required_data={'Standarization':['normalized_data_full_population',
                                         'normalized_data_full_population_weekly'],
                            'MergeEcon':['econ_data_final','econ_data_final_weekly'],
                            'ActiveMatrix':['active_matrix','active_matrix_weekly'],
                            'AdditionalGANFeatures':['future_returns_bme','future_returns_weekly']})




PARAMS_DICTIONARY = {'CalibrationDatesJump':calibration_jump_params,
                     'S3SecurityMasterReader':s3_security_master_reader,
                     'S3GANUniverseReader':s3_gan_universe_reader,
                     'S3IndustryMappingReader':s3_industry_map_reader,
                     'S3EconTransformationReader':s3_econ_transformation,
                     'YahooDailyPriceReader':yahoo_daily_price_reader,
                     'S3RussellComponentReader':s3_russell_component_reader,
                     'S3RawQuandlDataReader':s3_raw_quandl_reader,
                     'SQLMinuteToDailyEquityPrices':sql_minute_to_daily_equity_prices,
                     'DownloadEconomicData':download_daily_economic_data,
                     'QuandlDataCleanup':quandl_data_cleanup,
                     'CalculateTargetReturns':targets,
                     'CalculateDerivedQuandlFeatures':calculate_derived_quandl_features,
                     'CalculateTaLibSTOCHRSIMultiParam':calculate_talib_stochrsimulti_param,
                     'CalculateVolatilityMultiParam':calculate_volatility_multi_param,
                     'CalculateTaLibWILLRMultiParam':calculate_talib_willr_multi_param,
                     'CalculateTaLibPPOMultiParam':calculate_talib_ppo_multi_param,
                     'CalculateTaLibADXMultiParam':calculate_talib_adx_mult_param,
                     'ComputeBetaQuantamental':compute_beta_quantamental,
                     'CalculateMACD':calculate_macd,
                     'CalcualteCorrelation':calculate_correlation,
                     'CalculateDollarVolume':calculate_dollar_volume,
                     'CalculateOvernightReturn':calculate_overnight_return,
                     'CalculatePastReturnEquity':calculate_past_returns_equity,
                     'CalculateTaLibSTOCH':calculate_talib_stoch,
                     'CalculateTaLibSTOCHF':calculate_talib_stochf,
                     'CalculateTaLibTRIX':calculate_talib_trix,
                     'CalculateTaLibULTOSC':calculate_talib_ultosc,
                     'QuantamentalMerge':quantamental_merge,
                     'FilterMonthlyDatesFullPopulationWeekly':filter_monthly_dates_full_pop,
                     'CreateMonthlyDataSingleNamesWeekly':create_monthly_data_single_names,
                     'CreateYahooDailyPriceRolling':create_yahoo_daily_price_rolling,
                     'TransformEconomicDataWeekly':transform_economic_data_weekly,
                     'CreateIndustryAverageWeekly':CreateIndustryAverageWeekly,
                     'QuantamentalMergeEconIndustryWeekly':quantamental_merge_econ_industry_weekly,
                     'FactorStandardizationFullPopulationWeekly':factor_standardization_full_pop_weekly,
                     'GenerateActiveMatrixWeekly':generate_active_matrix_weekly,
                     'GenerateBMEReturnsWeekly':generate_bme_returns_weekly,
                     'GenerateDataGANWeekly':generate_data_gan_weekly}


