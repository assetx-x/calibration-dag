import warnings

warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
from scipy.stats import percentileofscore
from dotenv import load_dotenv


load_dotenv()
growth_factors = [
    "revenue_growth",
    "grossmargin",
    "netmargin_indgrp",
    "netmargin",
    "capex_yield",
    "log_mktcap",
    "revenue_yield",
]
quality_factors = [
    "currentratio",
    "fcf_yield",
    "debt2equity_indgrp",
    "debt2equity",
    "amihud",
    "current_to_total_assets",
    "retearn_to_total_assets",
]
macro_factors = [
    "DCOILWTICO",
    "T5YFFM",
    "IPDCONGD",
    "DEXUSUK",
    "RPI",
    "USTRADE",
    "RETAILx",
    "AAAFFM",
    "GS10",
    "HWI",
    "GS5",
    "CUSR0000SAC",
    "CPFF",
    "CPITRNSL",
    "T10YFFM",
    "T1YFFM",
    "WPSID62",
    "CUSR0000SA0L2",
]
momentum_fast_factors = [
    "STOCHRSI_FASTK_63_15_10",
    "STOCHRSI_FASTD_14_5_3",
    "volatility_126_indgrp",
    "volatility_126",
    "volatility_21",
    "VIXCLS",
]
momentum_slow_factors = [
    "ret_10B",
    "macd_diff_indgrp",
    "momentum",
    "ret_126B",
    "ret_1B",
    "WILLR_14",
    "macd_diff_ConsumerStaples",
    "macd_diff",
    "bm_Financials",
    "bm_Utilities",
    "bm_indgrp",
    "bm",
]
trend_following_factors = [
    "PPO_21_126",
    "ADX_14_MA_3",
    "PPO_12_26",
    "PPO_21_126_ConsumerDiscretionary",
    "PPO_3_14",
    "rel_to_high",
    "PPO_21_126_Industrials",
    "PPO_21_126_Energy",
    "PPO_21_126_InformationTechnology",
    "PPO_21_126_indgrp",
    "PPO_12_26_indgrp",
]
value_factors = [
    "pe1",
    "ebitdamargin",
    "investments_yield",
    "ebitda_to_ev_indgrp",
    "ebitda_to_ev",
    "pe_indgrp",
    "divyield_indgrp",
    "divyield_Industrials",
    "divyield",
    "gp2assets",
    "payoutratio",
    "divyield_ConsumerStaples",
]
other_factors = [
    "std_turn",
    "EWJ_volume",
    "ret_63B_indgrp",
    "overnight_return",
    "EWG_close",
    "log_avg_dollar_volume",
    "ret_252B",
    "log_dollar_volume",
    "SPY_beta",
    "maxret",
    "SPY_close",
    "ret_63B",
    "ret_2B",
]

MAPPING_DICTIONARY = {
    "Growth": growth_factors,
    "Quality": quality_factors,
    "Macro": macro_factors,
    "Momentum Fast": momentum_fast_factors,
    "Momentum Slow": momentum_slow_factors,
    "Trend Following": trend_following_factors,
    "Value": value_factors,
    "Other Factors": other_factors,
}


def factor_grouping(feature_importance, sub_factor_map):
    shap_df_21D_full = feature_importance
    shap_df_21D = feature_importance.iloc[-1]
    raw_data = feature_importance
    shap_variables = shap_df_21D[
        abs(shap_df_21D).sort_values(ascending=False).index.tolist()
    ]

    sub_data_dictionary = sub_factor_map

    subfactor_dictionary_21 = {}
    subfactor_dictionary_5 = {}

    longitudinal_subfactor_exposure = pd.DataFrame([])

    longitudinal_subfactor_exposure_raw = pd.DataFrame([])

    for shap_group in sub_data_dictionary:
        subfactor_dictionary_21 = {}
        subfactor_dictionary_5 = {}
        overlapping_features = [
            i
            for i in sub_data_dictionary[shap_group]
            if i in shap_df_21D.index.tolist()
        ]

        subgroup_long = abs(
            shap_df_21D_full.reindex(overlapping_features, axis="columns")
        ).sum(axis=1)

        subgroup_long_raw = shap_df_21D_full.reindex(
            overlapping_features, axis="columns"
        ).sum(axis=1)

        longitudinal_subfactor_exposure[shap_group] = subgroup_long.values
        longitudinal_subfactor_exposure_raw[shap_group] = subgroup_long_raw.values

        longitudinal_subfactor_exposure_raw.index = feature_importance.index
        longitudinal_subfactor_exposure.index = feature_importance.index

    return longitudinal_subfactor_exposure, longitudinal_subfactor_exposure_raw
