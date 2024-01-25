from ax_gcp_functions import (get_security_master_full,
                              v3_feature_importance_edit,
                              grab_single_name_prediction,
                              pull_single_name_returns,
                              get_company_information_from_ticker,
                              fetch_raw_data,
                              grab_multiple_predictions,
                              get_ticker_from_security_id_mapper)

from ax_feature_importance_helpers import factor_grouping


from ax_miscellaneous_utils import (
    prediction_waterfall_ploty,
    prediction_waterfall_plotly_full,
    convert_and_round,
    plotly_area_graph_data,
    plotly_area_graph_data_overall,
    image_fetch)

from dotenv import load_dotenv



load_dotenv()
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np
from datetime import timedelta
import gcsfs
import datetime
import os
import sys


# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.getcwd())
json_auth_file = os.path.join('plugins', 'data_processing', 'dcm-prod.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_auth_file
os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'
print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")
print(f"GCS_BUCKET: {os.environ['GCS_BUCKET']}")
formal_name_mapper_path = os.path.join('plugins', 'data_processing', 'variable_names_formatted_updated.csv')

formal_name_mapper = (
            pd.read_csv(formal_name_mapper_path)
            .set_index(["variable_names"])["interpretable_names"]
            .to_dict()
        )


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

def signal_to_sentence(signal, feature_importance, ticker):
    signal_text_dict = {
        4: "very bullish",
        3: "somewhat bullish",
        2: "neutral",
        1: "bearish",
        0: "very bearish",
    }

def format_comparables(ticker, info):
    company = get_object_or_404(Company, ticker__name=ticker)
    sec_m_data = info.loc[ticker]

    return {
        "company_logo": company.company_logo,
        "ticker": ticker,
        "sector": sec_m_data.Sector,
        "company_name": sec_m_data.company_name,
    }


class CreateInsights(object):
    def __init__(self,ticker,security_id):
        self.ticker = ticker
        self.security_id = security_id
        #self.feature_importance = None

    def retrieve_feature_importance(self):
        fi ,model_type= v3_feature_importance_edit(self.security_id)
        self.model_type = model_type
        fi['date'] = [i.strftime('%Y-%m-%d') for i in fi['date']]
        fi.set_index(['date','ticker'],inplace=True)
        return fi

    def format_feature_importance(self,feature_importance):
        return factor_grouping(
            feature_importance, MAPPING_DICTIONARY
        )

    def fetch_predictions(self,model_type):
        return grab_single_name_prediction(model_type, self.security_id)

    def fetch_pnl(self):
        single_name_returns = pull_single_name_returns(self.ticker,end='2022-01-01')
        # single_name_returns.set_index(['date'], inplace=True)
        single_name_returns.index = [i.strftime('%Y-%m-%d') for i in single_name_returns.index]
        ticker_pnl= single_name_returns['ret_1B']
        LINESERIES = [{'time': k, 'value': ticker_pnl.loc[k]} for k in ticker_pnl.index]

        return LINESERIES

    def construct_feature_importance_waterfall(self,feature_importance,feature_importance_grouped,expected_value):
        plotly_waterfall_graph_dictionary = {
            k: prediction_waterfall_ploty(
                feature_importance,
                MAPPING_DICTIONARY[k],
                formal_name_mapper,
            )
            for k in MAPPING_DICTIONARY.keys()
        }

        plotly_waterfall_graph_dictionary[
            "Overall"
        ] = prediction_waterfall_plotly_full(
            feature_importance_grouped, expected_value
        )

        return plotly_waterfall_graph_dictionary

    def construct_feature_importance_area(self,feature_importance_absolute,feature_importance_absolute_grouped):
        plotly_area_graph_dictionary = {k: plotly_area_graph_data(feature_importance_absolute[MAPPING_DICTIONARY[k]],
                                                                  formal_name_mapper) for k in MAPPING_DICTIONARY.keys()
                                        }

        plotly_area_graph_dictionary["Overall"] =plotly_area_graph_data_overall(feature_importance_absolute_grouped)

        return plotly_area_graph_dictionary

    def factor_attribution(self,fi_grouped_raw):
        return {
            "current": {
                key: round(value * 100, 2)
                for key, value in fi_grouped_raw.iloc[-1].items()
            },
            "historical": {
                key: round(value * 100, 2)
                for key, value in fi_grouped_raw.mean().items()
            },
        }

    def factor_contribution(self,fi_grouped_exposure_mean_date,value_shap,similar_companies_sec_ids):


        expsoure_mean_sum_axis = fi_grouped_exposure_mean_date.sum(axis=1)
        percentage_df = fi_grouped_exposure_mean_date.div(expsoure_mean_sum_axis, axis=0)

        longitudinal_percentages_average = (
                percentage_df.iloc[-1]
                .apply(convert_and_round)
                .astype(str)
                + "%"
        )

        longitudinal_percentages_average_global = (
                percentage_df.mean().apply(convert_and_round).astype(str)
                + "%"
        )

        sector_fi = value_shap[
            (
                value_shap.index.get_level_values(1).isin(
                    similar_companies_sec_ids
                )
            )
        ]
        numeric_columns = sector_fi.select_dtypes(include=[np.number]).columns
        sector_fi = sector_fi[numeric_columns]
        sector_fi = sector_fi.groupby(["date"]).sum()
        sector_fi_grouped_exposure, sector_fi_grouped_raw = factor_grouping(
            sector_fi, MAPPING_DICTIONARY
        )

        sector_fi_grouped_exposure_mean_date = (
            sector_fi_grouped_exposure.groupby(["date"]).mean()
        )
        sector_expsoure_mean_sum_axis = sector_fi_grouped_exposure_mean_date.sum(axis=1)
        percentage_df_sector = sector_fi_grouped_exposure_mean_date.div(sector_expsoure_mean_sum_axis, axis=0)

        sector_longitudinal_percentages_average = (
                percentage_df_sector.iloc[-1]
                .apply(convert_and_round)
                .astype(str)
                + "%"
        )

        sector_longitudinal_percentages_average_global = (
                percentage_df_sector.mean()
                .apply(convert_and_round)
                .astype(str)
                + "%"
        )

        factor_contribution = {}
        factor_contribution[
            "current_contribution"
        ] = longitudinal_percentages_average.to_dict()
        factor_contribution[
            "historical_contribution"
        ] = longitudinal_percentages_average_global.to_dict()
        factor_contribution[
            "sector_current_contribution"
        ] = sector_longitudinal_percentages_average.to_dict()
        factor_contribution[
            "sector_historical_contribution"
        ] = sector_longitudinal_percentages_average_global.to_dict()

        return factor_contribution

    def get_sector_cohort(self):
        similar_companies_sector = get_company_information_from_ticker(self.security_id)
        similar_companies_sec_ids = similar_companies_sector[
            "dcm_security_id"
        ].values.tolist()

        return similar_companies_sector,similar_companies_sec_ids

    def sector_comparables(self,similar_companies_sector,similar_companies_sec_ids):


        sector_signals = grab_multiple_predictions(
            "value", similar_companies_sec_ids
        )
        sector_signals_terminal_date = sector_signals.date.iloc[-1]
        top_sector_signals = sector_signals[
            (sector_signals.date == sector_signals_terminal_date)
            & (sector_signals.ensemble_qt == 4)
            ]
        filtered_sector_selections = similar_companies_sector[
            (
                similar_companies_sector.dcm_security_id.isin(
                    top_sector_signals.ticker
                )
            )
        ]
        leng_filtered = len(filtered_sector_selections)
        comparibles = filtered_sector_selections.sample(
            n=leng_filtered // 2, replace=False
        )
        comparibles.drop(["dcm_security_id"], axis=1, inplace=True)
        comparibles.set_index(["ticker"], inplace=True)
        formatted_comparibles = [
            {k: format_comparables(k, comparibles) for k in comparibles.index}
        ]

        ai_comparables = [(formatted_comparibles[0][i]['ticker'],
                           formatted_comparibles[0][i]) for i in formatted_comparibles[0].keys()]

        return ai_comparables

    def get_raw_feature_summary(self,feature_importance):
        raw_data = fetch_raw_data(self.security_id).sort_values(by=['date'])
        raw_data.set_index(['date','ticker'],inplace=True)
        recent_values = raw_data.iloc[-2].fillna(0)
        recent_feature_importance = feature_importance.iloc[-1].fillna(0)

        dictionary_results = {}
        for k in MAPPING_DICTIONARY.keys():
            sub_dictionary = {}
            features = MAPPING_DICTIONARY[k]
            top_factors_group = abs(feature_importance[features]).iloc[-1].sort_values(ascending=False)[0:3].index.tolist()
            subfactor_df = feature_importance[features].iloc[-1].sort_values(ascending=False)[
                                0:3].index.tolist()
            raw_value_subset = raw_data[features].iloc[-2]
            for factor in top_factors_group:
                sub_dictionary[formal_name_mapper[factor]] = [round(recent_values[factor],3),
                                          is_positive_or_negative(recent_feature_importance[factor])]

            dictionary_results[k] = sub_dictionary


        return dictionary_results,

    def get_insight_overview(self,fi):
        signal = grab_single_name_prediction(self.model_type, self.security_id)
        bull_ratio,bear_ratio = compute_percentages_hit_ratio(signal)

        current_signal = signal['ensemble_qt'].iloc[-1]
        past_occurances = len(signal[(signal['ensemble_qt']==current_signal)])

        return {'cross_sectional_rank':float(current_signal),
         'hit_ratio_bull':str(round((bull_ratio*100),2)),
         'hit_ratio_bear': str(round((bear_ratio*100),2)),
         'past_occurances':float(past_occurances),
                'sentence': signal_to_sentence(signal, fi, self.ticker)}


    def get_news(self):
        return [["News",'Coming Soon']*10]


def compute_percentages_hit_ratio(df):
    # Filter the dataframe for portfolio value 4 and compute percentage of positive returns
    portfolio_4 = df[df['ensemble_qt'] == 4]
    percentage_4_positive = (portfolio_4['future_ret_5B'] > 0).mean()

    # Filter the dataframe for portfolio value 0 and compute percentage of negative returns
    portfolio_0 = df[df['ensemble_qt'] == 0]
    percentage_0_negative = (portfolio_0['future_ret_5B'] < 0).mean()

    return percentage_4_positive, percentage_0_negative

def is_positive_or_negative(val):
    if val >0:
        return 'Positive'
    else:
        return 'Negative'


def load_data():
    value_prediction_csv_path = (
        "gs://dcm-prod-ba2f-us-dcm-data-test/alex/value_predictions (1).csv"
    )
    growth_prediction_csv_path = (
        "gs://dcm-prod-ba2f-us-dcm-data-test/alex/growth_predictions (1).csv"
    )

    value_predictions = pd.read_csv(value_prediction_csv_path).set_index(
        ["date", "ticker"]
    )
    growth_predictions = pd.read_csv(growth_prediction_csv_path).set_index(
        ["date", "ticker"]
    )

    security_ids = sorted(
        set(
            list(growth_predictions.index.levels[1])
            + list(value_predictions.index.levels[1])
        )
    )

    return security_ids


def main():
    security_ids = load_data()

    dictionary_mapper = get_ticker_from_security_id_mapper(security_ids)['ticker'].to_dict()

    security_master = get_security_master_full()
    value_shap = pd.read_csv(
        "gs://dcm-prod-ba2f-us-dcm-data-test/alex/value_shap (2).csv"
    ).set_index(["date", "ticker"])

    for sec in security_ids:
        try:
            name = dictionary_mapper[sec]
            print(name)
            insight_object = CreateInsights(name, sec)
            feature_importance = insight_object.retrieve_feature_importance()
            pnl = insight_object.fetch_pnl()
            feature_importance_absolute_val, feature_importance_raw = insight_object.format_feature_importance(
                feature_importance)
            waterfall = insight_object.construct_feature_importance_waterfall(
                feature_importance.groupby(["date"]).sum(),
                feature_importance_raw.groupby(["date"]).sum(),
                -0.04)

            area_plot = insight_object.construct_feature_importance_area(
                abs(feature_importance).groupby(["date"]).sum(),
                feature_importance_raw.groupby(["date"]).sum())

            factor_attribution = insight_object.factor_attribution(feature_importance_raw)
            similar_companies_sector, similar_companies_sec_ids = insight_object.get_sector_cohort()
            factor_contribution = insight_object.factor_contribution(feature_importance_raw.groupby(['date']).sum(),
                                                                     value_shap, similar_companies_sec_ids)
            comparable_recommendations = insight_object.sector_comparables(similar_companies_sector,
                                                                           similar_companies_sec_ids)
            insight_overview = insight_object.get_insight_overview(feature_importance)
            key_values = insight_object.get_raw_feature_summary(feature_importance)
            news = insight_object.get_news()

            model_dictionary = {}
            ticker_info = security_master[(security_master.ticker == name)]
            model_dictionary["symbol"] = name
            model_dictionary["sector"] = ticker_info["Sector"].iloc[0]
            model_dictionary["industry"] = ticker_info["Industry"].iloc[0]
            model_dictionary["company_name"] = ticker_info["company_name"].iloc[0]
            model_dictionary["company_logo"] = image_fetch(name)

            header_info = {
                "company_name": model_dictionary["company_name"],
                "company_logo": model_dictionary["company_logo"],
            }

            data = {
                "forecast": pnl,
                "feature_importance": waterfall,
                "feature_importance_historical": area_plot,
                "factor_contribution": factor_contribution,
                "factor_attribution": factor_attribution,
                "summary_stats": {
                    "overview": insight_overview,
                    'comparables': comparable_recommendations,
                    'news': news,
                    "key_values": key_values,
                },
                "header_info": header_info,
            }




