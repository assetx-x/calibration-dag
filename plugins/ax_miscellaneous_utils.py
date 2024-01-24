import functools
from datetime import datetime

import json
import pandas as pd
import re
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from pandas.tseries.offsets import BDay


def format_influence_label_data(data):
    new_label = str(round(data,3))
    if '-' in new_label:
        return new_label
    else:
        return '+' + new_label


def combine_values_titles(values, titles):
    combined_list = []

    for value, title in zip(values, titles):
        rounded_value = round(value, 4)
        combined_string = f"{rounded_value} = {title}"
        combined_list.append(combined_string)

    return combined_list

def df_to_dicts_plotly(df):
    # Initialize the list
    data_list = []

    # Loop through each column in the DataFrame
    for idx, col in enumerate(df.columns):
        # Basic structure
        data_dict = {
            'x': df.index.tolist(),
            'y': df[col].values.tolist(),
            'name': col,
            'stackgroup': 'one'
        }

        # Add the 'groupnorm' key only for the first column
        if idx == 0:
            data_dict['groupnorm'] = 'percent'

        # Append the dictionary to the list
        data_list.append(data_dict)

    return data_list

def prediction_waterfall_ploty(data, feature_dictionary, formal_name_mapper):
    value_shap_example = data[feature_dictionary]
    value_shap_example_recent = value_shap_example.iloc[-1].reindex(
        abs(value_shap_example.iloc[-1]).sort_values(ascending=False
                                                     ).index)

    if len(value_shap_example_recent) > 9:
        top9 = value_shap_example_recent[0:9] * 100
        top9.rename(index=formal_name_mapper, inplace=True)
        bottom9 = value_shap_example_recent[9:] * 100
        bottom9_sum = bottom9.sum()
        bottom_labels = '{} other features'.format(len(bottom9))
        labels = [bottom_labels] + top9.index.tolist()
        values = [bottom9_sum] + top9.values.tolist()
        labels_rounded_text = [format_influence_label_data(i) for i in values]
        axis_titles = combine_values_titles(values, labels)
        measure = ['relative'] * len(axis_titles)



    else:

        value_shap_example_recent.rename(index=formal_name_mapper, inplace=True)
        labels = value_shap_example_recent.index.tolist()
        values = (value_shap_example_recent.values * 100).tolist()
        labels_rounded_text = [format_influence_label_data(i) for i in values]
        axis_titles = combine_values_titles(values, labels)
        measure = ['relative'] * len(axis_titles)

    return {'measure':measure,
            'x':axis_titles,
            'y':values,
            'text':labels_rounded_text}


def plotly_area_graph_data(value_shap_example_abs,formal_name_mapper):
    top_long_factors = value_shap_example_abs.sum().sort_values(ascending=False)

    factor_cutoff = 9
    if len(top_long_factors) > factor_cutoff:
        top_factors = top_long_factors[0:factor_cutoff].index.tolist()
        topx = value_shap_example_abs[top_factors]
        topx.rename(columns=formal_name_mapper, inplace=True)

        bottom_factors = top_long_factors[factor_cutoff:].index.tolist()
        bottomx = value_shap_example_abs[bottom_factors]
        bottomx_sum = bottomx.sum(axis=1)
        bottom_labels = '{} other features'.format(len(bottom_factors))

        topx[bottom_labels] = bottomx_sum
        all_factors_df = topx * 100

        return df_to_dicts_plotly(all_factors_df)

    else:
        topx = value_shap_example_abs.rename(columns=formal_name_mapper)
        all_factors_df = topx * 100

        return df_to_dicts_plotly(all_factors_df)

def plotly_area_graph_data_overall(value_shap_example_abs):

    all_factors_df = value_shap_example_abs * 100

    return df_to_dicts_plotly(all_factors_df)


def prediction_waterfall_plotly_full(data, expected_value):
    recent_exposures_raw = data.iloc[-1]
    recent_exposures_raw = recent_exposures_raw.reindex(abs(recent_exposures_raw).sort_values().index)
    feature_names = ['Market'] + recent_exposures_raw.index.tolist()
    values = [(expected_value * 100)] + list((recent_exposures_raw.values * 100))
    labels_rounded_text = [format_influence_label_data(i) for i in values]
    axis_labels = combine_values_titles(values, feature_names)
    measure = ['relative'] * len(axis_labels)
    return {'measure':measure,
            'x':axis_labels,
            'y':values,
            'text':labels_rounded_text}

def convert_and_round(value):
    return round(value * 100, 2)


def find_tradingview_image(ticker):
    # May be in one exchange or the other will have to check both
    url = "https://www.tradingview.com/symbols/NASDAQ-{}/".format(ticker)
    r = requests.get(url)
    example_text = r.text
    example_text
    split = example_text.split(" src=")
    holder = [i for i in split if ".svg" in i]
    image = holder[1].split(" ")[0][1:-1]
    # if it parses the download from apple store image, it means we got it wrong and to use the other exchange NYSE
    if (
        image
        == "https://static.tradingview.com/static/images/svg/app-store-badge/light-theme/app-store-badge-en.svg"
    ):
        url = "https://www.tradingview.com/symbols/NYSE-{}/".format(ticker)
        r = requests.get(url)
        example_text = r.text
        example_text
        split = example_text.split(" src=")
        holder = [i for i in split if ".svg" in i]
        image = holder[1].split(" ")[0][1:-1]

    return image

def image_override(ticker, image):
    override_dict = {
        "GOOG": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcR4cj2hofQiP3hTnXiHfft9QT_Xoie7BpHWTi-P-KKZGHDbMOBX&usqp=CAU",
        "GOOGL": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcR4cj2hofQiP3hTnXiHfft9QT_Xoie7BpHWTi-P-KKZGHDbMOBX&usqp=CAU",
        "AAPL": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSuvkS_Q4vi5Q7bA0u3IiW9iN6WwIT0wNQ6PgPFhf5k9RK6nqLRR7wu1Yc&usqp=CAU",
        "TSLA": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcR5ZvRdX6EZNVtF-vYJauJ5d5sBg8RI9jXKt8ZyyzMG5aywFKRq&usqp=CAU",
        "MSFT": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRlicwmttfKbb4KEUQmjClemUmAqTX36zUc_tDpi3jSIQKNMNSk&usqp=CAU",
        "MS": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQS76uGl5kaxhtvYmrEkgal5WurEYyBK2yDvm6D96aLu4FDZVQ4dEuiPrI&usqp=CAU",
        "JPM": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcT_qtr1hl32z7nsEeWvCwiXuEpl4wt27fDQN9M3yGw&usqp=CAU",
        "placeholder": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSDxERAYWuaehTgMPu7oHmymoJ4ZgrcaoBSlFVIKgAh8o2YLPoqioym7mA&usqp=CAU",
    }

    if ticker in override_dict:
        return override_dict[ticker]
    else:
        return image

def image_fetch(ticker):
    try:
        # img_data = image_override(ticker, get_suggested_search_data(company_name + ' ' + 'logo'))
        img_data = find_tradingview_image(ticker)
    except Exception:
        img_data = image_override("placeholder", "N/A")

    return img_data