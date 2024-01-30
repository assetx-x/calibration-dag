import numpy as np

from core_classes import GCPReader, download_yahoo_data, DataReaderClass
import pandas as pd
from datetime import datetime
import gc

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')

from core_classes import construct_required_path, construct_destination_path


class CalculateDerivedQuandlFeatures(DataReaderClass):
    '''
    Creates derived fundamental features from the clean quandl data sets
    '''

    PROVIDES_FIELDS = ["fundamental_features"]
    REQUIRES_FIELDS = ["industry_map", "daily_price_data", "quandl_daily", "quandl_quarterly"]

    def __init__(self):
        self.data = None
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _construct_simple_features(self, sector, daily):
        daily = pd.merge(daily, sector[['dcm_security_id', 'GICS_Sector', 'GICS_IndustryGroup']], how='left',
                         on='dcm_security_id')
        daily['GICS_Sector'] = daily['GICS_Sector'].fillna('n/a')
        daily['GICS_IndustryGroup'] = daily['GICS_IndustryGroup'].fillna('n/a')
        daily['log_mktcap'] = np.log(daily['marketcap'])
        daily['log_dollar_volume'] = np.log(daily['close'] * daily['volume'])
        daily['bm'] = 1.0 / daily['pb']
        daily['bm_ia_sector'] = daily['bm'] - daily.groupby(['date', 'GICS_Sector'])['bm'].transform('mean')
        daily['bm_ia_indgrp'] = daily['bm'] - daily.groupby(['date', 'GICS_IndustryGroup'])['bm'].transform('mean')
        daily['debt2equity'] = daily['debt'] / daily['equity']
        daily['fcf_yield'] = daily['fcf'] / daily['marketcap']
        daily['gp2assets'] = daily['gp'] / daily['assets']
        daily['turnover'] = daily['volume'] / daily['shareswa']

        # 2019/08/23 These are added
        daily['capex_yield'] = daily['capex'] / daily['marketcap']
        daily['current_to_total_assets'] = daily['assetsc'] / daily['assets']
        daily['inventory_to_revenue'] = daily['inventory'] / daily['revenue']
        daily['ncf_yield'] = daily['ncf'] / daily['marketcap']
        daily['ncfdiv_yield'] = daily['ncfdiv'] / daily['marketcap']
        daily['ncfcommon_yield'] = daily['ncfcommon'] / daily['marketcap']
        daily['ncfdebt_yield'] = daily['ncfdebt'] / daily['marketcap']
        daily['retearn_to_liabilitiesc'] = daily['retearn'] / daily['liabilitiesc']
        daily['retearn_to_total_assets'] = daily['retearn'] / daily['assets']
        daily['rnd_to_revenue'] = daily['rnd'] / daily['revenue']

        for col in ['investments', 'ncff', 'revenue', 'ncfi']:
            new_col = "{0}_yield".format(col)
            daily[new_col] = daily[col] / daily["assets"]
        return daily

    def _construct_quarterly_pivot_data(self, daily, quarterly):
        df_dict = dict()
        for key in ["assets", "revenue", "capex", "inventory", "shareswa"]:
            df_dict[key] = quarterly.pivot_table(index='fq', columns='dcm_security_id', values=key,
                                                 dropna=False).sort_index()

        for key in ["assets", "revenue", "shareswa"]:
            df_dict[key + "_growth"] = df_dict[key].pct_change(4, fill_method=None)

        df_dict["delta_capex"] = df_dict["capex"].diff(4) / df_dict["assets"].rolling(4, min_periods=1).mean()
        df_dict["delta_inventory"] = df_dict["inventory"].diff(4) / df_dict["assets"].rolling(4, min_periods=1).mean()

        for key in ["assets_growth", "revenue_growth", "shareswa_growth", "delta_capex", "delta_inventory"]:
            df_dict[key] = df_dict[key].stack().reset_index().rename(columns={0: key})

        turnover = daily.pivot_table(index='date', columns='dcm_security_id', values='turnover',
                                     dropna=False).sort_index()
        std_turn = turnover.rolling(21).std().shift(1)
        std_turn = std_turn.stack().reset_index().rename(columns={0: 'std_turn'})

        # This is just for demo

        for key in ["revenue", "assets"]:
            df_dict[key + "_growth_qoq"] = df_dict[key].pct_change(1, fill_method=None)
            df_dict[key + "_growth_qoq"] = df_dict[key + "_growth_qoq"].stack().reset_index().rename(columns={0:
                                                                                                                  key + "_growth_qoq"})
            df_dict["future_" + key + "_growth_qoq"] = df_dict[key].pct_change(1, fill_method=None).shift()
            df_dict["future_" + key + "_growth_qoq"] = df_dict[
                "future_" + key + "_growth_qoq"].stack().reset_index().rename(columns={0:
                                                                                           "future_" + key + "_growth_qoq"})

        while df_dict:
            key, df = df_dict.popitem()
            if key in ["assets_growth", "revenue_growth", "shareswa_growth", "delta_capex",
                       "delta_inventory", "revenue_growth_qoq", "future_revenue_growth_qoq",
                       "assets_growth_qoq", "future_assets_growth_qoq"]:
                daily = pd.merge(daily, df, how='left', on=['dcm_security_id', 'fq'])
            gc.collect()
        daily = pd.merge(daily, std_turn, how='left', on=['dcm_security_id', 'date'])
        daily = daily.rename(columns={"assets_growth": "asset_growth", "assets_growth_qoq": "asset_growth_qoq",
                                      "future_assets_growth_qoq": "future_asset_growth_qoq"})
        return daily

    def _construct_liquidity_ratio_features(self, price, quandl):
        close = price.pivot_table(index='date', columns='dcm_security_id', values='close', dropna=False).sort_index()
        ret = close.pct_change()
        retvol = ret.rolling(21).std()
        maxret = ret.rolling(21).max()
        volume = price.pivot_table(index='date', columns='dcm_security_id', values='volume', dropna=False).sort_index()
        dolvol = close * volume
        high = price.pivot_table(index='date', columns='dcm_security_id', values='close', dropna=False).sort_index()
        high52wk = high.rolling(252).max()
        rel_to_high = close.div(high52wk)
        zerodays = (volume == 0).rolling(21).sum()
        amihud = (ret.abs() / dolvol).rolling(21).mean()

        quandl['ebitda_to_ev'] = quandl['ebitda'] / quandl['ev']
        quandl['sharey'] = (quandl['ncfdiv'] + quandl['ncfcommon'] + quandl['ncfdebt']) / quandl['marketcap']
        quandl['shares'] = quandl['sharesbas'] * quandl['sharefactor']
        shares = quandl.pivot_table(index='date', columns='dcm_security_id', values='shares', dropna=False).sort_index()
        chcsho = shares.pct_change(252)

        features = ['retvol', 'maxret', 'dolvol', 'rel_to_high', 'amihud', 'zerodays', 'chcsho']
        objects = [retvol, maxret, dolvol, rel_to_high, amihud, zerodays, chcsho]
        df = pd.concat((f.shift(1) for f in objects), keys=features, names=['feature'], axis=1).reset_index()
        df = df.set_index('date').stack().reset_index()
        df.dropna(how='all', subset=features, inplace=True)

        quandl = quandl[['date', 'dcm_security_id', 'ebitda_to_ev', 'ps', 'netmargin', 'sharey']]
        quandl["date"] = quandl["date"].apply(pd.Timestamp)
        df['amihud'] = -np.log(df['amihud'] + 1e-12)  # normalize the skew

        df["dcm_security_id"] = df["dcm_security_id"].astype(int)
        df['date'] = df['date'].apply(pd.Timestamp)
        merged = pd.merge(df, quandl, how='left', on=['date', 'dcm_security_id'])
        merged["dcm_security_id"] = merged["dcm_security_id"].astype(int)
        return merged

    def do_step_action(self, **kwargs):
        sector = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        price = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        # price = price.drop(['ticker'],axis=1)
        price.rename(columns={"ticker": "dcm_security_id"}, inplace=True)
        quandl = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        quarterly = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)

        daily = self._construct_simple_features(sector, quandl)
        daily = self._construct_quarterly_pivot_data(daily, quarterly)
        cols = ['log_mktcap', 'bm', 'bm_ia_sector', 'bm_ia_indgrp', 'assetturnover', 'gp2assets', 'divyield',
                'shareswa_growth', 'pe', 'log_dollar_volume', 'debt2equity', 'delta_inventory', 'delta_capex',
                'fcf_yield', 'asset_growth', 'revenue_growth', 'std_turn', 'investments_yield', 'ncff_yield',
                'revenue_yield', 'ncfi_yield']
        # Added 2019/08/23
        cols = cols + ["revenue_growth_qoq", "future_revenue_growth_qoq", "asset_growth_qoq", "future_asset_growth_qoq"]
        cols = cols + \
               ['assets', 'assetsc', 'capex', 'currentratio', 'ebitdamargin', 'ebitda', 'eps', 'epsdil',
                'equityusd', 'ev', 'fcf', 'gp', 'grossmargin', 'invcap', 'inventory', 'investments', 'ncf',
                'ncfcommon', 'ncfdebt', 'ncfdiv', 'ncff', 'ncfi', 'ncfinv', 'payoutratio',
                'pb', 'pe1', 'ps1', 'retearn', 'revenue', 'rnd', 'roa', 'roe', 'roic', 'ros', 'sps',
                'ncfdiv_yield', 'ncfdebt_yield', 'inventory_to_revenue', 'retearn_to_liabilitiesc',
                'current_to_total_assets', 'capex_yield', 'ncfcommon_yield', 'ncf_yield', 'retearn_to_total_assets',
                'rnd_to_revenue']

        cols = list(set(cols))
        cols.sort()

        daily = daily[['date', 'dcm_security_id', 'fq'] + cols]
        daily["dcm_security_id"] = daily["dcm_security_id"].astype(int)
        extra_features = self._construct_liquidity_ratio_features(price, quandl)
        mapper = kwargs['security_master'][['dcm_security_id', 'ticker']].set_index(['dcm_security_id']).to_dict()[
            'ticker']
        daily['date'] = daily['date'].apply(pd.Timestamp)
        daily = pd.merge(daily, extra_features, how="left", on=["date", "dcm_security_id"]) \
            .rename(columns={"dcm_security_id": "ticker"})
        #daily['ticker'] = daily['ticker'].map(mapper)
        self.data = daily
        return self.data


############# AIRFLOW ############


CalculateDerivedQuandlFeatures_PARAMS = {
    'params': {},
    'class': CalculateDerivedQuandlFeatures,
    'start_date': RUN_DATE,
    'provided_data': {'fundamental_features': construct_destination_path('derived_fundamental')},
    'required_data': {"industry_map": construct_required_path('data_pull', 'industry_mapper'),
                      'daily_price_data': construct_required_path('data_pull', 'daily_price_data'),
                      'quandl_daily': construct_required_path('fundamental_cleanup', 'quandl_daily'),
                      'quandl_quarterly': construct_required_path('fundamental_cleanup', 'quandl_quarterly'),
                      'security_master': construct_required_path('data_pull', 'security_master'),
                      }
}
