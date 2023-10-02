from core_classes import GCPReader,download_yahoo_data,DataReaderClass
from market_timeline import marketTimeline
import pandas as pd
from fredapi import Fred
from datetime import datetime

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path



class QuandlDataCleanup(DataReaderClass):
    '''
    Cleans Quandl Data and resamples to daily, monthly, and quarterly frequencies
    '''

    PROVIDES_FIELDS = ["quandl_daily", "quandl_quarterly"]
    REQUIRES_FIELDS = ["daily_price_data", "raw_quandl_data"]

    def __init__(self):
        self.data = None
        self.monthly = None
        self.quarterly = None
        # CalibrationTaskflowTask.__init__(self)

    def _resolve_duplicates(self, raw):
        raw['fq'] = pd.to_datetime(raw['calendardate'])
        raw['filing_date'] = pd.to_datetime(raw['datekey'])
        raw['date'] = (raw['filing_date'].dt.to_period('M') + 1).dt.to_timestamp(freq='B')
        raw.drop(['calendardate', 'datekey', 'reportperiod', 'lastupdated'], axis=1, inplace=True)
        raw = raw[['dcm_security_id', 'date', 'filing_date', 'fq'] + \
                  raw.columns.drop(['dcm_security_id', 'date', 'filing_date', 'fq']).tolist()]

        raw.sort_values(['dcm_security_id', 'filing_date', 'fq', 'dimension'], inplace=True)
        raw.drop_duplicates(subset=['dcm_security_id', 'filing_date', 'fq'], keep='first', inplace=True)
        raw.drop_duplicates(subset=['dcm_security_id', 'date'], keep='last', inplace=True)
        return raw

    def _get_data_lineage(self, **kwargs):
        pass

    def _prepare_to_pull_data(self, **kwargs):
        pass

    def _get_close_volume_filing_periods(self, raw, price, dates):
        close = price.pivot_table(index='date', columns='dcm_security_id', values='close', dropna=False)
        close = close.reindex(dates).sort_index()
        close = close.ffill(limit=5)
        close = close.stack().reset_index().rename(columns={0: 'close'})
        volume = price.pivot_table(index='date', columns='dcm_security_id', values='volume', dropna=False)
        volume = volume.reindex(dates).sort_index()
        volume = volume.ffill(limit=5)
        volume = volume.stack().reset_index().rename(columns={0: 'volume'})
        filing_periods = raw.groupby(['dcm_security_id', 'dimension']).size().unstack().idxmax(axis=1)
        filing_periods = pd.Series(filing_periods.index, index=filing_periods)
        return close, volume, filing_periods

    def _process_quarterly(self, raw, arq):
        quarterly = arq.reset_index()
        quarterly.sort_values(['dcm_security_id', 'fq', 'filing_date'], inplace=True)
        self.quarterly = quarterly.groupby(['dcm_security_id', 'fq']).last().reset_index()

    def _process_monthly(self, raw, close, arq, art, months):
        arq = arq.unstack().reindex(months)
        art = art.unstack().reindex(months)
        # arq.ffill(limit=3, inplace=True)
        # art.ffill(limit=12, inplace=True)
        arq.ffill(limit=4, inplace=True)
        art.ffill(limit=13, inplace=True)
        monthly = pd.concat([arq.stack().reset_index(), art.stack().reset_index()], ignore_index=True)
        monthly = pd.merge(monthly, close, how='left', on=['date', 'dcm_security_id'])
        monthly['marketcap'] = monthly['sharesbas'] * monthly['close'] * monthly['sharefactor']
        self.monthly = monthly.sort_values(['dcm_security_id', 'date'])

    def _process_daily(self, raw, close, volume, filing_periods, dates):
        arq_temp = raw[raw['dcm_security_id'].isin(filing_periods['ARQ'])].drop(['dimension', 'date'], axis=1) \
            .rename(columns={'filing_date': 'date'})
        arq = arq_temp.set_index(['date', 'dcm_security_id'])

        dummy_date = pd.Timestamp("2040-12-31")
        dummy_security_id = 9999999
        if "ART" in filing_periods:
            try:
                art = raw[raw['dcm_security_id'].isin(filing_periods['ART'])].drop('dimension', axis=1) \
                    .set_index(['date', 'dcm_security_id'])

            except Exception:
                art = raw[raw['dcm_security_id'] == filing_periods['ART']].drop('dimension', axis=1) \
                    .set_index(['date', 'dcm_security_id'])
        else:
            art = pd.DataFrame(columns=arq_temp.columns)
            art.loc[0, "dcm_security_id"] = dummy_security_id
            for col in ["date", "fq"]:
                art.loc[0, col] = dummy_date
            art.set_index(["date", "dcm_security_id"], inplace=True)

        arq = arq.unstack().reindex(dates).shift(1)
        art = art.unstack().reindex(dates).shift(1)
        arq.ffill(limit=126, inplace=True)
        art.ffill(limit=400, inplace=True)
        daily = pd.concat([arq.stack().reset_index(), art.stack().reset_index()], ignore_index=True)
        daily = pd.merge(daily, close, how='left', on=['date', 'dcm_security_id'])
        daily = pd.merge(daily, volume, how='left', on=['date', 'dcm_security_id'])
        daily['marketcap'] = daily['sharesbas'] * daily['close'] * daily['sharefactor']
        self.data = daily.sort_values(['dcm_security_id', 'date'])

    def do_step_action(self, **kwargs):
        price = kwargs['daily_price_data'].copy(deep=True)
        price = price.drop(['ticker'], axis=1)
        # price.rename(columns={"ticker": "dcm_security_id"}, inplace=True)
        raw = kwargs['raw_quandl_data'].copy(deep=True)

        raw = self._resolve_duplicates(raw)
        # Old version
        # dates = pd.date_range(raw['date'].min(), max(raw['date'].max(), pd.datetime.today()), freq='B', name='date')
        # New Version
        dates = pd.date_range(raw['date'].min(), max(raw['date'].max(), pd.to_datetime(datetime.today())),
                              freq='B', name='date')
        months = raw['date'].drop_duplicates().sort_values()
        close, volume, filing_periods = self._get_close_volume_filing_periods(raw, price, dates)

        arq_temp = raw[raw['dcm_security_id'].isin(filing_periods['ARQ'])].drop('dimension', axis=1)
        arq = arq_temp.set_index(['date', 'dcm_security_id'])
        dummy_date = pd.Timestamp("2040-12-31")
        dummy_security_id = 9999999
        if "ART" in filing_periods:
            art = raw[raw['dcm_security_id'].isin(filing_periods['ART'])].drop('dimension', axis=1) \
                .set_index(['date', 'dcm_security_id'])
        else:
            art = pd.DataFrame(columns=arq_temp.columns)
            art.loc[0, "dcm_security_id"] = dummy_security_id
            for col in ["date", "filing_date", "fq"]:
                art.loc[0, col] = dummy_date
            art.set_index(["date", "dcm_security_id"], inplace=True)

        self._process_quarterly(raw, arq)
        self._process_monthly(raw, close, arq, art, months)
        self._process_daily(raw, close, volume, filing_periods, dates)

        self.data = self.data[self.data["dcm_security_id"] < dummy_security_id].reset_index(drop=True)
        self.monthly = self.monthly[self.monthly["dcm_security_id"] < dummy_security_id].reset_index(drop=True)
        self.quarterly = self.quarterly[self.quarterly["dcm_security_id"] < dummy_security_id].reset_index(drop=True)

        self.data["dcm_security_id"] = self.data["dcm_security_id"].astype(int)
        self.monthly["dcm_security_id"] = self.monthly["dcm_security_id"].astype(int)
        self.quarterly["dcm_security_id"] = self.quarterly["dcm_security_id"].astype(int)
        return {"quandl_daily": self.data, "quandl_monthly": self.monthly, "quandl_quarterly": self.quarterly}





############# AIRFLOW PARAMS ##########

QuandlDataCleanup_PARAMS = {"params": {},
                          "start_date": RUN_DATE,
                          'class': QuandlDataCleanup,
                          'provided_data': {'quandl_daily': construct_destination_path('fundamental_cleanup'),
                                            'quandl_monthly': construct_destination_path('fundamental_cleanup'),
                                            'quandl_quarterly':construct_destination_path('fundamental_cleanup')},
                          'required_data': {'daily_price_data': construct_required_path('data_pull','daily_price_data'),
                                            'raw_quandl_data': construct_required_path('data_pull','raw_quandl_data')
                                            }
                          }