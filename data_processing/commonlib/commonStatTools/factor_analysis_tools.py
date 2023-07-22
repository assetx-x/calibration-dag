from __future__ import division

import pandas as pd
# TODO: must be updated later
#from pandas.stats.ols import MovingOLS
import numpy as np

# Needs sklearn 0.19.0
from sklearn.linear_model import HuberRegressor

# Wrapper for robust regression
def huber_regression(X, y, epsilon=1.35, fit_intercept=True, alpha=0.0001, warm_start=False, tol=1e-5, max_iter=100):
    model = HuberRegressor(epsilon=epsilon, max_iter=max_iter, alpha=alpha, warm_start=warm_start,
                           fit_intercept=fit_intercept, tol=tol)
    X = X.values
    y = y.values
    if X.ndim == 1:
        X = X.reshape((X.size, 1))
    model.fit(X, y)
    return np.insert(model.coef_, 0, model.intercept_)

if __name__=="__main__":
    # This is for testing modules
    import os
    DATA_DIR = r"D:\DCM\temp\etf_experiment"
    data_location = os.path.join(DATA_DIR, "pairs_trading_data_old.h5")

    target_fields = ['volatility_data', 'beta_data', 'correlation_data', 'daily_price_data', 'etf_information',
                     'sector_industry_mapping', 'daily_index_data', 'overnight_return_data',
                     'technical_indicator_data']
    results = {}
    with pd.HDFStore(data_location, "r") as store:
        for el in target_fields:
            print(el)
            results[el] = store[el]

    daily_price_data = results['daily_price_data']
    dropna_pctg = 0.1
    start_date = pd.Timestamp("2014-01-01")
    end_date = pd.Timestamp("2015-06-30")
    normalize = False
    price_column = "close"

    pivot_data = daily_price_data[["date", "ticker", price_column]] \
        .pivot_table(index="date", values = price_column, columns="ticker").sort_index()
    return_data = pd.np.log(pivot_data).diff()
    return_data = return_data.ix[start_date:end_date]
    required_records = int(len(return_data)*(1-dropna_pctg))
    return_data = return_data.dropna(thresh=required_records, axis=1).fillna(value=0.0)
    if normalize:
        return_data = (return_data - return_data.mean())/return_data.std()

    huber_regression(return_data[["JPM", "HON"]], return_data["SPY"], fit_intercept=True)
    print("done")