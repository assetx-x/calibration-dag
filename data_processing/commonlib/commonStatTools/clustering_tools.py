import pandas as pd
import numpy as np
import os
from sklearn.decomposition import PCA#, RandomizedPCA
from sklearn.covariance import GraphLasso, GraphLassoCV, shrunk_covariance
from sklearn.cluster import KMeans
from scipy.stats import zscore
from commonlib.market_timeline import marketTimeline
from commonlib.subjects import *
from scipy.stats import zscore
# Needs sklearn 0.19.0
from sklearn.linear_model import HuberRegressor, LinearRegression
from pandas.tseries.frequencies import to_offset
from numpy.matlib import repmat

RANDOM_STATE = 12345

def transform_data(data, take_return=False, return_type='log', lookback=None, normalize=False):
    if take_return:
        if return_type=='pct_change':
            data = data.pct_change().fillna(0.0)
        elif return_type=='log':
            data = pd.np.log(data).diff().fillna(0.0)
        else:
            raise ValueError("return_type must be pct_change or log. Given unknown type {0}".format(return_type))
    start_date = data.index[-1] - to_offset(lookback) if lookback else None
    data = data.loc[start_date:].fillna(method='ffill')
    if normalize:
        data = (data-data.mean())/data.std()
    return data

class CalculateBeta(object):
    # If take_return=True, it takes the returns first
    def __init__(self):
        self.model = None
        self.betas = None

    def __fit(self, X, y, use_robust_method, fit_intercept, epsilon, regularization):
        if use_robust_method:
            model = HuberRegressor(epsilon=epsilon, alpha=regularization, warm_start=False,
                                   fit_intercept=fit_intercept, max_iter=300)
            model.fit(X, y)
        else:
            model = LinearRegression(fit_intercept=fit_intercept)
            model.fit(X, y)
        self.model = model

    def __call__(self, data_matrix, dependent_var, take_return=True, return_type='log', fit_intercept=True,
                 lookback=None, normalize=False, use_robust_method=False, epsilon=1.35, regularization=0.0001):
        if not (isinstance(data_matrix, pd.Series) or isinstance(data_matrix, pd.DataFrame)):
            raise ValueError("data_matrix must be a pandas Series or DataFrame. data_matrix given is of type {0}"\
                             .format(data_matrix.__class__))
        if not (isinstance(dependent_var, pd.Series) or isinstance(dependent_var, pd.DataFrame)):
            raise ValueError("dependent_var must be a pandas Series or DataFrame. dependent_var given is of type {0}"\
                             .format(dependent_var.__class__))
        data_matrix = transform_data(data_matrix, take_return, return_type, lookback, normalize)
        dependent_var = transform_data(dependent_var, take_return, return_type, lookback, normalize)
        common_timeline = dependent_var.index.intersection(data_matrix.index)
        if not len(common_timeline):
            raise ValueError("No common timestamps between dependent_var and data_matrix.")
        data_matrix = data_matrix.loc[common_timeline]
        dependent_var = dependent_var.loc[common_timeline]

        X = data_matrix.values
        y = dependent_var.values
        if X.ndim == 1:
            X = X.reshape((X.size, 1))
        if X.shape[0] != y.shape[0]:
            raise ValueError("The sample size of the data_matrix {0} and dependent_var {1} do not match"\
                             .format(X.shape[0], y.shape[0]))
        if (use_robust_method and (len(y.shape)>1) and (y.shape[1]>1)):
            raise ValueError("Multiple dependent variables at once is not supported for the robust method.")
        self.__fit(X, y, use_robust_method, fit_intercept, epsilon, regularization)
        if isinstance(dependent_var, pd.DataFrame):
            dependent_var_labels = list(dependent_var.columns)
        else:
            dependent_var_labels = [dependent_var.name] or [0]
        if isinstance(data_matrix, pd.DataFrame):
            column_labels = list(data_matrix.columns)
        else:
            column_labels = [data_matrix.name] or [0]

        columns = map(lambda x:"{0}_beta".format(x), column_labels)
        coeffs = self.model.coef_
        if coeffs.ndim==1:
            coeffs = coeffs.reshape((coeffs.size, 1)).T
        betas = pd.DataFrame(coeffs, index=dependent_var_labels, columns=columns)
        if fit_intercept:
            betas['intercept'] = self.model.intercept_.T
        else:
            betas['intercept'] = 0.0
        self.betas = betas
        return self.betas

class PrincipalComponents(object):
    # If called without a dependent_var (transform variable) returns the components, if called with a dependent_var
    # returns a tuple of the components, and the loadings
    def __init__(self):
        self.model = None
        self.components = None
        self.coef = None

    def __fit(self, data_matrix, n_components, method, iterated_power):
        #TODO: May accommodate other models e.g. TGA, PPCA or RobPCA in the future
        model = PCA(n_components=n_components, svd_solver=method, iterated_power=iterated_power,
                    random_state=RANDOM_STATE)
        model.fit(data_matrix)
        self.model = model

    def __call__(self, data_matrix, dependent_var=None, n_components=3, method='randomized', take_return=True,
                 return_type='log', lookback=None, normalize=True, demean=True, iterated_power=4):
        if not (isinstance(data_matrix, pd.Series) or isinstance(data_matrix, pd.DataFrame)):
            raise ValueError("data_matrix must be a pandas Series or DataFrame. data_matrix given is of type {0}"\
                             .format(data_matrix.__class__))
        data_matrix = transform_data(data_matrix, take_return, return_type, lookback, normalize)
        if demean and (not normalize):
            data_matrix = data_matrix - data_matrix.mean()
        self.__fit(data_matrix, n_components, method, iterated_power)
        pc = self.model.transform(data_matrix)
        self.components = pd.DataFrame(zscore(pc), index=data_matrix.index,
                                       columns=map(lambda x:"PC_{0}".format(x), range(1, n_components+1)))

        if dependent_var is not None:
            if not (isinstance(dependent_var, pd.Series) or isinstance(dependent_var, pd.DataFrame)):
                raise ValueError("dependent_var must be a pandas Series or DataFrame. dependent_var given is of type {0}"\
                                 .format(dependent_var.__class__))
            dependent_var = transform_data(dependent_var, take_return, return_type, lookback, normalize)
            if demean and (not normalize):
                dependent_var = dependent_var - dependent_var.mean()
            data_matrix = self.components.copy()
            betas = get_beta(data_matrix, dependent_var, normalize=False, take_return=False, fit_intercept=False)
            self.betas = betas.drop(['intercept'], axis=1)
            return self.components, self.betas
        else:
            return self.components

class KMeansCluster(object):
    def __init__(self):
        self.model = None
        self.clusters = None

    def __fit(self, feature_matrix, n_clusters, method):
        #TODO: will accommodate different algorithms in the future (e.g. minibatch)
        model = KMeans(n_clusters=n_clusters, algorithm=method, random_state=RANDOM_STATE)
        model.fit(feature_matrix)
        self.model = model

    def __call__(self, feature_matrix, n_clusters=3, method='auto', feature_weight_list=None):
        #TODO: add number of cluster diagnostics, and auto determination in the future (gap statistics, elbow analysis)
        if not (isinstance(feature_matrix, pd.Series) or isinstance(feature_matrix, pd.DataFrame)):
            raise ValueError("feature_matrix must be a pandas Series or DataFrame. feature_matrix given is of type {0}"\
                             .format(feature_matrix.__class__))
        feature_matrix = feature_matrix.copy()
        if feature_weight_list:
            if not isinstance(feature_weight_list, list):
                raise ValueError("feature_weight_list must be a list. Given type is {0}"\
                                 .format(feature_weight_list.__class__))
            feature_dim = 1 if (feature_matrix.ndim==1) else feature_matrix.shape[1]
            if len(feature_weight_list)!=feature_dim:
                raise ValueError("The length of feature_weight_list {0} does not match the number of features in "
                                 "feature_matrix {1}".format(len(feature_weight_list), feature_dim))
            weight_array = np.sqrt(feature_weight_list)
            weight_matrix = repmat(weight_array, feature_matrix.shape[0], 1)
            feature_matrix *= weight_matrix

        self.__fit(feature_matrix, n_clusters, method)
        cluster_ids = list(self.model.labels_)
        feature_matrix['cluster_id'] = cluster_ids
        return feature_matrix

class GraphicalLassoPartiallyDependentGroups(object):
    def __init__(self):
        self.model = None
        self.dependent_groups = None
        self.precision_matrix = None
        self.covariance_matrix = None
        self.alpha = None

    def __fit(self, data_matrix, shrinkage, alpha, max_iter, tol, use_cross_validation, precision_threshold,
              covariance_threshold):
        if shrinkage:
            cmat = shrunk_covariance(data_matrix.cov(), shrinkage=shrinkage)
        else:
            cmat = data_matrix.cov()
        if use_cross_validation:
            model = GraphLassoCV(max_iter=max_iter, tol=tol).fit(cmat)
            self.alpha = model.alpha_
        else:
            model = GraphLasso(alpha=alpha, max_iter=max_iter, tol=tol).fit(cmat)
            self.alpha = model.alpha
        precision = pd.DataFrame(model.get_precision(), index=data_matrix.columns, columns=data_matrix.columns)
        precision[abs(precision)<=precision_threshold] = 0
        self.precision_matrix = precision
        cov = pd.DataFrame(model.covariance_, index=data_matrix.columns, columns=data_matrix.columns)
        cov[abs(cov)<=covariance_threshold] = 0
        self.covariance_matrix = cov

    def _filter_row(self, row, precision_matrix, max_num):
        num = len(row[0])
        if num<=max_num:
            basket = row[0]
        else:
            current_pmat = abs(precision_matrix[list(row[0])])[row['ticker']].copy()
            current_pmat.sort_values(ascending=False, inplace=True)
            basket = set(current_pmat[:max_num].index)
        return basket

    def __filter(self, min_group_size, max_group_size):
        condition_matrix = self.precision_matrix.abs()>0.0
        good_names = condition_matrix.apply(lambda x:set(condition_matrix.columns[x.values]), axis=1)
        baskets = good_names[good_names.apply(lambda x:len(x)>=min_group_size)]
        final_baskets = baskets.reset_index()\
            .apply(lambda x:self._filter_row(x, self.precision_matrix, max_group_size), axis=1)
        if len(final_baskets):
            baskets = list(final_baskets.values)
            unique_baskets = [list(x) for x in set(tuple(x) for x in baskets)]
            dependent_groups = unique_baskets
        else:
            dependent_groups = []
        self.dependent_groups = dependent_groups

    def __call__(self, data_matrix, min_group_size=5, max_group_size=18, take_return=True,
                 return_type='log', lookback=None, normalize=True, demean=True, shrinkage=0.1, alpha=0.01, max_iter=200, tol=5e-3,
                 use_cross_validation=False, precision_threshold=1e-3, covariance_threshold=1e-3):
        if not (isinstance(data_matrix, pd.Series) or isinstance(data_matrix, pd.DataFrame)):
            raise ValueError("data_matrix must be a pandas Series or DataFrame. data_matrix given is of type {0}"\
                             .format(data_matrix.__class__))
        data_matrix = transform_data(data_matrix, take_return, return_type, lookback, normalize)
        if demean and (not normalize):
            data_matrix = data_matrix - data_matrix.mean()

        self.__fit(data_matrix, shrinkage, alpha, max_iter, tol, use_cross_validation, precision_threshold,
                   covariance_threshold)
        self.__filter(min_group_size, max_group_size)
        return self.dependent_groups, self.precision_matrix, self.alpha

get_beta = CalculateBeta()
get_pca = PrincipalComponents()
get_kmeans = KMeansCluster()
get_glasso_groups = GraphicalLassoPartiallyDependentGroups()

if __name__=="__main__":
    DATA_DIR = r"D:\DCM\temp\etf_experiment"
    def load_data(data_location, target_fields=['daily_price_data', 'merged_data']):
        results = {}
        with pd.HDFStore(data_location, "r") as store:
            for el in target_fields:
                print(el)
                results[el] = store["/pandas/{0}".format(el)]
        print("done")
        return results

    data_location = os.path.join(DATA_DIR, "mvp_data.h5")
    target_fields = ['beta_data', 'correlation_data', 'daily_price_data', 'etf_information']
    results = load_data(data_location, target_fields)
    start_date = pd.Timestamp('2016-01-01')
    end_date = pd.Timestamp('2017-10-18')
    daily_price_data = results['daily_price_data']
    beta_data = results['beta_data'].reset_index()
    etf_information = results["etf_information"]
    stock_list = list(etf_information[np.logical_not(etf_information['is_etf'])]['ticker'])
    etf_list = list(set(etf_information['ticker']) - set(stock_list))
    etf_list.sort()
    price_data = daily_price_data[["date", "ticker", "close"]] \
        .pivot_table(index="date", values = "close", columns="ticker").sort_index()
    price_data = price_data.ix[start_date-pd.tseries.frequencies.to_offset('1B'):end_date]
    dropna_pctg = 0.1

    required_records = int(len(price_data)*(1-dropna_pctg))
    price_data = price_data.dropna(thresh=required_records, axis=1).fillna(method='ffill')
    price_data = price_data[price_data.columns[price_data.columns.isin(etf_list)]]
    return_data = pd.np.log(price_data).diff()
    return_data = return_data.ix[start_date:end_date]

    X = price_data['SPY']
    y = price_data[['IEI', 'TLT']]
    beta = get_beta(X, y, lookback='126B', take_return=True, use_robust_method=False, fit_intercept=True)

    data_matrix = price_data
    pc, loadings = get_pca(data_matrix, data_matrix, n_components=4)

    kmean_clusters = get_kmeans(loadings, feature_weight_list=[0.5, 0.3, 0.1, 0.1])
    sub_group = list(kmean_clusters[kmean_clusters['cluster_id']==0].index)
    group_price_data = price_data[sub_group]
    glasso_groups, precision_matrix = get_glasso_groups(group_price_data)
    print("done")

