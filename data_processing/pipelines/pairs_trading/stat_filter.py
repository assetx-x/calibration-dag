import numpy as np
import pandas as pd
import statsmodels.tsa.tsatools as smtsa
from arch.unitroot import ADF, DFGLS, PhillipsPerron, KPSS
from statsmodels.regression.linear_model import OLS
from pykalman import KalmanFilter


from base import TaskWithStatus, PySparkTask
from pipelines.pairs_trading.common import add_constant, FLS


class StatFilterTask(TaskWithStatus, PySparkTask):
    def collect_keys(self):
        PySparkTask.run(self)
        return {}

    def run_spark(self):
        with self.input()["pairs"]["data"].open() as fd:
            delta_pairs_df = pd.read_csv(fd)
        with self.input()["close_prices"]["data"].open() as fd:
            price_df = pd.read_csv(fd, parse_dates=["date"], index_col=["date"])

        temp_df = delta_pairs_df[["Name_1", "Name_2", "delta_opt"]]
        pair_delta_df = pd.DataFrame(
            data=temp_df.values, index=xrange(len(temp_df)),
            columns=["Name_1", "Name_2", "delta"]
        )

        result_df = stat_filter_wrapper(
            stocks_df=price_df, pair_delta_df=pair_delta_df, stdate=self.start_date, endate=self.date,
            constant_term=True, test_fraction=0.0, spark_context=self.spark_context
        )

        with self.output()["data"].open("w") as fd:
            result_df.to_csv(fd, index=False)


def generate_residuals(X, y, constant_term=True, delta=0.0001, test_fraction=0.0):
    """
    Generates beta and residual vector using pykalman.
    Note that the optimal delta has already been trained.
    Hence, the residuals should be generated contemporaneously with test_fraction=0.0

    Args:
        X: observation matrix
        y: response variable
        constant_term: whether to include intercept (Boolean)
        delta: learning parameter in the dynamic beta regression
        test_fraction: in-sample fraction

    Returns:
        beta: time-series of dynamic betas
        bcov: time-series of transition covariances
        var_vect: time-series of prediction variances
        spread_vect: time-series of dynamic regression residuals (spreads)
    """
    nobs = y.size
    ntest = int(nobs * test_fraction)
    ntrain = nobs - ntest

    if constant_term:
        if X.ndim == 1:
            x_temp = add_constant(X.reshape((X.size, 1)))
        else:
            x_temp = add_constant(X.copy())
    else:
        if X.ndim == 1:
            x_temp = X.reshape((X.size, 1))
        else:
            x_temp = X.copy()

    nvars = x_temp.shape[1]
    observation_matrix = x_temp.reshape((nobs, 1, nvars))

    # Already added constant term don"t add again
    fls = FLS(constant_term=False)
    # Run EM on the training set
    fls.fit(x_temp[:ntrain], y[:ntrain], delta=delta, method="filter")
    k = fls.kalman
    kalman = KalmanFilter(
        transition_matrices=k.transition_matrices, observation_matrices=observation_matrix,
        observation_offsets=k.observation_offsets, transition_offsets=k.transition_offsets,
        observation_covariance=k.observation_covariance, transition_covariance=k.transition_covariance,
        initial_state_mean=k.initial_state_mean, initial_state_covariance=k.initial_state_covariance
    )

    beta, bcov = kalman.filter(y)

    var_vect = np.zeros((nobs, 1))
    for i in xrange(nobs):
        current_x = x_temp[i, :]
        trans_cov = kalman.transition_covariance
        obs_cov = kalman.observation_covariance

        if i == 0:
            fwd_trans_cov = np.zeros_like(bcov[i])
        else:
            fwd_trans_cov = bcov[i-1] + trans_cov
        pred_cov = current_x.dot(fwd_trans_cov).dot(current_x.T) + obs_cov
        var_vect[i] = pred_cov

    # Compute predictions over the hold-out set
    yhat = np.sum(beta*x_temp, axis=1)
    # Store the loss function values, the obs_cov (observation noise variance) and betas
    spread_vect = y - yhat
    if spread_vect.ndim == 1:
        spread_vect = spread_vect.reshape((X.size, 1))
    else:
        spread_vect = spread_vect.copy()

    return beta, bcov, var_vect, spread_vect


def stationarity_test(X, vol, sdflag=True, creg="c", rreg="dfgls", lags=None, autolag="AIC", alpha=0.05, thresh=1.0):
    """
    Stationarity Tests - ADF, PP, DFGLS, KPSS tests
    DFGLS and KPSS are preferred

    Args:
        X: the time series to be tested
        vol: the instantaneous volatility series.
        sdflag: if True we standardize X by vol
        creg: the intercept type where
            c = constant / nc = no constant / ct = time drift / ctt = quadratic time drift
        rreg: the unit-root test type for the regression residuals
            adf / pp / dfgls
            dfgls is the preferred method as it takes into account possible heteroskedasticity
        lags: the number of lags for the unit root test
        autolag: specifies the lag selection methodology in case of the adf or dfgls test
            AIC, BIC or t-stat
        alpha: the significance level
            since it is a table based lookup permissible values are 0.01, 0.05, 0.1

    Returns:
        pvalue: p-value of statistic
        stat: test statistic
        cvalue: critical value of the test at the alpha-significance level
        h: accept/reject null
        half_life: mean-reversion half life
        num_cross: number of 1-sigma crossings in the test period
    """

    creg = creg.lower()
    rreg = rreg.lower()
    
    if creg not in ["c", "nc", "ct", "ctt"]:
        raise ValueError("regression option is invalid")
    if rreg not in ["adf", "dfgls", "pp", "kpss"]:
        raise ValueError("unitroot test option is invalid")
    if autolag not in ["None", "AIC", "BIC", "t-stat"]:
        raise ValueError("autolag option is invalid")
    if alpha not in [0.01, 0.05, 0.10]:
        raise ValueError("Significance level must be 1%, 5% or 10%")

    if X.ndim == 1:
        X = X.reshape((X.size, 1))
    if vol.ndim == 1:
        vol = vol.reshape((vol.size, 1))

    nobs, nvars = X.shape
    nobs_v, nvars_v = vol.shape
    if nobs != nobs_v:
        raise ValueError("The volatility series does not match dimensions of the time series.")
    if (nvars != 1) | (nvars_v != 1):
        raise ValueError("Time series must be univariate.")

    try:
        # Heuristic for max lag term in the search
        max_lags = int(12*(nobs/100.0)**.25)

        # Standardize series with instantaneous vol time series if sdflag==True
        # num_cross counts the number of threshold crossings.
        # indicator of profitable trading opportunities
        if sdflag == True:
            X = X/vol
            num_cross = np.sum(X >= 1.0*thresh) + np.sum(X <= -1.0*thresh)
        else:
            num_cross = np.sum(X >= vol*thresh) + np.sum(X <= -vol*thresh)

        if rreg == "adf":
            if autolag != None:
                model_2 = ADF(y=X, lags=lags, trend="c", max_lags=max_lags, method=autolag)
            else:
                model_2 = ADF(y=X, lags=None, trend="c", max_lags=max_lags, method=autolag)
        elif rreg == "dfgls":
            if autolag != None:
                model_2 = DFGLS(y=X, lags=lags, trend="c", max_lags=max_lags, method=autolag)
            else:
                model_2 = DFGLS(y=X, lags=None, trend="c", max_lags=max_lags, method=autolag)
        elif rreg == "pp":
            model_2 = PhillipsPerron(y=X, lags=lags, trend="c", test_type="tau")
        else:
            model_2 = KPSS(y=X, lags=lags, trend="c")

        alpha_str = str(int(alpha*100)) + "%"

        pvalue = model_2.pvalue
        stat = model_2.stat
        cvalue = model_2.critical_values[alpha_str]

        # Note that the KPSS test null hypothesis is the opposite of the others. Need to flip for uniformity
        if rreg == "kpss":
            h = (pvalue > alpha)
        else:
            h = (stat < cvalue)

        r_1 = X[1:]
        r_2 = X[:-1]
        if creg != "nc":
            r_2 = smtsa.add_trend(r_2, trend=creg, prepend=False)
        model_3 = OLS(r_1, r_2).fit()
        betas = model_3.params

        delta_t = 1 / 252.0
        eta = -np.log(betas[0]) / delta_t

        if (eta <= 0) or (betas[0] <= 0):
            half_life = 100000
        else:
            half_life = (1.0 / delta_t) * np.log(2) / np.log(eta)

        return pvalue, stat, cvalue, h, half_life, num_cross

    except (ValueError, ArithmeticError) as err:
        print(err)
        pvalue = 1.0
        stat = 1000.0
        h = False       #pylint: disable=C0103
        half_life = 100000
        num_cross = 0
        return pvalue, stat, cvalue, h, half_life, num_cross


def dynamic_coint(
    X, y, constant_term=True, delta=0.0001, test_fraction=0.0, sdflag=True, creg="c", rreg="dfgls", lags=None,
    autolag="AIC", alpha=0.05, thresh=1.0, burnin=10
):
    """
    This wrapper function generates the residuals and variance vector first
    then proceeds to perform the goodness tests

    For function signature see generate_residuals and stationarity_test
    """
    beta, bcov, var_vect, spread_vect = generate_residuals(
        X=X, y=y, constant_term=constant_term, delta=delta, test_fraction=test_fraction
    )
    pvalue, stat, cvalue, h, half_life, num_cross = stationarity_test(
        X=spread_vect[burnin:],
        vol=np.sqrt(var_vect[burnin:]),
        sdflag=sdflag,
        creg=creg,
        rreg=rreg,
        lags=lags,
        autolag=autolag,
        alpha=alpha,
        thresh=thresh
    )

    return delta, beta[-1][0], beta[-1][1], float(np.sqrt(var_vect[-1])), pvalue, stat, cvalue, h, half_life, num_cross


def dynamic_coint_wrapper(
    stock_df, names, constant_term=True, delta=0.0001, test_fraction=0.0, sdflag=True, creg="c", rreg="dfgls",
    lags=None, autolag="AIC", alpha=0.05, thresh=1.0, burnin=10
):
    """
    Wrapper function takes a dataframe and calls dynamic_coint

    Args:
        stock_df: stock price dataframe
        names: tickers
    """
    Y = stock_df[names].values
    y = Y[:, 0]
    X = Y[:, 1]

    return dynamic_coint(
        X=X, y=y, constant_term=constant_term, delta=delta, test_fraction=test_fraction, sdflag=sdflag, creg=creg,
        rreg=rreg, lags=lags, autolag=autolag, alpha=alpha, thresh=thresh, burnin=burnin
    )


def stattest_wrapper_parallel(
    stocks_df, pair_delta_df, stdate, endate, constant_term=True, test_fraction=0.0, spark_context=None,
    partitions_count=24
):
    """
    Parallelized residual stationarity test

    Args:
        stock_df: stock price dataframe

    Second input should be a dataframe that has the Name_1, Name_2, delta
    """
    trunc_df = stocks_df.ix[stdate:endate]
    result_list = []

    pairs_rdd = spark_context.parallelize(pair_delta_df.values, partitions_count)
    df_broadcast_stationary = spark_context.broadcast(trunc_df)

    comb_delta = pairs_rdd.map(lambda x: (x[0], x[1], x[2]))
    stat_results = comb_delta.map(
        lambda x: (
            [x[0], x[1]],
            dynamic_coint_wrapper(
                stock_df=df_broadcast_stationary.value, names=[x[0], x[1]], constant_term=True, delta=x[2],
                test_fraction=test_fraction, sdflag=True, creg="c", rreg="dfgls", lags=None, autolag="AIC",
                alpha=0.1, thresh=1.0, burnin=10
            )
        )
    )

    temp_list = stat_results.collect()
    for i in xrange(len(temp_list)):
        elem_1, elem_2 = temp_list[i]
        name_1 = elem_1[0]
        name_2 = elem_1[1]
        delta_opt = elem_2[0]
        beta_0 = elem_2[1]
        beta_1 = elem_2[2]
        vol = elem_2[3]
        pvalue = elem_2[4]
        stat = elem_2[5]
        cvalue = elem_2[6]
        h = elem_2[7]
        half_life = elem_2[8]
        num_cross = elem_2[9]
        res = [name_1, name_2]
        res.extend([delta_opt, beta_0, beta_1, vol, pvalue, stat, cvalue, h, half_life, num_cross])
        result_list.append(res)

    return pd.DataFrame(
        data=result_list,
        columns=[
            "Name_1", "Name_2", "delta_opt", "beta_0", "beta_1", "V", "pvalue", "stat", "cvalue", "h", "halfLife",
            "num_cross"
        ]
    )


def stat_filter_wrapper(
    stocks_df, pair_delta_df, stdate, endate, constant_term=True, test_fraction=0.0, spark_context=None,
    partitions_count=24
):
    """
    Filters names that have passed the dynamic stationarity test by (hardcoded halflife cutoff)
    Sorts (ranks) by number of crossings, optimal delta, pvalue, and halflife

    Args:
        See stattest_wrapper_parallel signature

    Returns:
        filtered and ranked dataframe with relationship variables
    """
    stat_pairs = stattest_wrapper_parallel(
        stocks_df=stocks_df, pair_delta_df=pair_delta_df, stdate=stdate, endate=endate, constant_term=constant_term,
        test_fraction=test_fraction, spark_context=spark_context, partitions_count=partitions_count)
    filter_pairs = stat_pairs[stat_pairs["h"] == True]
    filter_pairs = filter_pairs[filter_pairs["halfLife"] < 104.0]
    filter_pairs.sort(columns=["num_cross", "delta_opt", "pvalue", "halfLife"], inplace=True,
                      ascending=[False, True, True, True])
    filter_pairs["rank"] = xrange(len(filter_pairs))

    return filter_pairs
