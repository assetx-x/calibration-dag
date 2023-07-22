from base import PySparkTask, TaskWithStatus
from statsmodels.regression.linear_model import OLS
from arch.unitroot import ADF, PhillipsPerron, DFGLS
import statsmodels.tsa.tsatools as smtsa
import pandas as pd
import numpy as np


class CointFilterTask(TaskWithStatus, PySparkTask):
    def run_spark(self):
        with self.input()["close_prices"]["data"].open() as fd:
            close_prices_df = pd.read_csv(fd, parse_dates=["date"], index_col=["date"])
        assert len(close_prices_df) > 0

        terminal = close_prices_df.index[-1]
        self.log("Terminal date of timeseries: " + str(terminal))

        ci_pairs = egtest_wrapper(close_prices_df, start_date=self.start_date, end_date=pd.Timestamp.now(),
                                  rreg="dfgls", alpha=0.1, parallel_context=self.spark_context, partitions_count=24)
        filter_pairs = ci_pairs[ci_pairs["halfLife"] <= 84.0]

        with self.output()["data"].open("w") as fd:
            filter_pairs.to_csv(fd)

    def collect_keys(self):
        PySparkTask.run(self)
        return {}


def egcitest(Y, creg="c", rreg="dfgls", lags=None, autolag="AIC", alpha=0.05):
    """
    The Engle-Granger bivariate cointegration test

    Args:
        Y: nobs*2 observation matrix. Y must be bivariate, for nvars>2 use Johansen"s test
        creg: the intercept type where
        c = constant / nc = no constant / ct = time drift / ctt = quadratic time drift
        rreg: the unit-root test type for the regression residuals
            adf / pp / dfgls
            dfgls is the preferred method as it takes into account possible heteroskedasticity
        lags: the number of lags for the unit root test
        autolag: specifieds the lag selection methodology in case of the adf or dfgls test
            AIC, BIC or t-stat
        alpha: the significance level
            since it is a table based lookup, only permissible values are 0.01, 0.05, 0.1

    Returns:
        pvalue: p-value of statistic
        stat: statistic
        cvalue: critical value at the alpha-significance level
        h: accept/reject null hypothesis
        half_life: mean-reversion half-life

    Raises:
        ValueError: for invalid input parameter values
    """

    creg = creg.lower()
    rreg = rreg.lower()
    if creg not in ["c", "nc", "ct", "ctt"]:
        raise ValueError("regression option is invalid")
    if rreg not in ["adf", "dfgls", "pp"]:
        raise ValueError("unitroot test option is invalid")
    if autolag not in ["None", "AIC", "BIC", "t-stat"]:
        raise ValueError("autolag option is invalid")
    if alpha not in [0.01, 0.05, 0.10]:
        raise ValueError("Significance level must be 1%, 5% or 10%")

    nobs, nvars = Y.shape
    if nvars != 2:
        raise ValueError("must have 2 variables")

    try:
        # Heuristic for max lag term in the search
        max_lags = int(12 * (nobs / 100.0) ** .25)

        y_1 = np.asarray(Y[:, 0])
        y_2 = np.asarray(Y[:, 1])
        if creg != "nc":
            y_2 = smtsa.add_trend(y_2, trend=creg, prepend=False)
        model = OLS(y_1, y_2).fit()
        resid_1 = model.resid

        if rreg == "adf":
            if autolag is not None:
                model_2 = ADF(y=resid_1, lags=lags, trend="c", max_lags=max_lags, method=autolag)
            else:
                model_2 = ADF(y=resid_1, lags=None, trend="c", max_lags=max_lags, method=autolag)
        elif rreg == "dfgls":
            if autolag is not None:
                model_2 = DFGLS(y=resid_1, lags=lags, trend="c", max_lags=max_lags, method=autolag)
            else:
                model_2 = DFGLS(y=resid_1, lags=None, trend="c", max_lags=max_lags, method=autolag)
        else:
            model_2 = PhillipsPerron(y=resid_1, lags=lags, trend="c", test_type="tau")

        alpha_str = str(int(alpha * 100)) + "%"

        pvalue = model_2.pvalue
        stat = model_2.stat
        cvalue = model_2.critical_values[alpha_str]
        h = (stat < cvalue)

        r_1 = resid_1[1:]
        r_2 = resid_1[:-1]
        if creg != "nc":
            r_2 = smtsa.add_trend(r_2, trend=creg, prepend=False)
        model_3 = OLS(r_1, r_2).fit()
        betas = model_3.params

        delta_t = 1 / 252.0
        eta = -np.log(betas[0]) / delta_t

        if eta <= 0 or betas[0] <= 0:
            half_life = 100000
        else:
            half_life = (1 / delta_t) * np.log(2) / np.log(eta)

        return pvalue, stat, cvalue, h, half_life

    except:
        pvalue = 1.0
        stat = 1000.0
        h = False
        half_life = 100000
        cvalue = 100.0
        return pvalue, stat, cvalue, h, half_life


# wrapper function for pairwise output
def egtest_wrapper(stock_df, start_date, end_date, rreg="dfgls", alpha=0.1, parallel_context=None, partitions_count=24):
    """
    Parallelized wrapper function for the Engle-Granger test.
    Receives a pandas DataFrame as input, other arguments are passed on to egcitest

    Args:
        stock_df: stock price DataFrame
        start_date: start date of test period
        end_date: end date of test period
        rreg: the unit-root test type for the regression residuals
            adf / pp / dfgls
            dfgls is the preferred method as it takes into account possible heteroskedasticity
        alpha: the significance level
            since it is a table based lookup, only permissible values are 0.01, 0.05, 0.1
        parallel_context: Spark context
        partitions_count: number of partitions to be used

    Returns:
        coint_pairs_list: DataFrame that consists only of the pairs that pass the test
            name_1, name_2, along with the output of egcitest
    """
    stock_df = stock_df.fillna(method="ffill").fillna(method="bfill")
    trunc_df = stock_df.ix[start_date:end_date]
    names = stock_df.columns
    print(names.size)

    sc = parallel_context
    names_rdd = sc.parallelize(trunc_df.columns, partitions_count).cache()
    combinations = names_rdd.cartesian(names_rdd).filter(lambda x: x[0] < x[1]).cache()
    df_broadcast = sc.broadcast(trunc_df)
    combination_ts = combinations.map(lambda x: (x, df_broadcast.value[list(x)].values)).cache()
    stat_results = combination_ts.map(lambda x: (x[0], egcitest(Y=x[1], creg="c", rreg=rreg, lags=None, autolag="AIC",
                                                                alpha=alpha))).cache()
    valid_pairs = stat_results.filter(lambda x: x[1][3]).map(lambda x: sum(x, ())).cache()
    coint_pairs_list = valid_pairs.collect()

    coint_pairs_list = pd.DataFrame(
        data=coint_pairs_list,
        columns=['Name_1', 'Name_2', 'pvalue', 'stat', 'cvalue', 'h', 'halfLife']
    )

    return coint_pairs_list

