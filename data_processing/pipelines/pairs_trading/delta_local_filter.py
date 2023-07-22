import numpy as np
import pandas as pd
#TODO: bring the library back when this is re-enabled
#from pykalman import KalmanFilter

from base import PySparkTask, TaskWithStatus
from pipelines.pairs_trading.common import mean_sq_error, mean_abs_error, med_abs_error, rmse, \
    add_constant, FLS


class DeltaLocalFilterTask(TaskWithStatus, PySparkTask):
    def collect_keys(self):
        PySparkTask.run(self)
        return {}

    def run_spark(self):
        with self.input()["pairs"]["data"].open() as fd:
            pairs_df = pd.read_csv(fd)
        with self.input()["close_prices"]["data"].open() as fd:
            price_df = pd.read_csv(fd, parse_dates=["date"], index_col=["date"])

        temp_df = pairs_df[['Name_1', 'Name_2']]
        pairs_df = pd.DataFrame(data=temp_df.values, index=xrange(len(temp_df)), columns=['Name_1', 'Name_2'])

        result_df = delta_local_filter_wrapper(
            stock_df=price_df, pairs_df=pairs_df, stdate=self.start_date, endate=self.date, method="filter",
            constant_term=True, test_fraction=0.7, ngrid=20, x_0=0.01, delta_min=1e-5, delta_max=0.6,
            loss=mean_sq_error, spark_context=self.spark_context, deltacut=0.6
        )

        with self.output()["data"].open("w") as fd:
            result_df.to_csv(fd)


def delta_loss(
    X, y, method="filter", delta=1e-5, constant_term=True, test_fraction=0.7, loss=mean_sq_error, simple=False
):
    """
    Computes loss function value for a given delta

    Args:
        X: observation matrix
        y: response variable
        method: filter or smoother
        constant_term: to include constant intercept
        test_fraction: faction to be included in training (0.0 - 1.0)
        loss: loss function to assess out of sample fit
        simple: whether output should be verbose or simply the error function value

    Returns:
        error: loss function value
        obs_cov: observation covariance estimated via EM algorithm
        last_beta: terminal beta value
    """
    method = method.lower()
    if method not in ["filter", "smoother"]:
        raise ValueError("Method must be either smoother or filter")

    if loss not in [mean_abs_error, med_abs_error, mean_sq_error, rmse]:
        raise ValueError("Invalid loss function specification")

    try:
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
        fls.fit(x_temp[:ntrain], y[:ntrain], delta=delta, method=method)
        k = fls.kalman
        kalman = KalmanFilter(
            transition_matrices=k.transition_matrices, observation_matrices=observation_matrix,
            observation_offsets=k.observation_offsets, transition_offsets=k.transition_offsets,
            observation_covariance=k.observation_covariance, transition_covariance=k.transition_covariance,
            initial_state_mean=k.initial_state_mean, initial_state_covariance=k.initial_state_covariance
        )

        if method is "smoother":
            beta, bcov = kalman.smooth(y)
        else:
            beta, bcov = kalman.filter(y)

        # Compute predictions over the hold-out set
        yhat = np.sum(beta[ntrain-1:-1] * x_temp[ntrain-1:-1], axis=1)
        # Store the loss function values, the obs_cov (observation noise variance) and betas
        error = loss(y[ntrain:], yhat)
        obs_cov = kalman.observation_covariance
        last_beta = beta[-1]

        if simple:
            return error
        else:
            return error, obs_cov, last_beta

    except ValueError:
        error = 1000000.0
        obs_cov = 1000.0
        last_beta = [0.0, 0.0]

        if simple:
            return error
        else:
            return error, obs_cov, last_beta


def delta_loss_wrapper(
    stock_df, names, delta, method="filter", constant_term=True, test_fraction=0.7, loss=mean_sq_error
):
    """
    Wrapper function that accepts a stock price dataframe

    Args:
        stock_df: stock price dataframe
        other arguments are identical to the signature of the delta_loss function
    """
    Y = stock_df[names].values
    y = Y[:, 0]
    X = Y[:, 1]
    return delta_loss(
        X=X, y=y, method=method, delta=delta, constant_term=constant_term, test_fraction=test_fraction, loss=loss,
        simple=False
    )


def delta_opt_local_wrapper_pair(
    stock_df, pairs_df, stdate, endate, method="filter", constant_term=True, test_fraction=0.7, ngrid=20, x_0=0.01,
    delta_min=1e-5, delta_max=0.999, loss=mean_sq_error, spark_context=None, partitions_count=24
):
    """
    Parallelized optimization wrapper for specific pairs. This is the local version around x_0.

    Arguments:
        stock_df: stock price DataFrame
        pairs_df: dataframe that contains the tickers for specified pairs
        x_0: center of local search
        spark_context: Spark Context
        partitions_count: number of partitions
    Returns:
        dataframe with ["Name_1", "Name_2", "delta_opt", "ve", "beta_0", "beta_1"] for the optimized pairs
    """
    trunc_df = stock_df.ix[stdate:endate]

    d_1 = np.logspace(np.log10(delta_min), np.log10(x_0), int(ngrid/2))
    d_2 = np.logspace(np.log10(x_0), np.log10(delta_max), int(ngrid/2)+1)
    delta_vect = np.hstack((d_1, d_2[1:]))
    #delta_vect = np.logspace(np.log10(delta_min), np.log10(delta_max), ngrid)

    result_list = []

    numpairs = len(pairs_df)

    pairs_rdd = spark_context.parallelize(pairs_df.values, partitions_count)
    delta_rdd = spark_context.parallelize(delta_vect, partitions_count)
    df_broadcast_delta = spark_context.broadcast(trunc_df)

    combinations_basic = pairs_rdd.map(lambda x: (x[0], x[1]))
    # combinations = names_1_RDD.cartesian(names_2_RDD).filter(lambda x: x[0] < x[1])
    comb_delta = combinations_basic.cartesian(delta_rdd)

    delta_results = comb_delta.map(
        lambda x: (
            x[0],
            (x[1], delta_loss_wrapper(
                    stock_df=df_broadcast_delta.value, names=list(x[0]), delta=x[1], method=method,
                    constant_term=constant_term, test_fraction=test_fraction, loss=loss
            ))
        )
    )
    delta_grouped = delta_results.groupByKey()
    minimums_rdd = delta_grouped.map(lambda x: (x[0], reduce(lambda y, z: y if y[1][0] <= z[1][0] else z, x[1])))
    all_min = minimums_rdd.collect()

    temp_list = all_min
    for i in xrange(len(temp_list)):
        elem_1, elem_2 = temp_list[i]
        name_1 = elem_1[0]
        name_2 = elem_1[1]
        delta = elem_2[0]
        error = elem_2[1][0]
        obs_cov = float(elem_2[1][1])
        beta_0 = float(elem_2[1][2][0])
        beta_1 = float(elem_2[1][2][1])
        res = [name_1, name_2]
        res.extend([delta, obs_cov, beta_0, beta_1])
        result_list.append(res)

    return pd.DataFrame(data=result_list, columns=["Name_1", "Name_2", "delta_opt", "ve", "beta_0", "beta_1"])


def delta_local_filter_wrapper(
    stock_df, pairs_df, stdate, endate, method="filter", constant_term=True, test_fraction=0.7, ngrid=20, x_0=0.01,
    delta_min=1e-5, delta_max=0.6, loss=mean_sq_error, spark_context=None, partitions_count=24, deltacut=0.6
):
    """
    Filters by optimal delta. High optimal delta means relationship is highly unstable.
    Hence, should be removed. Local optimization version of delta_filter_wrapper

    Args:
        deltacut: delta cut off for rejection
        for others see function signature of delta_opt_local_wrapper_pairs

    Returns:
        filtered dataframe of optimized deltas
    """
    delta_pairs = delta_opt_local_wrapper_pair(
        stock_df=stock_df, pairs_df=pairs_df, stdate=stdate, endate=endate, method=method, constant_term=constant_term,
        test_fraction=test_fraction, ngrid=ngrid, x_0=x_0, delta_min=delta_min, delta_max=delta_max, loss=loss,
        spark_context=spark_context, partitions_count=partitions_count
    )

    return delta_pairs[delta_pairs["delta_opt"] < deltacut]
