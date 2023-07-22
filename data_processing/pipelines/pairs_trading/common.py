import numpy as np
from pykalman import KalmanFilter


class FLS(object):
    """
    Flexible Least Squares Class enables user to update regression coefficients
    (betas) dynamically through Kalman filtering

    Attributes:
        kalman: calibrated pykalman KalmanFilter object
        beta: history of calibrated betas. beta is the state (transition) variable
        beta_cov: history of beta (state transition) covariances
        current_beta: the last beta value
        current_bcov: last beta covariance matrix
        constant_term: whether an intercept should be included in the regression
            (Boolean)
        delta: delta used in the Kalman filter estimation
    """

    def __init__(self, constant_term=True):
        """
        Initializes FLS class attributes
        """
        self.kalman = KalmanFilter()

        # beta is the entire history
        # current_beta is the most recent entry
        self.beta = np.zeros(2)
        self.beta_cov = np.eye(2, 2)
        self.current_beta = np.zeros(2)
        self.current_bcov = np.eye(2, 2)

        # constant_term specifies whether or not to include constant_term
        # we do not check to see if an array of ones already exist in
        # observation matrix
        self.constant_term = constant_term
        # initialize with small delta, which approximates Ordinary Least Squares
        # (Minimal learning)
        self.delta = 1e-5

    def fit(self, X, y, method="filter", delta=None, constant_term=None):
        """
        Fit the coefficients for the flexible linear model.

        Args:
            X: covariates, and (nobs, nvars) array.
                if X already has array of 1"s don"t set the constant_term to True
                best practice is to control this by constant_term of the instance, i.e. allow the fit
                to inherit from self
            y: response variable
            method: "filter" or "smoother"
                filter infers state based on current history, smoother infers based on all history
            delta: regularization penalty
            constant_term: Boolean, include constant

        Returns:
           Nothing

        FLS instance is updated with fitted attributes, similar to the OLS class
        """
        method = method.lower()
        if method not in ["filter", "smoother"]:
            raise ValueError("Method must be either smoother or filter")

        # if attributes are not specified, inherit them from self
        if delta is None:
            delta = self.delta
        else:
            self.delta = delta
        if constant_term is None:
            constant_term = self.constant_term
        else:
            self.constant_term = constant_term

        # Add constant if necessary and reorient 1D arrays
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

        nobs, nvars = x_temp.shape
        # Pykalman requires this reshaping
        observation_matrix = x_temp.reshape((nobs, 1, nvars))

        # offsets are not needed in our model
        observation_offset = np.array([0.0])
        transition_offset = np.zeros(nvars)

        # mu as defined in the literature
        mu = (1.0 - delta)/(delta*1.0)      #pylint: disable=C0103
        # for FLS the transition matrix is the identity
        transition_matrix = np.eye(nvars, nvars)

        # trans_cov = identity/mu
        transition_covariance = np.eye(nvars, nvars)/(mu*1.0)

        # Expectation Maximization
        # infers the initial state mean, covariance, and obs_cov (observation noise) for a specified delta
        # initialize pykalman instance
        kalman = KalmanFilter(transition_matrices=transition_matrix,
                              em_vars=["initial_state_mean", "initial_state_covariance", "observation_covariance"],
                              observation_matrices=observation_matrix,
                              observation_offsets=observation_offset, transition_offsets=transition_offset,
                              transition_covariance=transition_covariance)
        # runs EM
        kalman.em(y)
        if method is "smoother":
            beta, beta_covar = kalman.smooth(y)
        else:
            beta, beta_covar = kalman.filter(y)

        # update instance with fitted attributes
        self.beta = beta
        self.beta_cov = beta_covar
        self.current_beta = beta[-1]
        self.current_bcov = beta_covar[-1]
        self.kalman = kalman

    # These are update and predicts using pykalman directly. The inputs are single observations
    def kalman_update(self, y, x):
        """
        One step update of filter
        """
        # Insert constant if necessary
        if self.constant_term:
            observation_matrix = add_constant(x, prepend=False)
        else:
            observation_matrix = x.copy()

        nvars = observation_matrix.size
        # Pykalman requires this reshaping
        observation_matrix = observation_matrix.reshape((1, nvars))

        # Kalman gain and variance propagations
        self.current_beta, self.current_bcov = self.kalman.filter_update(self.current_beta, self.current_bcov,
                                                                         observation=y,
                                                                         observation_matrix=observation_matrix)

        # Append current values to existing beta and beta_cov stacks
        self.beta = np.vstack((self.beta, self.current_beta))
        self.beta_cov = np.dstack((self.beta_cov.T, self.current_bcov)).T

    def kalman_predict(self, x):
        """
        Kalman one-step prediction
        """
        # add constant term if necessary
        if self.constant_term:
            x = add_constant(x, prepend=False)

        # returns yhat
        return np.sum(self.current_beta*x)

    # Note that this output is the variance not the standard deviation
    # TODO(Jack) amend to synch time labels, this is done correctly in the residual generator
    def kalman_band(self, x):
        """
        Generates the prediction variance bands
        """
        k = self.kalman
        trans_cov = k.transition_covariance
        obs_cov = k.observation_covariance
        fwd_trans_cov = self.current_bcov + trans_cov
        pred_cov = x.dot(fwd_trans_cov).dot(x.T) + obs_cov
        # This is the variance, not the standard deviation!
        return pred_cov

    def kalma_spread(self, y, x):
        """
        Outputs the prediction residual
        """
        spread = y - self.kalman_predict(x)
        return spread


def add_constant(X, prepend=False):
    """
    Helper function to add intercept for regression

    Args:
        X: input matrix
        prepend: whether to prepend or append. Default is to append.

    Returns:
        X: matrix with column of ones prepended or appended
    """
    if X.ndim == 1:
        if prepend == False:
            X = np.insert(X[:, np.newaxis], 0, np.ones(len(X)), axis=1)
        else:
            X = np.insert(np.ones(len(X)), 0, X[:, np.newaxis], axis=1)
    else:
        if prepend == False:
            X = np.insert(X, 0, np.ones(X.shape[0]), axis=1)
        else:
            X = np.insert(np.ones(X.shape[0]), 0, X, axis=1)

    return X


def mean_abs_error(y, yhat):
    """
    Loss function for mean absolute error

    Args:
        y: input vector
        yhat: predicted vector

    Returns:
        mean absolute error
    """
    if y.shape != yhat.shape:
        raise ValueError("Dimension mismatch")

    return np.mean(np.abs(y - yhat))


def med_abs_error(y, yhat):
    """
    Loss function for median absolute error

    Args:
        y: input vector
        yhat: predicted vector

    Returns:
        median absolute error
    """
    if y.shape != yhat.shape:
        raise ValueError("Dimension mismatch")

    return np.median(np.abs(y - yhat))


def mean_sq_error(y, yhat):
    """
    Loss function for mean square error

    Args:
        y: input vector
        yhat: predicted vector

    Returns:
        mean square error
    """
    if y.shape != yhat.shape:
        raise ValueError("Dimension mismatch")

    return np.mean((y - yhat) ** 2)


def rmse(y, yhat):
    """
    Loss function for root mean square error

    Args:
        y: input vector
        yhat: predicted vector

    Returns:
        root mean square error
    """
    if y.shape != yhat.shape:
        raise ValueError("Dimension mismatch")

    return np.sqrt(mean_sq_error(y, yhat))