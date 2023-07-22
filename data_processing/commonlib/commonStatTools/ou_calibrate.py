import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_redshift_engine


def ou_calibrate(S, delta_t=1./252., method="mle"):
    if isinstance(S, pd.Series):
        S = S.to_frame()
    S = S.fillna(method="ffill")
    if method=="regression":
        X = S[:-1]
        y = S[1:].set_index(X.index) - X
        model = LinearRegression()
        model.fit(X, y)
        a = model.intercept_[0]
        b = model.coef_[0][0]
        resid = model.predict(X) - y
        kappa = -b/delta_t
        theta = a/kappa/delta_t
        sigma = resid.std()/pd.np.sqrt(delta_t)
        sigma = sigma.values[0]
    elif method=="least_squares":
        X = S[:-1]
        y = S[1:].set_index(X.index)
        model = LinearRegression()
        model.fit(X, y)
        a = model.intercept_[0]
        b = model.coef_[0][0]
        resid = model.predict(X) - y
        kappa = -np.log(b)/delta_t
        theta = a/(1-b)
        sigma = resid.std()*pd.np.sqrt(2*kappa/(1-b**2))
        sigma = sigma.values[0]
    else:
        n = len(S) - 1
        x = S[:-1]
        y = S[1:].set_index(x.index)
        Sx  = np.sum(x).values[0]
        Sy  = np.sum(y).values[0]
        Sxx = x.multiply(x).sum().values[0]
        Sxy = x.multiply(y).sum().values[0]
        Syy = y.multiply(y).sum().values[0]
        theta = (Sy*Sxx - Sx*Sxy) / (n*(Sxx-Sxy) - (Sx**2 - Sx*Sy))
        kappa = -(1./delta_t)*pd.np.log((Sxy - theta*Sx - theta*Sy + n*theta**2)/(Sxx - 2*theta*Sx + n*theta**2))
        alpha = pd.np.exp(-kappa*delta_t)
        alpha_2 = pd.np.exp(-2.*kappa*delta_t)
        sigma_hat_2 = (1./n)*(Syy - 2*alpha*Sxy + alpha_2*Sxx - 2*theta*(1-alpha)*(Sy-alpha*Sx) + n*theta**2*(1-alpha)**2)
        sigma = pd.np.sqrt(sigma_hat_2*2*kappa/(1-alpha_2))
    return kappa, theta, sigma

def ou_calibrate_jackknife(S, delta_t=1./252., num_part=2, include_all=False):
    S = S.fillna(method="ffill")
    m = num_part
    part_length = int(len(S)/m)
    # Here we ignore the original indices (does not matter for the parameter estimation)
    Spart = pd.DataFrame(pd.np.zeros((part_length, m)))
    for i in range(m):
        Spart[i] = S[part_length*i:part_length*(i+1)].values

    kappa_T, theta_T, sigma_T = ou_calibrate(S, delta_t)
    kappa_part = pd.np.zeros(m)
    theta_part = pd.np.zeros(m)
    sigma_part = pd.np.zeros(m)

    for i in range(m):
        kappa_part[i], theta_part[i], sigma_part[i] = ou_calibrate(Spart[i], delta_t)

    kappa = 1.*m/(m-1)*kappa_T - pd.np.sum(kappa_part)/(m**2-m)
    # Others don't need jackknife
    if include_all:
        theta = 1.*m/(m-1)*theta_T - pd.np.sum(theta_part)/(m**2-m)
        sigma = 1.*m/(m-1)*sigma_T - pd.np.sum(sigma_part)/(m**2-m)
    else:
        theta = theta_T
        sigma = sigma_T
    return kappa, theta, sigma

def ou_simulate_one_step_given_resid(kappa, theta, sigma, S_0, resid, delta_t=1./252.):
    exp_minus_kappa_delta_t = pd.np.exp(-kappa*delta_t)
    S_t = S_0*exp_minus_kappa_delta_t + theta*(1-exp_minus_kappa_delta_t) + resid
    return S_t

def test_data_index():
    engine = get_redshift_engine()
    query = "select * from daily_index where symbol in ('VXST') and as_of_end is null order by symbol, date"
    df = pd.read_sql(query, engine)
    df = df[df["symbol"]=="VXST"][["date", "close"]].set_index("date")
    df = df.loc[pd.Timestamp("2011-01-01"):]
    return df

def test_data_futures():
    engine = get_redshift_engine()
    query = "select * from daily_continuous_futures_prices where continous_contract in ('VX2') and as_of_end is null order by continous_contract, date"
    df = pd.read_sql(query, engine)
    df = df.rename(columns={"continous_contract": "symbol"})
    df = df[df["symbol"]=="VX2"][["date", "close"]].set_index("date")
    df = df.loc[pd.Timestamp("2011-01-01"):]
    return df

if __name__=="__main__":
    data = test_data_futures()

    kappa, theta, sigma = ou_calibrate(pd.np.log(data), method="mle")
    kappa, theta, sigma = ou_calibrate(pd.np.log(data), method="regression")
    kappa, theta, sigma = ou_calibrate(pd.np.log(data), method="least_squares")
    kappa, theta, sigma = ou_calibrate_jackknife(pd.np.log(data), num_part=4, include_all=True)

    print("done")