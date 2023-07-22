import pandas as pd
from datetime import datetime, timedelta

import pytz

from base import TaskWithStatus


class FormatCalibrationTask(TaskWithStatus):
    def collect_keys(self):
        with self.input()["pairs"]["data"].open() as fd:
            stat_pairs_df = pd.read_csv(fd)

        temp = stat_pairs_df[["Name_1", "Name_2", "beta_0", "beta_1", "V", "halfLife", "rank"]]
        param_df = pd.DataFrame(data=temp.values, index=xrange(len(temp)),
                                columns=["Name_1", "Name_2", "beta_0", "beta_1", "V", "halfLife", "rank"])

        calibration_df = print_out(param_df=param_df, glflag=True, timescale=1.3, shortlimit=(1.0, -1.25),
                                   longlimit=(-1.0, 1.25), stoplimit=(-0.20, 4.0), total_funding=1500000.0,
                                   fundingRequestsAmount=30000.0, funding_buffer=30000.0,
                                   asof=(self.date + timedelta(days=1)).strftime("%Y%m%d"))

        with self.output()["data"].open("w") as fd:
            calibration_df.to_csv(fd, index=False)


def print_out(param_df, glflag=True, timescale=1.3, shortlimit=(2.5, -2.5), longlimit=(-2.5, 2.5),
              stoplimit=(-0.14, 1.5), total_funding=2500000.0, fundingRequestsAmount=50000.0,
              funding_buffer=50000.0, asof="19000101"):
    """
    Prints configuration file in ingestible format

    Args:
        param_df: raw dataframe of parameters of filtered and ranked pairs
        glflag: indicates whether limits should be globally assigned or individually assigned
            in which case they are part of the param_df
        timescale: limit multiple of the mean-reversion halflife
        shortlimit: short spread limits, enter and exit barriers in terms of volatility
        longlimit: long spread limits, enter and exit barriers in terms of volatility
        stoplimit: %-return stop limits
        asof: enddate of time-series to be included in output file name
    """
    pre_index = list(param_df.index)
    pre_index.append(len(param_df))
    temp_df = pd.DataFrame(index=pre_index, columns=["date", "ranking", "relation",
                                                     "short_spread_limits", "long_spread_limits",
                                                     "stop_limits_pct", "timeLimit", "valueNorm",
                                                     "total_funding", "fundingRequestsAmount",
                                                     "funding_buffer", "actorID", "role"])
    temp_df["date"] = datetime.strptime(asof, "%Y%m%d").date()

    eastern = pytz.timezone("US/Eastern")
    fmt = "%Y%m%d_%H%M%S"
    local_dt = eastern.localize(datetime.now())

    for i in xrange(len(param_df)):
        name_1 = param_df["Name_1"].ix[i]
        name_2 = param_df["Name_2"].ix[i]
        rank = param_df["rank"].ix[i] + 1

        # Quantopian and Yahoo have different conventions
        name_1 = name_1.replace("-", "_")
        name_2 = name_2.replace("-", "_")

        actors = name_1 + "_" + name_2
        actor_type = 2

        beta_0 = param_df["beta_0"].ix[i]
        beta_1 = param_df["beta_1"].ix[i]
        vol = param_df["V"].ix[i]
        half_life = param_df["halfLife"].ix[i]
        delta = half_life*timescale


        # Internal rank started from 0, add 1
        temp_df["ranking"].ix[i] = rank

        relation = "{" + name_1 + ": " + str(1.0) + ", " + name_2 + ": " + \
            str(-beta_1) + ", " + "constant: " + str(-beta_0) + "}"

        if glflag == True:
            temp_df["short_spread_limits"].ix[i] = str(shortlimit)
            temp_df["long_spread_limits"].ix[i] = str(longlimit)
            temp_df["stop_limits_pct"].ix[i] = str(stoplimit)
        else:
            temp_df["short_spread_limits"].ix[i] = param_df["shortlimit"].ix[i]
            temp_df["long_spread_limits"].ix[i] = param_df["longlimit"].ix[i]
            temp_df["stop_limits_pct"].ix[i] = param_df["stoplimit"].ix[i]

        temp_df["relation"].ix[i] = relation
        temp_df["timeLimit"].ix[i] = timedelta(days=delta)
        temp_df["valueNorm"].ix[i] = vol

        temp_df["actorID"].ix[i] = actors
        temp_df["role"].ix[i] = actor_type

    # create supervisor row
    sup = len(param_df)
    temp_df["total_funding"].ix[sup] = total_funding
    temp_df["fundingRequestsAmount"].ix[sup] = fundingRequestsAmount
    temp_df["funding_buffer"].ix[sup] = funding_buffer
    temp_df["actorID"].ix[sup] = "SUP"
    temp_df["role"].ix[sup] = 1

    return temp_df
