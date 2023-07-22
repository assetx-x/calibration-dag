import pandas as pd
from luigi import Task

from base import DCMTaskParams


DIMENSIONS_LABEL = "Dimension"
FACT_LABEL = "Fact"
RULE_LABEL = "Rule"
SAMPLE_LABEL = "Sample"


class CalibrateEarningsTask(DCMTaskParams, Task):
    def run(self):
        with self.input()["rules"].open() as rules_fd:
            rules_df = pd.read_csv(rules_fd, header=[0,1])

        with self.input()["enriched_tickers"].engine() as engine:
            enriched_tickers_df = pd.read_sql(self.input()["enriched_tickers"].table, engine)

        best_rules, rules_matched = find_best_possible_rule_per_sample(rules_df, enriched_tickers_df)
        best_rules.columns = ["rule"]

        result_df = enriched_tickers_df.join(best_rules)
        result_df = result_df[result_df["Rule"].notnull()][["ticker", "timestamp", "Rule"]]
        result_df["count"] = result_df.apply(lambda row: rules_df[FACT_LABEL].loc[int(row["Rule"])]["count"], axis=1)
        result_df["mean"] = result_df.apply(lambda row: rules_df[FACT_LABEL].loc[int(row["Rule"])]["mean"], axis=1)
        result_df["matched_rules"] = result_df.apply(lambda row: repr(rules_matched[row.name]), axis=1)

        with self.output()["calibration"].open("w") as result_fd:
            result_df.to_csv(result_fd)


def generate_function_from_dimension(filt, dimension_label):
    print(filt, dimension_label)
    #import wingdbstub
    if isinstance(filt, list):
        return lambda x:x[dimension_label].isin(filt)
    elif isinstance(filt, str) and filt.startswith("@"):
        return lambda x:x.eval(filt[1:].format(dimension_label))
    elif filt is pd.np.NaN:
        return lambda x:pd.Series(None, index=x.index)
    elif pd.np.isnan(filt):
        return lambda x:pd.Series(None, index=x.index)
    else:
        raise RuntimeError("Unknown rule format: {0} for dimension {1}".format(filt, dimension_label))

def get_best_rule_using_statistics(stat_df):
    return stat_df.sort_values(["count","mean"], ascending=False).index[0]


def find_best_possible_rule_per_sample(rules_and_stats, samples):
    #import wingdbstub
    rules = rules_and_stats[DIMENSIONS_LABEL]
    rules = rules.applymap(lambda x: eval(x) if isinstance(x, str) and x.startswith("[")
                           and x.endswith("]") else x)
    rules_as_functions = rules.apply(lambda x: x.apply(lambda y: generate_function_from_dimension(y, x.name)))
    rules_applications = rules_as_functions.applymap(lambda x: x(samples))
    del rules_as_functions
    rules_applications = pd.Panel(rules_applications.to_dict()).transpose(0,2,1)
    rules_applications.items.name = DIMENSIONS_LABEL
    rules_applications.major_axis.name = RULE_LABEL
    rules_applications.minor_axis.name = SAMPLE_LABEL
    matched_samples_by_rule = rules_applications.sum(axis=0)[rules_applications.all(axis=0) == True]
    possible_best_rules_for_each_sample = matched_samples_by_rule[matched_samples_by_rule
                                                                  == matched_samples_by_rule.max()].dropna(how="all")

    best_rule_per_sample = []
    for k in possible_best_rules_for_each_sample:
        facts_for_best_rules = rules_and_stats.ix[possible_best_rules_for_each_sample[k].dropna().index,FACT_LABEL]
        best_rule = get_best_rule_using_statistics(facts_for_best_rules) if len(facts_for_best_rules) else None
        best_rule_per_sample.append((k,best_rule))
    result = pd.Series(dict(best_rule_per_sample), name=RULE_LABEL)
    result.index.name = SAMPLE_LABEL

    Z = possible_best_rules_for_each_sample.dropna(how='all', axis=1)

    final_dict = {k:Z[k].dropna() for k in Z.columns }

    return result, {key: list(final_dict[key].keys()) for key in final_dict}


def find_list_of_sample(rules_and_stats, samples):
    #import wingdbstub
    rules = rules_and_stats[DIMENSIONS_LABEL]
    rules = rules.applymap(lambda x: eval(x) if isinstance(x, str) and x.startswith("[")
                           and x.endswith("]") else x)
    rules_as_functions = rules.apply(lambda x: x.apply(lambda y: generate_function_from_dimension(y, x.name)))

    rules_applications = rules_as_functions.applymap(lambda x: x(samples))
    del rules_as_functions
    rules_applications = pd.Panel(rules_applications.to_dict()).transpose(0,2,1)
    rules_applications.items.name = DIMENSIONS_LABEL
    rules_applications.major_axis.name = RULE_LABEL
    rules_applications.minor_axis.name = SAMPLE_LABEL
    matched_samples_by_rule = rules_applications.sum(axis=0)[rules_applications.all(axis=0) == True]
    possible_best_rules_for_each_sample = matched_samples_by_rule[matched_samples_by_rule
                                                                  == matched_samples_by_rule.max()].dropna(how="all")
    return list(possible_best_rules_for_each_sample.dropna(how='all', axis=1).columns)
