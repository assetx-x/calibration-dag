from operator import iand

import pandas as pd
import numpy as np
from io import BytesIO
from luigi import Task

from base import DCMTaskParams


class CalculateRulesStatsTask(DCMTaskParams, Task):
    DIMENSIONS_LABEL = "Dimension"
    FACT_LABEL = "Fact"
    RULE_LABEL = "Rule"
    SAMPLE_LABEL = "Sample"

    @staticmethod
    def generate_function_from_dimension(filt, dimension_label):
        #import wingdbstub
        if isinstance(filt, list):
            return lambda x:x[dimension_label].isin(filt)
        elif isinstance(filt, str) and filt.startswith("@"):
            return lambda x:x.eval(filt[1:].format(dimension_label))
        if filt is pd.np.NaN:
            return lambda x:pd.Series(None, index=x.index)
        elif pd.np.isnan(filt):
            return lambda x:pd.Series(None, index=x.index)
        else:
            raise RuntimeError("Unknown rule format: {0} for dimension {1}".format(filt, dimension_label))

    @staticmethod
    def _filter_func(filter, dimension_label):
        print(dimension_label)
        if isinstance(filter, list):
            return lambda samples: samples[dimension_label].isin(filter)
        #elif isinstance(filter, str) and filter.startswith("@"):
        #    return lambda samples: samples.eval(filter[1:].format(dimension_label))
        else:
            return lambda samples: samples.apply(lambda row: True, axis=1)

    @staticmethod
    def _get_samples_stat(rules_row, samples, func):
        for rules_column in rules_row.keys():
            rule = rules_row[rules_column]
            if isinstance(rule, list):
                samples = samples[samples[rules_column].isin(rule)]
            elif isinstance(rule, str) and rule.startswith("@"):
                samples = samples[samples.eval(rule[1:].format(rules_column))]
                print("triangular filtered")

    #         if isinstance(filt, list):
    #     return lambda x:x[dimension_label].isin(filt)
    # elif isinstance(filt, str) and filt.startswith("@"):
    #     return lambda x:x.eval(filt[1:].format(dimension_label))
        return func(samples)

    def run(self):
        with self.input()["rules"].open() as rules_fd:
            rules_df = pd.read_csv(rules_fd)
        with self.input()["digitized_deep_impact_data"].engine() as engine:
            samples = pd.read_sql_table(self.input()["digitized_deep_impact_data"].table, engine)
        #samples = pd.read_hdf("/home/fluder/digitized_results.h5", "df")

        rules_df = rules_df.applymap(lambda x: eval(x) if isinstance(x, str) and x.startswith("[") and x.endswith("]") else x)
        facts_df = pd.DataFrame()
        facts_df["count"] = rules_df.apply(
            lambda row: self._get_samples_stat(row, samples, lambda samples: samples.shape[0]),
            axis=1
        )
        facts_df["mean"] = rules_df.apply(
            lambda row: self._get_samples_stat(row, samples, lambda samples: samples["result"].mean()),
            axis=1
        )

        rules_df = rules_df.drop(["udf_name", "offset"], axis=1)

        rules_buf = BytesIO()
        facts_buf = BytesIO()
        rules_df.to_csv(rules_buf, index=False)
        facts_df.to_csv(facts_buf, index=False)

        result = "\n".join(
            [
                ",".join((["Dimension"] * len(rules_df.keys()))) + "," +
                ",".join((["Fact"] * len(facts_df.keys())))
            ] +
            list(map(lambda row: row[0] + "," + row[1], zip(rules_buf.getvalue().split("\n"), facts_buf.getvalue().split("\n"))))
        )

        with self.output().open("w") as output_fd:
            output_fd.write(result)