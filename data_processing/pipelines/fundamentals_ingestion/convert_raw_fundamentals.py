import json
import os
from datetime import datetime

import pandas as pd
from luigi import Task, DictParameter
from lxml import etree

from base import DCMTaskParams
from pipelines.fundamentals_ingestion.fundamentals_common import BitemporalFundamentalValue

converted_input_file = 'fundamentals_input.h5'


# noinspection PyTypeChecker
class ConvertRawFundamentalsTask(DCMTaskParams, Task):
    derived_metrics = DictParameter(description="Derived metric mapping to formulas", default={})

    def run(self):
        if os.path.exists(self.output().path):
            self.logger.info(
                "Won't run fundamentals raw json conversion, the converted file already exists: " + self.output().path)
            return
        converted_data_path = os.path.join(self.df_data_dir, converted_input_file)
        if os.path.exists(converted_data_path):
            self.logger.info("Detected converted input data file " + converted_data_path)
            input_data = self._load_converted_data(converted_data_path)
            self._process_converted_data(input_data)
        else:
            raw_data_json = self._load_raw_data()
            self.process_raw_data(raw_data_json)

    def process_raw_data(self, raw_data_json):
        input_data = self._convert_json_to_binary_records(raw_data_json)
        self._store_metrics_as_hfs(input_data, os.path.join(self.df_data_dir, converted_input_file))
        self._process_converted_data(input_data)

    def _process_converted_data(self, input_data):
        derived_data = self._calculate_derived_metrics(input_data)
        if derived_data is not None:
            all_data = pd.concat([input_data, derived_data], ignore_index=True)
        else:
            all_data = input_data
        self._store_metrics_as_hfs(all_data)

    def _load_raw_data(self):
        self.logger.info("Loading raw json file from s3: " + self.input().path)
        with self.input().open('r') as input_fd:
            raw_json = json.load(input_fd)
        self.logger.info("Done loading raw json file from s3")
        return raw_json

    def _convert_json_to_binary_records(self, raw_data_jason):
        self.logger.info("Constructing fundamentals records")
        metrics_records = []
        derived_calc_context = {}
        for chunk in raw_data_jason:
            # noinspection PyArgumentList
            parser = etree.XMLParser(huge_tree=True)
            xml_tree = etree.fromstring(chunk, parser)

            for ciq_element in xml_tree:
                ticker, raw_date, metric_name = ciq_element.get("cellAddress").split(";")
                metric_date = datetime.strptime(raw_date, "%Y-%m-%d").date()

                raw_value = ciq_element[0].text
                # noinspection PyBroadException
                try:
                    value = float(raw_value)
                except Exception:
                    value = None

                bitemporal_value = BitemporalFundamentalValue(metric_name, ticker, metric_date, value, self.date, None,
                                                              raw_value)
                derived_calc_context[metric_name] = value
                metrics_records.append(bitemporal_value)
                if len(metrics_records) % 100000 == 0:
                    self.logger.info(" ... so far added " + str(len(metrics_records)) + " records")

        self.logger.info("Done. Added " + str(len(metrics_records)) + " total records from json")
        input_data = pd.DataFrame(metrics_records, columns=BitemporalFundamentalValue)
        return input_data

    def _calculate_derived_metrics(self, input_data):
        self.logger.info("Calculating derived metrics")
        grouped = input_data.sort_values(["date"]).groupby(['ticker'])
        self.logger.info("done grouping")
        derived_data = grouped.apply(self._calc_derived_metrics_for_group)
        return derived_data

    def _calc_derived_metrics_for_group(self, group_data):
        ticker = group_data['ticker'].iloc[0]
        pivoted_data = pd.pivot_table(group_data, values="value", index="date", columns="metric")
        derived_data = pd.DataFrame({k: self._eval(ticker, pivoted_data, k) for k in self.derived_metrics})
        if len(derived_data.index) <= 0:
            return None
        melted_data = pd.melt(derived_data.reset_index(), id_vars="date", var_name="metric")
        melted_data["raw_value"] = "derived_value"
        for k in group_data.columns.difference(melted_data.columns):
            melted_data[k] = group_data.iloc[0, :][k]
        melted_data = melted_data[group_data.columns]
        return melted_data

    # noinspection PyUnresolvedReferences
    def _eval(self, ticker, pivoted_data, k):
        try:
            return pivoted_data.eval(self.derived_metrics[k])
        except Exception:
            self.logger.info("Can't eval metric '%s' formula '%s' for %s" % (k, self.derived_metrics[k], ticker))
            self.logger.info("    available columns are " + str(pivoted_data.columns))
            return pd.Series([pd.np.NaN] * len(pivoted_data), index=pivoted_data.index)

    def _store_metrics_as_hfs(self, metrics_records, file_path=None):
        file_path = file_path if file_path else self.output().path
        self.logger.info("Saving fundamentals data to hfs: " + file_path)
        if not os.path.exists(self.df_data_dir):
            os.makedirs(self.df_data_dir)
        pd.DataFrame(metrics_records, columns=BitemporalFundamentalValue).to_hdf(file_path,
                                                                                 key="fundamentals_data", complevel=9,
                                                                                 complib="blosc")
        self.logger.info("Done saving fundamentals data to temp location")

    def _load_converted_data(self, converted_data_path):
        self.logger.info("Loading converted input data file")
        return pd.read_hdf(converted_data_path, key="fundamentals_data")
