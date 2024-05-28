from plugins.DataCalculations.strategies.base_metrics_abc import BaseMetrics


class AccuracyMetrics(BaseMetrics):
    columns = ['future_ret_5B']

    def __init__(self, dataframe):
        self.dataframe = dataframe
        self._validate()

    def _validate(self):
        if len(self.dataframe) == 0:
            raise Exception(
                f'[*] Empty dataframe {self.__class__.__name__} cannot be calculated'
            )
        if not all([col in self.dataframe.columns for col in self.columns]):
            raise Exception(
                f'[*] Columns not found in dataframe {self.__class__.__name__} cannot be calculated'
            )

    def calculate_correct_predictions(self):
        df = self.dataframe.copy()

        for c in self.columns:
            df[c] = df[c].apply(lambda x: 0 if x == 0 else (1 if x > 0 else -1))

        total_count = len(df)
        same_sign_count = 0

        for idx1, col1 in enumerate(self.columns):
            for idx2, col2 in enumerate(self.columns):
                if idx1 < idx2:
                    same_sign_count += (df[col1] == df[col2]).sum()

        return same_sign_count if total_count != 0 else 0

    def calculate_number_of_predictions(self):
        return len(self.dataframe)

    def _calculate_sign_array(self):
        df = self.dataframe.copy()
        for c in self.columns:
            df[c] = df[c].apply(lambda x: 0 if x == 0 else (1 if x > 0 else -1))
        return df

    def calculate_number_of_correct_predictions(self):
        df = self._calculate_sign_array()
        total_count = len(df)
        same_sign_count = sum(
            (df[col1] == df[col2]).sum()
            for idx1, col1 in enumerate(self.columns)
            for idx2, col2 in enumerate(self.columns)
            if idx1 < idx2
        )
        return same_sign_count if total_count != 0 else 0

    def _calculate_consecutive_periods(self, sign):
        df = self._calculate_sign_array()
        consecutive_periods = 0
        max_consecutive_periods = 0

        for _, row in df.iterrows():
            if row[self.columns[0]] == sign:
                consecutive_periods += 1
                max_consecutive_periods = max(
                    max_consecutive_periods, consecutive_periods
                )
            else:
                consecutive_periods = 0

        return max_consecutive_periods

    def calculate_up_periods(self):
        return self._calculate_consecutive_periods(1)

    def calculate_down_periods(self):
        return self._calculate_consecutive_periods(-1)

    def calculate_up_down_ratio(self):
        up_periods = self.calculate_up_periods()
        down_periods = self.calculate_down_periods()
        return (
            up_periods / down_periods
            if down_periods != 0
            else up_periods if up_periods != 0 else 0
        )
