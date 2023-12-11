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
