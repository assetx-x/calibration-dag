from datetime import timedelta
from uuid import uuid4

import pandas as pd
from luigi import Task

from base import DCMTaskParams, credentials_conf

import to_redshift


class EnrichTickersWithNearEarningsTask(DCMTaskParams, Task):
    def run(self):
        with self.input()["current_quarter_earnings"].open() as current_quarter_earnings_fd:
            current_quarter_earnings_df= pd.read_csv(current_quarter_earnings_fd, parse_dates=["Timestamp", "Date"])

        current_quarter_earnings_df = current_quarter_earnings_df[["Ticker", "Date", "Timestamp", "Quarter"]]
        current_quarter_earnings_df["Id"] = current_quarter_earnings_df.apply(lambda row: str(uuid4()), axis=1)

        with self.input()["sector_mapping"].engine() as engine:
            mapping_df = pd.read_sql_table(self.input()["sector_mapping"].table, engine).set_index("ticker")

            for index, row in current_quarter_earnings_df.iterrows():
                for column in mapping_df.keys():
                    if column not in ["sector", "industry"]:
                        continue
                    try:
                        current_quarter_earnings_df.set_value(index, column, mapping_df[column][row["Ticker"]])
                    except Exception:
                        print("No sector-industry mapping for %s" % row["Ticker"])

        for enrichment_input, date_column in self.input()["enrichment_plan"]:
            with enrichment_input.engine() as engine:
                print("Loading %s" % enrichment_input.table)
                enrichment_df = pd.read_sql_table(enrichment_input.table, engine)

                enrichment_df = enrichment_df[enrichment_df[date_column] <= self.date]
                # enrichment_df["_diff"] = abs(enrichment_df[date_column] - pd.to_datetime(self.date))
                enrichment_df["_diff"] = enrichment_df.apply(lambda row: self.date - row[date_column].to_pydatetime().date(), axis=1)
                enrichment_df = enrichment_df[enrichment_df["_diff"] < timedelta(days=90)] \
                    .sort_values("_diff") \
                    .drop_duplicates(["ticker"], keep="first") \
                    .set_index("ticker")

                for index, row in current_quarter_earnings_df.iterrows():
                    for column in enrichment_df.keys():
                        if column in ["ticker", "_diff", date_column, "as_of_start", "as_of_end"] or column.startswith("Unnamed"):
                            continue
                        try:
                            current_quarter_earnings_df.set_value(index, column, enrichment_df[column][row["Ticker"]])
                        except Exception:
                            print("No %s data for %s" % (enrichment_input.table, row["Ticker"]))

        with self.output().engine() as engine:
            current_quarter_earnings_df.to_redshift(
                self.output().table,
                engine,
                "dcm-data-temp",
                index=False,
                if_exists="replace",
                aws_access_key_id=credentials_conf["s3"]["access_key"],
                aws_secret_access_key=credentials_conf["s3"]["secret_key"]
            )
        self.output().touch()
