import json
from pprint import pprint

from luigi import Task
from luigi.contrib.s3 import S3Client
# from prometheus_client import Gauge, Counter

from base import DCMTaskParams


class CollectManifestsTask(DCMTaskParams, Task):
    def run(self):
        # counter = Counter(
        #     MetricPrefix + "AdjustmentManifest_tickers_total",
        #     "Total tickers adjusted or updated",
        #     ["process_type", "status", "ticker"],
        #     registry=Registry
        # )
        # records_updated = Counter(
        #     MetricPrefix + "AdjustmentManifest_records_total",
        #     "Total records adjusted or updated",
        #     ["process_type", "ticker"],
        #     registry=Registry
        # )
        s3_files = []

        for ticker, target in self.input().iteritems():
            status = target.get_statuses()[-1]
            # try:
            #     counter.labels({"process_type": status["process_type"],
            #                     "status": status["status"],
            #                     "ticker": status["ticker"]}).inc(1)
            #     if "total_records_adjusted" in status and "process_type" in status:
            #         records_updated.labels({"process_type": status["process_type"],
            #                                 "ticker": status["ticker"]}).inc(status["total_records_adjusted"])
            # except Exception:
            #     self.log("Failed to update counters")
            if status["status_type"] == "Success":
                s3_files.append(status["S3_Location"])

        if not any(s3_files):
            raise ValueError("No updates or adjustments could be found to generate a manifest from")

        with self.output().open("w") as output_fd:
            json.dump(
                {
                    "entries": [
                        {"url": s3_file, "mandatory": True}
                        for s3_file in s3_files
                    ],
                },
                output_fd
            )
        #self.push_metrics()