"""
This script is used to breakdown monthly Ravenpack files into daily ones, that can then be processed by the regular Luigi Flow. 
It works as a standlaone tool.
"""

import glob
import boto3
from datetime import datetime

start_date = '2017-03-07'  # inclusive
end_date = '2017-05-31'  # inclusive

in_file_name_template = r"D:\Downloads\RPNA_2017_4.0-Equities\2017-%02d-equities.csv"
out_file_name_template = r"D:\temp\ravenpack\RPNA_%s_4.0-equities.csv"

s3_bucket = 'dcm-ravenpack-ingest'


class RavenpackMonthlyDataSplitter(object):
    def __init__(self):
        super(RavenpackMonthlyDataSplitter, self).__init__()
        self.header = None
        self.current_date = None
        self.current_file = None

    def split_monthly_files(self, months):
        for month in months:
            in_file_name = in_file_name_template % month
            print("Processing", in_file_name)
            with open(in_file_name, 'r') as monthly_file:
                self.header = monthly_file.readline()
                if self._split_monthly_to_daily(monthly_file):
                    print("Done!")
                    return

    @staticmethod
    def copy_files_to_s3():
        s3_client = boto3.client('s3',
                                 aws_access_key_id="",
                                 aws_secret_access_key="")
        file_names = glob.glob(out_file_name_template % '*')
        for file_name in file_names:
            with open(file_name, 'r') as raven_pack_file:
                print(datetime.now().strftime("%H:%M:%S.%f"), 'Copying', file_name)
                raven_pack_data = ''.join(raven_pack_file.readlines())
                s3_client.put_object(Body=raven_pack_data, Bucket=s3_bucket, Key=file_name.split('\\')[-1])

    def _split_monthly_to_daily(self, monthly_file):
        try:
            for line in monthly_file:
                line_date = line[:10]
                if line_date > end_date:
                    return True  # Done
                if line_date < start_date:
                    continue
                self._try_reset_file(line_date)
                self.current_file.write(line)
        finally:
            self._close_current_file()
        return False  # Not done yet

    def _try_reset_file(self, line_date):
        if line_date != self.current_date:
            self._close_current_file()
            self.current_date = line_date
            out_file_name = out_file_name_template % self.current_date
            print("Creating", out_file_name)
            self.current_file = open(out_file_name, 'w')
            self.current_file.write(self.header)

    def _close_current_file(self):
        if self.current_file:
            self.current_file.close()
            self.current_file = None


if __name__ == '__main__':
    # splitter = RavenpackMonthlyDataSplitter()
    # splitter.split_monthly_files([3, 4, 5])
    RavenpackMonthlyDataSplitter.copy_files_to_s3()
