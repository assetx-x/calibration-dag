import boto3
import requests
from luigi import Task

from base import credentials_conf, DCMTaskParams


class RavenpackIngestionTask(DCMTaskParams, Task):
    def run(self):
        s3_bucket = self.output()['s3_file'].path.split('/')[2]
        file_name = self.output()['s3_file'].path.split('/')[3]

        rp_url = 'https://ravenpack.com/rpna/newsanalytics/4.0/daily_eqt/' + file_name
        self.logger.info('Downloading file ' + rp_url)
        response = requests.get(rp_url, auth=(credentials_conf['ravenpack']['username'],
                                              credentials_conf['ravenpack']['password']))

        self.logger.info('Response code: ' + str(response.status_code))

        if response.status_code == 200:
            s3_client = boto3.client('s3', aws_access_key_id=credentials_conf['s3']['access_key'],
                                     aws_secret_access_key=credentials_conf['s3']['secret_key'])
            s3_client.put_object(Body=response.content, Bucket=s3_bucket, Key=file_name)
        else:
            self.logger.info('Response text: ' + response.text)
