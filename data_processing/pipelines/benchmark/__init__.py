import csv
import random
from time import time

import boto3
import pandas as pd
from io import BytesIO, BufferedReader

from botocore.response import StreamingBody
from luigi import IntParameter, Parameter, Task

from base import PySparkTask, S3Target

SAMPLE_FRAC = 0.006
CHUNK_ROWS_COUNT = 2 * 1024 * 1024


class BenchmarkS3Task(PySparkTask):
    executor_memory = "42GB"

    name = Parameter()
    num_tasks = IntParameter()
    input_bucket = Parameter()
    input_key = Parameter()
    output_bucket = Parameter()
    output_key = Parameter()

    def run_spark(self):
        input_bucket = self.spark_context.broadcast(str(self.input_bucket))
        input_key = self.spark_context.broadcast(str(self.input_key))
        output_bucket = self.spark_context.broadcast(str(self.output_bucket))
        output_key = self.spark_context.broadcast(str(self.output_key))
        name = self.spark_context.broadcast(str(self.name))
        num_tasks = self.spark_context.broadcast(int(self.num_tasks))

        workers = self.spark_context \
            .parallelize(range(int(self.num_tasks))) \
            .map(lambda x: (x, x)) \
            .partitionBy(int(self.num_tasks))

        def write_chunk(worker_key, worker_num):
            s3_client = boto3.client('s3')
            print("Getting object")
            input_s3_object = s3_client.get_object(Bucket=input_bucket.value, Key=input_key.value)
            #input_fd = input_s3_object["Body"]
            print("Parsing to dataframe")
            input_fd = BytesIO(input_s3_object["Body"].read())
            df = pd.read_csv(input_fd)
            #input_fd.close()
            #del input_fd

            samples = []
            for i in xrange(num_tasks.value):
                print("Sampling chunk [%i/%i]" % (i, num_tasks.value))
                buf = BytesIO()
                df.iloc[CHUNK_ROWS_COUNT * worker_num:CHUNK_ROWS_COUNT * worker_num + CHUNK_ROWS_COUNT].to_csv(buf)
                #df.sample(frac=SAMPLE_FRAC).to_csv(buf)
                samples.append(buf)

            for i, sample_buf in enumerate(samples):
                print("Writing chunk [%i/%i]" % (i, len(samples)))
                s3_client.put_object(Bucket=output_bucket.value, Key="%s/%s-%d-%d.csv" % (output_key.value, name.value, worker_num, i), Body=buf.getvalue())


            return True

        start_time = time()
        workers.map(write_chunk).collect()
        end_time = time()

        print(end_time - start_time)


class BenchamrkEfsTask(PySparkTask):
    name = Parameter()
    num_tasks = IntParameter()
    input_path = Parameter()
    output_dir_path = Parameter()

    def run_spark(self):
        input_path = self.spark_context.broadcast(str(self.input_path))
        output_dir_path = self.spark_context.broadcast(str(self.output_dir_path))
        name = self.spark_context.broadcast(str(self.name))

        workers = self.spark_context.parallelize(range(int(self.num_tasks)))

        def write_chunk(worker_num):
            input_fd = open(input_path.value)
            input_fd.seek(worker_num * CHUNK_SIZE)

            input_data = input_fd.read()
            input_data = input_data[:input_data.rfind("\n")]
            input_data = input_data[input_data.find("\n") + 1:]

            output_fd = open("%s/%s-%d.csv" % (output_dir_path.value, name.value, worker_num), "w")
            output_fd.write(input_data)
            output_fd.flush()
            output_fd.close()

        start_time = time()
        workers.map(write_chunk)
        end_time = time()

        print(end_time - start_time)


class GenerateBenchmarkData(Task):
    rows_count = IntParameter()

    def run(self):
        with self.output().open("w") as output_fd:
            csv_writer = csv.writer(output_fd)
            for i in xrange(int(self.rows_count)):
                if i % 10000 == 0:
                    print("%d/%d" % (i, int(self.rows_count)))
                csv_writer.writerow([random.randint(0, 1000)])

    def output(self):
        return S3Target("s3://dcm-data-test/fluder/benchmark/input_data.csv")
