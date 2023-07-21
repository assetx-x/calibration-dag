import glob

from google.cloud import bigquery


client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    # schema=[
    #     bigquery.SchemaField("name", "STRING"),
    #     bigquery.SchemaField("post_abbr", "STRING"),
    # ],
    # skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
files_path = "/Users/iklo/gcp_downloads/*.csv"

for f in glob.glob(files_path):
    print(f)
    with open(f, "rb") as source_file:
        job = client.load_table_from_file(
            source_file, 'ax-prod-393101.marketdata.daily_equity_prices',
            job_config=job_config
        )
    job.result()
