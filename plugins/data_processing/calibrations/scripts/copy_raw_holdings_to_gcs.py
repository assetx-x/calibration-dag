import os
import re
import paramiko
import tempfile
import pandas as pd
from google.cloud import storage


sftp_host = "data.ftse.com"
sftp_user = "DataCapM9582"
sftp_pass = "UTCDQUo5"
holdings_files_path = "/data/Standard/Russell_US_Indexes_Closing_Holdings/history"
gs_bucket = "{}-us-dcm-data-temp".format(os.environ["GOOGLE_PROJECT_ID"])


def copy_sftp_files_to_gcs():
    today = pd.Timestamp.now()
    start_dt = today - pd.tseries.offsets.BDay(3)

    b_dates = pd.bdate_range(start_dt, today).tolist()
    b_dates = [date.strftime("%Y%m%d") for date in b_dates]

    start_epoch = int(start_dt.asm8.astype(pd.np.int64)/1000000000)

    # need to find the different years during the new year turnover
    # since files are organized by year on russell sftp server
    years = [str(start_dt.year)]
    if years[0] != str(today.year):
        years.append(str(today.year))

    temp_dir = tempfile.gettempdir()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gs_bucket)

    print("\nConnecting to sftp server...")
    ssh = paramiko.SSHClient()
    # automatically add keys without requiring human intervention
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(sftp_host, username=sftp_user, password=sftp_pass)
    sftp = ssh.open_sftp()
    print("Connected!")

    for year in years:
        print("\nYEAR - ", year)
        sftp.chdir("{}/{}".format(holdings_files_path, year))
        filelist = sftp.listdir_attr()
        gs_blob_path = "russell_holdings/{}".format(year)

        # find all the files which got added or modified after the starting business day
        a_files = [f for f in filelist if f.st_atime > start_epoch]
        m_files = [f for f in filelist if f.st_mtime > start_epoch]
        req_files = list(set().union(a_files, m_files))

        for date_str in b_dates:
            # find the files with the particular date string from within required files list
            r = re.compile("H_" + date_str + ".*[\.csv|\.TXT]$")
            date_files = list(filter(lambda file:r.match(file.filename), req_files))
            if date_files:
                # sort the particular date files by their added time in desc order
                date_files.sort(key=lambda file:file.st_atime, reverse=True)
                # will pick the latest file from that day
                latest_file = date_files[0].filename

                local_path = os.path.join(temp_dir, latest_file)
                sftp.get(latest_file, local_path)
                blob = bucket.blob("{}/{}".format(gs_blob_path, latest_file))
                blob.upload_from_filename(local_path)
                print("Uploaded {} to {}/{}".format(latest_file, gs_bucket, gs_blob_path))
                if os.path.isfile(local_path):
                    os.remove(local_path)


if __name__=='__main__':
    copy_sftp_files_to_gcs()