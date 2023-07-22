import os, sys, glob
import pandas as pd


def normalize_weights(weight):
    try:
        val = float(weight)
    except:
        val = pd.np.nan
    return val

def clean_dataframe(raw_fwf):
    raw_fwf.columns = list(map(lambda x:x.lstrip(), raw_fwf.columns))
    raw_fwf = raw_fwf[common_cols]
    first = raw_fwf.loc[0, "Date"]
    if first.startswith("---"):
        raw_fwf = raw_fwf[1:].reset_index(drop=True)
    last_ind = raw_fwf[pd.isnull(raw_fwf["Date"])]
    if len(last_ind)>0:
        raw_fwf = raw_fwf[:last_ind.index[0]]
    for c in weight_cols:
        raw_fwf[c] = raw_fwf[c].apply(normalize_weights)
    return raw_fwf

def process_text_file(filename, common_cols, weight_cols):
    raw_fwf = pd.read_fwf(filename)
    raw_fwf.columns = list(map(lambda x:x.lstrip(), raw_fwf.columns))
    raw_fwf = raw_fwf[common_cols]
    first = raw_fwf.loc[0, "Date"]
    if first.startswith("---"):
        raw_fwf = raw_fwf[1:].reset_index(drop=True)
    last_ind = raw_fwf[pd.isnull(raw_fwf["Date"])]
    if len(last_ind)>0:
        raw_fwf = raw_fwf[:last_ind.index[0]]
    for c in weight_cols:
        raw_fwf[c] = raw_fwf[c].apply(normalize_weights)
    raw_fwf["Date"] = raw_fwf["Date"].apply(pd.Timestamp)
    return raw_fwf

def process_csv_file(filename, common_cols, weight_cols):
    raw_fwf = pd.read_csv(filename, quotechar='"')
    raw_fwf.columns = list(map(lambda x:x.lstrip(), raw_fwf.columns))
    raw_fwf = raw_fwf[common_cols]
    first = raw_fwf.loc[0, "Date"]
    if first.startswith("---"):
        raw_fwf = raw_fwf[1:].reset_index(drop=True)
    last_ind = raw_fwf[raw_fwf["Date"].apply(lambda x:x.startswith(" ") or x.startswith("Total"))]
    if len(last_ind)>0:
        raw_fwf = raw_fwf[:last_ind.index[0]]
    for c in weight_cols:
        raw_fwf[c] = raw_fwf[c].apply(normalize_weights)
    raw_fwf["Date"] = raw_fwf["Date"].apply(pd.Timestamp)
    return raw_fwf

def process_csv_file_new(filename, common_cols, weight_cols):
    raw_fwf = pd.read_csv(filename)
    #raw_fwf.columns = list(map(lambda x:x.lstrip(), raw_fwf.columns))
    raw_fwf = raw_fwf[:-1]

    rename_dict = {"Mkt Cap (USD) after investability weight": "Mkt Value", "Value Shares": "ValShares", "Growth Shares": "GroShares",
                   "Constituent Name": "Name", "R1000": "1", "R2000": "2", "R2500": "5", "RMID": "M", "RTOP200": "T", "RSCC": "S",
                   "Industry Code": "ES", "Supersector Code": "SubS", "Subsector Code": "IND"}
    raw_fwf = raw_fwf.rename(columns=rename_dict)
    raw_fwf = raw_fwf[common_cols]
    for c in weight_cols:
        raw_fwf[c] = raw_fwf[c].apply(normalize_weights)
    raw_fwf["Date"] = raw_fwf["Date"].apply(pd.Timestamp)
    return raw_fwf


def sweep_directory(input_dir, output_dir, log_dir):
    os.chdir(input_dir)
    year = input_dir.split(os.sep)[-1]
    processed_dates = []
    file_failures = []

    common_cols = ["Date", "Cusip", "ISIN", "Ticker", "Exchange", "Return", "MTD Return", "Mkt Value", "Shares", "ValShares", "GroShares", "1", "2", "5", "M", "T", "S", "ShareChg", "Name",
                   "WT-R3", "WT-R3G", "WT-R3V", "WT-R1", "WT-R1G", "WT-R1V", "WT-R2", "WT-R2G", "WT-R2V", "WT-R25", "WT-R25G", "WT-R25V", "WT-RMID", "WT-MIDG", "WT-MIDV", "WT-RT2",
                   "WT-RT2G", "WT-RT2V", "WT-RSSC", "WT-RSSCG", "WT-RSSCV"]
    sector_cols = ["ES", "SubS", "IND"]
    common_cols += sector_cols
    weight_cols = ["WT-R3", "WT-R3G", "WT-R3V", "WT-R1", "WT-R1G", "WT-R1V", "WT-R2", "WT-R2G", "WT-R2V", "WT-R25", "WT-R25G", "WT-R25V", "WT-RMID", "WT-MIDG", "WT-MIDV", "WT-RT2",
                   "WT-RT2G", "WT-RT2V", "WT-RSSC", "WT-RSSCG", "WT-RSSCV"]

    for f in glob.glob("*.csv"):
        print(f)
        try:
            #df = process_csv_file(f, common_cols, weight_cols)
            df = process_csv_file_new(f, common_cols, weight_cols)
            #date_str = f.split("_")[1].split("_")[0]
            date_str = f.split("_")[1].split(".")[0]
            processed_dates.append(date_str)
            df.to_csv(os.path.join(output_dir, "{0}_R3000.csv".format(date_str)), quotechar='"')
        except:
            print("Processing failed for {0}".format(f))
            file_failures.append(f)
    for f in glob.glob("*.TXT"):
        date_str = f.split("_")[1].split("_")[0]
        if date_str in processed_dates:
            continue
        else:
            print(f)
            try:
                df = process_text_file(f, common_cols, weight_cols)
                df.to_csv(os.path.join(output_dir, "{0}_R3000.csv".format(date_str)), quotechar='"')
            except:
                print("Processing failed for {0}".format(f))
                file_failures.append(f)
    pd.Series(file_failures).to_csv(os.path.join(log_dir, "{0}_failure_log.csv".format(year)))


def convert_timestamp(output_dir):
    os.chdir(output_dir)
    for f in glob.glob("*.csv"):
        print(f)
        try:
            df = pd.read_csv(f, quotechar='"')
            df["Date"] =  pd.DatetimeIndex(df["Date"].astype(str)).strftime("%Y-%m-%d")
            df = df.loc[:,~df.columns.str.startswith("Unnamed")]
            df.to_csv(f, quotechar='"')
        except:
            print("failure with file {0}".format(f))


def combine_all_files(input_dir, output_dir):
    all_df = []
    os.chdir(input_dir)
    for f in glob.glob("*_R3000.csv"):
        current = pd.read_csv(f, parse_dates=["Date"])
        all_df.append(current)
    r3k = pd.concat(all_df, ignore_index=True)
    r3k = r3k.sort_values(["Date", "Ticker"])
    print("Last date in r3k files: ", r3k["Date"].max())

    r3k.columns = r3k.columns.str.lower()
    r3k['date'] = pd.to_datetime(r3k['date'])
    r3k[['wt-r1', 'wt-r1g', 'wt-r1v']] = r3k[['wt-r1', 'wt-r1g', 'wt-r1v']].fillna(0)
    r3k["name"] = r3k["name"].apply(lambda x:x.replace(",", ";"))

    r3k.to_hdf(os.path.join(output_dir, "r3k_full.h5"), format="table", key="df")

    sub_cols = ["Date", "Cusip", "Ticker", "Name", "WT-R1", "WT-R1G", "WT-R1V"]
    sub_cols = list(map(lambda x:x.lower(), sub_cols))
    r3k_reduced = r3k[sub_cols]
    r3k_reduced.to_hdf(os.path.join(output_dir, "r3k.h5"), format="table", key="df")
    print("\nGenerated r3k_full.h5 and r3k.h5 successfully at ", output_dir)



if __name__=="__main__":
    input_dir = r"/home/naveen/Documents/dcm/notebooks/russell_us_closing_holdings"
    processed_dir = r"/home/naveen/Documents/dcm/notebooks/processed_files_sector_new"
    log_dir = r"/home/naveen/Documents/dcm/notebooks/russell_logs_sector_new"
    output_dir = r"/home/naveen/Documents/dcm/notebooks/russell_update"

    cleanup = input("Please verify the file paths in the script once and cleanup old files, if any."
                    "\nPress Y/y to move forward else any other key to exit: ")
    if cleanup != 'Y' and cleanup != 'y':
        sys.exit()

    print("\nProcessing the files...")
    for year in ['2022']:
        print("**********   YEAR   {0}   *************".format(year))
        current_in = os.path.join(input_dir, year)
        sweep_directory(current_in, processed_dir, log_dir)

    print("\nHandling timestamps in the processed files...")
    convert_timestamp(processed_dir)

    print("\nCombining all processed files to generate full_r3k.h5 and r3k.h5 files...")
    combine_all_files(processed_dir, output_dir)
