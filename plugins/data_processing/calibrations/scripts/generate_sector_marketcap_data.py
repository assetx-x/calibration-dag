import os, sys
import pandas as pd
from sqlalchemy import create_engine
from commonlib.config import get_config


def get_bq_engine():
    gcp_project_id = os.environ["GOOGLE_PROJECT_ID"]
    sa_engine = create_engine(
        'bigquery://{}/marketdata'.format(gcp_project_id))
    return sa_engine

def find_marketcap(row, old_table):
    mc = row["marketcap"]
    if pd.notnull(mc):
        return mc

    old_ids = list(old_table["dcm_security_id"].unique())
    sec_id = row["dcm_security_id"]
    if sec_id in old_ids:
        mc = old_table.loc[old_table["dcm_security_id"]==sec_id, "marketcap"].values[-1]
        return mc
    else:
        return pd.np.nan

def find_marketcapdate(row, old_table):
    dt = row["marketcap_date"]
    if pd.notnull(dt):
        return dt

    old_ids = list(old_table["dcm_security_id"].unique())
    sec_id = row["dcm_security_id"]
    if sec_id in old_ids:
        dt = old_table.loc[old_table["dcm_security_id"]==sec_id, "marketcap_date"].values[-1]
        return dt
    else:
        return pd.np.nan

def marketcap_group(x, thresh):
    if pd.isnull(x):
        return pd.np.nan
    else:
        g = 1*(x<=thresh[0]) + 2*((x>thresh[0]) & (x<=thresh[1])) + 3*((x>thresh[1]) & (x<=thresh[2])) + 4*((x>thresh[2]) \
            & (x<=thresh[3])) + 5*((x>thresh[3]) & (x<=thresh[4])) + 6*((x>thresh[4]) & (x<=thresh[5])) \
            + 7*((x>thresh[5]) & (x<=thresh[6])) + 8*(x>thresh[6])
        g = int(g)
        return g


def main():
    data_dir = "/home/naveen/Documents/dcm/notebooks/russell_update"
    new_sec_file = "security_master_20211120.csv"

    new_sec_master = pd.read_csv(os.path.join(data_dir, new_sec_file), index_col=0)
    new_sec_master_noindex_file = os.path.join(data_dir, new_sec_file.split('.')[0]+'_noindex.csv')
    new_sec_master.to_csv(new_sec_master_noindex_file, index=False)
    print("\nGenerated updated security_master noindex file at ", new_sec_master_noindex_file)

    industry_map = pd.read_csv(os.path.join(data_dir, "industry_map.csv"))
    industry_map.columns = industry_map.columns.str.lower()
    sector_table = industry_map[["dcm_security_id", "ticker", "sector", "industry", "industrygroup"]]
    print("\nsector table data - ", sector_table.shape)
    print(sector_table.tail())

    date_str = str(pd.Timestamp.now().date()).replace("-", "")
    sector_file = os.path.join(data_dir, "sector_table_{0}.csv".format(date_str))
    sector_table.to_csv(sector_file, index=False)
    print("\nGenerated sector table file at ", sector_file)

    print("\nReading FundamentalCleanup.h5 file...")
    store = pd.HDFStore(os.path.join(data_dir, "FundamentalCleanup.h5"))
    print("FundamentalCleanup keys:- ", store.keys())

    daily = store['/pandas/quandl_daily']
    store.close()
    print("Last date in quandl_daily: ", daily["date"].max())

    print("\nGenerating the marketcap table data...")
    marketcap = daily[["date", "dcm_security_id", "marketcap"]]
    marketcap = marketcap.sort_values(["dcm_security_id", "date"])
    marketcap["marketcap"] = marketcap.groupby(["dcm_security_id"])["marketcap"].fillna(method="ffill")

    filter_dt = input("\nEnter the date to filter the marketcap data on (YYYY-MM-DD): ")
    marketcap = marketcap[marketcap["date"] <= pd.Timestamp(filter_dt)].reset_index(drop=True)

    sm = new_sec_master[["dcm_security_id", "ticker", "GICS_Sector"]]
    marketcap_table = pd.merge(sm, marketcap, how="left", on=["dcm_security_id"])
    marketcap_table = marketcap_table.sort_values(["dcm_security_id", "date"])
    marketcap_table = marketcap_table.drop_duplicates(["dcm_security_id", "ticker"], keep="last")

    print("\nNo. of null marketcap entries in the new table: ", len(marketcap_table[pd.isnull(marketcap_table["marketcap"])]))

    sa_engine = get_bq_engine()
    old_marketcap = pd.read_sql("SELECT * FROM marketcap_table", sa_engine)
    print("\nNo. of null marketcap entries in the old table: ", len(old_marketcap[pd.isnull(old_marketcap["marketcap"])]))

    print("\nFinding marketcap and marketcap_date entries for the null values in new table from old table data...")
    marketcap_table = marketcap_table.rename(columns={"date": "marketcap_date"})
    marketcap_table["marketcap_date"] = marketcap_table.apply(lambda x:find_marketcapdate(x, old_marketcap), axis=1)
    marketcap_table["marketcap"] = marketcap_table.apply(lambda x:find_marketcap(x, old_marketcap), axis=1)

    print("\nNo. of null marketcap entries in the new table after processing: ", len(marketcap_table[pd.isnull(marketcap_table["marketcap"])]))

    missing = list(set(marketcap_table["dcm_security_id"]) - set(old_marketcap["dcm_security_id"]))
    print(f"\n{len(missing)} dcm_security_id are new in the new marketcap table: ")
    print(missing)
    print("\n", marketcap_table[marketcap_table["dcm_security_id"].isin(missing)])

    mf = input("\nPress any key to move forward else x to exit: ")
    if mf=='x' or mf=='X':
        sys.exit()

    print("\nFilling null marketcap_date values in new marketcap table with ", filter_dt)
    marketcap_table.loc[pd.isnull(marketcap_table["marketcap_date"]), "marketcap_date"] = pd.Timestamp(filter_dt)

    thresh = [50, 300, 2000, 5000, 10000, 100000, 200000]
    thresh = list(map(lambda x:x*1000000., thresh))
    print("\nFinding marketcap_groups using the thresholds ", thresh)
    marketcap_table["marketcap_group"] = marketcap_table["marketcap"].apply(lambda x:marketcap_group(x, thresh))

    marketcap_table = marketcap_table.drop("GICS_Sector", axis=1)
    marketcap_table["marketcap_group"] = marketcap_table["marketcap_group"].fillna(0.0)
    marketcap_table["marketcap_group"] = marketcap_table["marketcap_group"].astype(int)

    # Commenting out the marketcap_date conversion below to be compatible with bigquery date format
    #marketcap_table["marketcap_date"] = marketcap_table["marketcap_date"].apply(lambda x:pd.Timestamp(x).date().strftime('%m/%d/%Y'))

    marketcap_file = os.path.join(data_dir, "marketcap_table.csv")
    marketcap_table.to_csv(marketcap_file, index=False)
    print("\nGenerated marketcap table file at ", marketcap_file)


if __name__=="__main__":
    main()