import os, sys
import quandl
import pandas as pd
from market_timeline import marketTimeline


quandl.ApiConfig.api_key = "tzfgtC1umXNxmDLcUZ-5"

ignore_cusips = ["24703L2X2", "7844XXXX1", "784XXX804", "079823XX0", "00XX6R102"]

orig_missing_tickers = ['HEI.A', 'SERV', 'UTX', 'LEN.B', 'BRK.B', 'MTCH_x', 'LGF.B', 'JW.A', 'APY', 'BPR', 'LGF.A', 'GDI_x', 'BF.A',
                        'AIV_x', 'BF.B', 'WTR', 'MSG', 'MYL_x', 'SLG_x', 'SLG1_x', 'CTL', 'DELL_x', 'CEGVV', 'BRBR_x', 'WM_x']


def carry_forward(old_r1k, old_r3k):
    copy_date = input("\nEnter the date to copy the data from (YYYY-MM-DD):  ")
    from_date = input("Enter the from_date to carry forward the data: ")
    from_date = pd.Timestamp(from_date).tz_localize("UTC")
    to_date = input("Enter the to_date to carry forward the data: ")
    to_date = pd.Timestamp(to_date).tz_localize("UTC")
    update_dates = marketTimeline.get_trading_days(from_date, to_date)

    print("\nGenrating new r1k csv file by carry forwarding the data...")
    copy_block = old_r1k[old_r1k["date"]==copy_date].reset_index(drop=True)
    all_new_r1k = []
    for dt in update_dates:
        current_df = copy_block.copy()
        current_df["date"] = pd.Timestamp(dt).normalize().tz_localize(None)
        all_new_r1k.append(current_df)
    all_new_r1k = pd.concat(all_new_r1k, ignore_index=True)
    new_r1k = pd.concat([old_r1k, all_new_r1k], ignore_index=True)

    print("Genrating new r3k csv file by carry forwarding the data...")
    copy_block = old_r3k[old_r3k["date"]==copy_date].reset_index(drop=True)
    all_new_r3k = []
    for dt in update_dates:
        current_df = copy_block.copy()
        current_df["date"] = pd.Timestamp(dt).normalize().tz_localize(None)
        all_new_r3k.append(current_df)
    all_new_r3k = pd.concat(all_new_r3k, ignore_index=True)
    new_r3k = pd.concat([old_r3k, all_new_r3k], ignore_index=True)

    return new_r1k, new_r3k


def find_missing_tickers(r3k_valid, update_dir, sec_master_nodup_file, new_sec_file):
    r3k_tickers = list(r3k_valid["ticker"].unique())
    print("\nNo. of new valid r3k unique tickers - ", len(r3k_tickers))

    sec_master = pd.read_csv(os.path.join(update_dir, sec_master_nodup_file)).drop("Unnamed: 0", axis=1)
    print("\n{} - {}".format(sec_master_nodup_file, sec_master.shape))
    #print(sec_master.tail())

    sec_tickers = list(sec_master["ticker"].unique())
    print(f"No. of unique tickers in {sec_master_nodup_file} - {len(sec_tickers)}")
    missing = list(set(r3k_tickers) - set(sec_tickers))
    missing.sort()
    print(f"\n{len(missing)} tickers are missing in new valid r3k compared to {sec_master_nodup_file} file")
    print(missing)

    r3k_cusips = list(r3k_valid["cusip"].unique())
    sec_cusips = list(sec_master["cusip"].unique())
    missing_cusips = list(set(r3k_cusips) - set(sec_cusips))
    missing_cusips.sort()
    print(f"\n{len(missing_cusips)} cusips are missing in new valid r3k compared to {sec_master_nodup_file} file")
    print(missing_cusips)

    new_sec_master = pd.read_csv(os.path.join(update_dir, new_sec_file))
    new_missing_tickers = list(set(missing) - set(new_sec_master["ticker"].unique()))
    print(f"\n{len(new_missing_tickers)} tickers are missing after further delta with {new_sec_file} file")
    new_missing_tickers.sort()
    print(new_missing_tickers)

    new_missing_cusips = list(set(missing_cusips) - set(new_sec_master["cusip"].unique()))
    print(f"\n{len(new_missing_cusips)} cusips are missing after further delta with {new_sec_file} file")
    new_missing_cusips.sort()
    print(new_missing_cusips)

    update_missing_cusips = list(set(new_missing_cusips)-set(ignore_cusips))
    update_missing_cusips.sort()
    print(f"\n{len(update_missing_cusips)} cusips only are new missing after removing the ignore_cusips")
    print(update_missing_cusips)

    update_missing_tickers = list(set(new_missing_tickers) - set(orig_missing_tickers))
    return update_missing_tickers, update_missing_cusips


def find_tickers_cusips_in_r3k(update_dir, update_missing_tickers, update_missing_cusips):
    r3k_full = pd.read_hdf(os.path.join(update_dir, "r3k_full.h5"))
    print("\nLast date in full new r3k: ", r3k_full["date"].max())

    r3k_full_ld = r3k_full[r3k_full["date"]==r3k_full["date"].max()]
    print("\nShape of new full r3k on last date: ", r3k_full_ld.shape)

    cond = r3k_full_ld["ticker"].isin(update_missing_tickers) | r3k_full_ld["cusip"].isin(update_missing_cusips)
    cond.sum()
    r3k_full_ld = r3k_full_ld[cond]
    r3k_full_ld = r3k_full_ld.drop_duplicates(subset=['cusip', 'ticker'], keep='last')
    r3k_full_ld = r3k_full_ld.drop("unnamed: 0", axis=1)
    print("\nNew full r3k on last date with missing tickers/cusips only: ", r3k_full_ld.shape)
    print(r3k_full_ld.sort_values(by=['ticker']).iloc[:,:22])

    full_tickers = list(r3k_full_ld["ticker"].unique())
    full_cusips = list(r3k_full_ld["cusip"].unique())
    still_missing_tickers = list(set(update_missing_tickers)-set(full_tickers))
    still_missing_cusips = list(set(update_missing_cusips)-set(full_cusips))

    if still_missing_cusips or still_missing_cusips:
        print(f"\n{len(still_missing_tickers)} are still missing tickers after finding missing tickers in new full r3k on last date")
        print(still_missing_tickers)

        print(f"\n{len(still_missing_cusips)} are still missing cusips after finding missing cusips in new full r3k on last date")
        print(still_missing_cusips)

        print("\nFinding still missing tickers/cusips in older dates in r3k_full...")
        #r3k_full = pd.read_hdf(os.path.join(update_dir, "r3k_full.h5"))
        cond = r3k_full["ticker"].isin(still_missing_tickers) | r3k_full["cusip"].isin(still_missing_cusips)
        cond.sum()
        r3k_full_sm = r3k_full[cond]
        r3k_full_sm = r3k_full_sm.sort_values(["ticker", "date"])
        r3k_full_sm = r3k_full_sm.drop_duplicates(subset=['cusip', 'ticker'], keep='last')
        r3k_full_sm = r3k_full_sm.drop("unnamed: 0", axis=1)
        print("\nNew full r3k with still missing tickers/cusips only: ", r3k_full_sm.shape)
        print(r3k_full_sm.sort_values(by=['ticker']).iloc[:,:22])


def download_quandl_mapping(update_dir):
    print("\nDownloading quandl data...")
    quandl_data = quandl.get_table("SHARADAR/TICKERS", paginate=True)

    date_str = str(pd.Timestamp.now().date()).replace("-", "")
    quandl_file = os.path.join(update_dir, "quandl_mapping_new_{0}.csv".format(date_str))
    quandl_data.to_csv(quandl_file)
    print("Downloaded quandl mapping data to ", quandl_file)


def fill_rem_cusips(r1k_merged, cusip_fillers):
    cusip_rem = list(r1k_merged[pd.isnull(r1k_merged["dcm_security_id"])]["cusip"].unique())
    print("\nRemaining cusips: ", cusip_rem)
    print("\nTrying to fill remaining cusips using r1k_cusip_ticker_id.csv file...")

    still_rem_cusips = list(set(cusip_rem) - set(list(cusip_fillers["cusip"])))
    if still_rem_cusips:
        print("\nFew cusips are still remaining after filling. Might need to add them to r1k_cusip_ticker_id.csv or ignore_cusip list!")
        print(still_rem_cusips)
        mf = input('\nPress enter to exit: ')
        if mf == '':
            sys.exit()

    print("\nUsing cusip_fillers file to fill remaining cusips...")
    for c in cusip_rem:
        cond = cusip_fillers["cusip"]==c
        ticker = cusip_fillers.loc[cond, "ticker"].values[-1]
        sec_id = cusip_fillers.loc[cond, "dcm_security_id"].values[-1]
        r1k_merged.loc[r1k_merged["cusip"]==c, "dcm_security_id"] = sec_id
    print("Filled the remaining cusips using cusip_fillers file.")

    cusip_rem_2 = list(r1k_merged[pd.isnull(r1k_merged["dcm_security_id"])]["cusip"].unique())
    assert len(cusip_rem_2)==0, "Some of the cusips are still remaining!"

    return r1k_merged


def generate_new_r3k_r1k(old_r3k, old_r1k, r3k_valid, new_sec_master, cusip_fillers):
    print("\nGenerating new r3k.csv...")

    old_last_date = old_r3k["date"].max()
    old_last_index = old_r3k.index[-1]

    r3k_new = r3k_valid[r3k_valid["date"] > old_last_date]
    r3k_new = r3k_new[~r3k_new.cusip.isin(ignore_cusips)]
    print("\nShape of new r3k: ", r3k_new.shape)

    combined_r3k = pd.concat([old_r3k, r3k_new], ignore_index=True)
    print("Combined r3k (old+new)'s old_last_index+1:10 -> ")
    print(combined_r3k[old_last_index+1:].head(10))

    print("\nGenerating new r1k.csv...")
    r1k_new = r3k_new[["date", "cusip"]]

    new_sec_master = new_sec_master.drop("Unnamed: 0", axis=1)
    new_sec_master["latest_update"] = new_sec_master["latest_update"].apply(pd.Timestamp)
    sorted_master = new_sec_master.sort_values(["latest_update", "dcm_security_id"])
    sorted_master = sorted_master.drop_duplicates(["cusip"], keep="last")

    r1k_merged = pd.merge(r1k_new, sorted_master[["cusip", "dcm_security_id"]], on=["cusip"], how="left")

    r1k_filled = fill_rem_cusips(r1k_merged, cusip_fillers)
    print("\nNo missing cusips remaining!")

    combined_r1k = pd.concat([old_r1k, r1k_filled], ignore_index=True)

    return combined_r3k, combined_r1k


def verify_new_r3k_r1k(update_dir):
    # TEST PIPELINE SECTION
    print("\nVerifying the new generated r3k and r1k files...")
    r3k = pd.read_csv(os.path.join(update_dir, "r3k.csv"), index_col=0)
    r1k = pd.read_csv(os.path.join(update_dir, "r1k.csv"), index_col=0)

    r3k.columns = r3k.columns.str.lower()
    r3k['date'] = pd.to_datetime(r3k['date'])
    r3k[['wt-r1', 'wt-r1g', 'wt-r1v']] = r3k[['wt-r1', 'wt-r1g', 'wt-r1v']].fillna(0)
    r1k.columns = r1k.columns.str.lower()
    r1k['date'] = pd.to_datetime(r1k['date'])
    r1k["cusip"] = r1k["cusip"].astype(str)
    r3k["cusip"] = r3k["cusip"].astype(str)
    r1k["dcm_security_id"] = r1k["dcm_security_id"].astype(int)

    try:
        df = pd.merge(r3k, r1k, how='left', on=['date', 'cusip'])
    except:
        print("\nNot able to join r3k and r1k as required! There is some problem. Need to revisit.")
        raise
    print("New generated r3k/r1k files pass the verification test!")


def generate_industry_map(update_dir, new_sec_file):
    new_sec_master = pd.read_csv(os.path.join(update_dir, new_sec_file), index_col=0)

    print("\nGenerating the updated industry map file...")
    industry_columns = ["ticker", "Security", "GICS_Sector", "GICS_Industry", "GICS_IndustryGroup", "GICS_SubIndustry", "Sector", "Industry", "IndustryGroup", "SubIndustry", "dcm_security_id"]
    industry_map = new_sec_master[industry_columns].rename(columns={"ticker": "Ticker"})
    for c in ["GICS_Sector", "GICS_Industry", "GICS_IndustryGroup", "GICS_SubIndustry"]:
        for i in industry_map.index:
            try:
                industry_map.loc[i, c] = int(industry_map.loc[i, c])
            except:
                pass

    industry_map_file = os.path.join(update_dir, "industry_map.csv")
    industry_map.to_csv(industry_map_file)
    print("\nGenerated updated industry map file at ", industry_map_file)


def main():
    update_dir = "/home/naveen/Documents/dcm/notebooks/russell_update"
    old_dir = "/home/naveen/Documents/dcm/notebooks/russell_update/old_files"
    sec_master_nodup_file = "security_master_nodup_202010.csv"
    new_sec_file = "security_master_20220529.csv"
    r1k_cusip_ticker_id_file = "r1k_cusip_ticker_id.csv"

    mf = input("\nPlease verify the file paths in the script once and "
               "\nensure you have got the most recently updated old r3k/r1k files. "
               "\nPress Y/y to move forward: ")
    if (mf != 'Y' and mf != 'y'):
        print("\nExiting...")
        sys.exit()

    print("\nReading old r3k and r1k csv files...")
    old_r3k = pd.read_csv(os.path.join(old_dir, "r3k.csv"), parse_dates=["date"]).drop("Unnamed: 0", axis=1)
    old_r1k = pd.read_csv(os.path.join(old_dir, "r1k.csv"), parse_dates=["date"]).drop("Unnamed: 0", axis=1)

    old_last_date = old_r3k["date"].max()
    old_last_index = old_r3k.index[-1]
    print("last date of old r3k: ", old_last_date)
    print("last index of old r3k: ", old_last_index)

    cs = input("\nPress C/c to carry-forward the dates without updating the security master"
               " \nelse press enter if you have got the updated russell holdings and want to update security master first: ")
    if (cs == 'C' or cs == 'c'):
        new_r1k, new_r3k = carry_forward(old_r1k, old_r3k)

        new_r1k.to_csv(os.path.join(update_dir, "r1k.csv"))
        print("\nGenerated new r1k file to ", os.path.join(update_dir, "r1k.csv"))

        new_r3k.to_csv(os.path.join(update_dir, "r3k.csv"))
        print("Generated new r3k file to ", os.path.join(update_dir, "r3k.csv"))

    else:
        print("\nReading new r3k.h5 ...")
        r3k_h5 = pd.read_hdf(os.path.join(update_dir, "r3k.h5"))
        print("new r3k h5 - ", r3k_h5.shape)
        print(r3k_h5.tail())

        cond = (r3k_h5["wt-r1"]>0.0) | (r3k_h5["wt-r1g"]>0.0) | (r3k_h5["wt-r1v"]>0.0)
        r3k_valid = r3k_h5[cond]
        print("\nNew valid r3k (with weights > 0): ", r3k_valid.shape)
        print(r3k_valid.tail())

        update_missing_tickers, update_missing_cusips = find_missing_tickers(r3k_valid, update_dir, sec_master_nodup_file, new_sec_file)
        print("\nNew missing tickers: ", update_missing_tickers)

        if update_missing_tickers:
            update_missing_tickers.sort()
            print(f"\n{len(update_missing_tickers)} tickers only are new missing after comparing with original missing tickers that need to be updated in the security master")
            print(update_missing_tickers)

            print("\nLooks like security master needs an update!!!"
                  " First step should be to add them to the universe file for adjustment pipeline.")

            mf = input("\nPress Y/y to find missing tickers/cusips in new r3k files to help with security_master update else any other key: ")
            if mf == 'Y' or mf == 'y':
                find_tickers_cusips_in_r3k(update_dir, update_missing_tickers, update_missing_cusips)

            mf = input("\nPress Y/y to generate quandl mapping file,"
                       " S/s to skip if you don't need or already downloaded it today: ")
            if (mf == 'Y' or mf == 'y'):
                download_quandl_mapping(update_dir)

            print("\nPlease update the security master file, and run this script again!!!")
            print("\nExiting...")
            sys.exit()

        else:
            print("All the missing tickers seem to be originally missing as well."
                  "\nNo new tickers are to be updated in security master!")

            if update_missing_cusips:
                print("\nFew cusips are missing, these will be tried to be filled using r1k_cusip_ticker_id file or ignored during r3k/r1k csv files generation.")
                print("Fetching the missing cusip information from latest russell files, in case needed for r1k_cusip_ticker_id.csv file update...")
                find_tickers_cusips_in_r3k(update_dir, update_missing_tickers, update_missing_cusips)

            mf = input("\nPress Y/y to move forward to generate the new r3k/r1k files else X/x: ")
            if (mf == 'X' or mf == 'x'):
                print("Exiting...")
                sys.exit()

            new_sec_master = pd.read_csv(os.path.join(update_dir, new_sec_file))
            cusip_fillers = pd.read_csv(os.path.join(update_dir, r1k_cusip_ticker_id_file)).drop("Unnamed: 0", axis=1)
            combined_r3k, combined_r1k = generate_new_r3k_r1k(old_r3k, old_r1k, r3k_valid, new_sec_master, cusip_fillers)

            mf = input("\nPress Y/y to write the new combined r3k.csv and r1k.csv: ")
            if (mf == 'Y' or mf == 'y'):
                combined_r3k.to_csv(os.path.join(update_dir, "r3k.csv"))
                combined_r1k.to_csv(os.path.join(update_dir, "r1k.csv"))
                print("\nWritten the new r3k.csv and r1k.csv files at ", update_dir)
            else:
                print("Exiting...")
                sys.exit()

    verify_new_r3k_r1k(update_dir)

    mf = input("\nPress enter to move forward to generate the industry map file: ")
    generate_industry_map(update_dir, new_sec_file)

    print("\nFinished Successfully!!!")


if __name__=="__main__":
    main()