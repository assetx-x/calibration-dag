import os
import pandas as pd


def find_baskets_overlap(quantile, ins1, ins2):
    overlaps = []
    for actor_id in ins1.actorID.values:
        if "Long" in actor_id and "Short" in actor_id:
            lb1 = eval(ins1[ins1["actorID"]==actor_id]["long_basket"].iloc[0])
            lb2 = eval(ins2[ins2["actorID"]==actor_id]["long_basket"].iloc[0])
    
            lb_intersection = set(lb1).intersection(set(lb2))
            lb_union = set(lb1).union(set(lb2))
    
            int_by_single = round((len(lb_intersection)/len(lb1))*100, 2)
            int_by_union = round((len(lb_intersection)/len(lb_union))*100, 2)
            overlaps.append([actor_id, quantile, "long_basket", int_by_single, int_by_union])

            sb1 = eval(ins1[ins1["actorID"]==actor_id]["short_basket"].iloc[0])
            sb2 = eval(ins2[ins2["actorID"]==actor_id]["short_basket"].iloc[0])
    
            sb_intersection = set(sb1).intersection(set(sb2))
            sb_union = set(sb1).union(set(sb2))
    
            int_by_single = round((len(sb_intersection)/len(sb1))*100, 2)
            int_by_union = round((len(sb_intersection)/len(sb_union))*100, 2)
            overlaps.append([actor_id, quantile, "short_basket", int_by_single, int_by_union])

    overlap_df = pd.DataFrame(overlaps, columns=['actor_id', 'quantile', 'basket', 'intersec_by_single (%)', 'intersec_by_union (%)'])
    return overlap_df


if __name__=='__main__':

    # Put <Q> in place of quantile number in the file paths below which will be replaced with all three quantiles later
    ins_template1 = "/home/naveen/Documents/dcm/notebooks/ins/20220307/20220306_gan_consolidated_<Q>.csv"
    ins_template2 = "/home/naveen/Documents/dcm/notebooks/ins/20220228/20220227_gan_consolidated_<Q>.csv"

    all_overlaps = pd.DataFrame(columns=['actor_id','quantile', 'basket', 'intersec_by_single (%)', 'intersec_by_union (%)'])

    pd.set_option('display.max_colwidth', -1)

    quantiles = ['8', '13', '30']
    for quantile in quantiles:
        ins_file1 = ins_template1.replace("<Q>", quantile)
        ins_file2 = ins_template2.replace("<Q>", quantile)
        if not (os.path.isfile(ins_file1) and os.path.isfile(ins_file2)):
            print("{} or {} could not be found!".format(ins_file1, ins_file2))
            print("\nSkipping quantile ", quantile)
            continue

        ins1 = pd.read_csv(ins_file1, parse_dates=["date"], quotechar="'")
        ins2 = pd.read_csv(ins_file2, parse_dates=["date"], quotechar="'")

        quantile_overlap_df = find_baskets_overlap(quantile, ins1, ins2)
        all_overlaps = pd.concat([all_overlaps, quantile_overlap_df], ignore_index=True)

    all_overlaps = all_overlaps.sort_values(by=["intersec_by_single (%)"])
    print(all_overlaps.to_string(index=False, justify='left'))

    print("\nintersec_by_single - No. of common tickers in baskets from both files /No. of tickers in one instruction file basket - "
          "Range -> [ {}%  -  {}% ]".format(all_overlaps["intersec_by_single (%)"].min(), all_overlaps["intersec_by_single (%)"].max()))
    print("intersec_by_union - No. of common tickers in baskets from both files /No. of combined tickers in baskets from both files - "
          "Range -> [ {}%  -  {}% ]".format(all_overlaps["intersec_by_union (%)"].min(), all_overlaps["intersec_by_union (%)"].max()))

    tmp_path = "/tmp/overlaps.csv"
    all_overlaps.to_csv(tmp_path, index=False)
    print("\nDumped overlaps csv file to ", tmp_path)