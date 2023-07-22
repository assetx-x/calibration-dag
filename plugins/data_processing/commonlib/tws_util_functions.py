from bs4 import BeautifulSoup
from commonlib.config import get_config
from commonlib.subjects import Ticker
import pandas as pd

from commonlib.util_functions import get_redshift_engine


def scrape_audittrail(path):
    data = []
    list_header = []
    soup = BeautifulSoup(open(path),'html.parser')
    header = soup.find_all("table")[0].find("tr")
    for items in header:
        try:
            list_header.append(items.get_text())
        except:
            continue
    HTML_data = soup.find_all("table")[0].find_all("tr")[1:]
    for element in HTML_data:
        sub_data = []
        for sub_element in element:
            try:
                sub_data.append(sub_element.get_text())
            except:
                continue
        data.append(sub_data)
    df = pd.DataFrame(data = data, columns = list_header)
    return df

def cleanup_audittrail_filled_df(raw_df):
    cleaned_df = raw_df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=False)
    cleaned_df = cleaned_df[cleaned_df['Action']=='Filled'].reset_index(drop=True)
    cleaned_df['ExecID'] = None
    cleaned_df['AllocatedTo'] = None
    cleaned_df['executed_dt_transaction'] = None
    cleaned_df['execution_amount'] = None
    cleaned_df['execution_price'] = None

    for row in cleaned_df.iterrows():
        if row[1]['Action']=="Filled":
            misc_elem = row[1]['Misc']
            row_list = misc_elem.split(';')
            for elem in row_list:
                elem_list = elem.split('=')
                if len(elem_list) > 1:
                    k= elem_list[0].replace(" ","")
                    v= elem_list[1].replace(" ","")
                    if (k == "ExecID"):
                        cleaned_df.at[row[0],"ExecID"] = v
                    elif (k == "OrderReference"):
                        cleaned_df.at[row[0],"AllocatedTo"] = v
                    elif (k == "TransactTime"):
                        cleaned_df.at[row[0],"executed_dt_transaction"] = v
                    elif (k == "CumQty"):
                        cleaned_df.at[row[0],"execution_amount"] = float(v)
                    elif (k == "AvgPx"):
                        cleaned_df.at[row[0],"execution_price"] = float(v)

    # Cleanup Size Column
    cleaned_df['Size'] = cleaned_df['Size'].astype('float')
    cleaned_df = cleaned_df[cleaned_df['Size'] != 0.0].reset_index(drop=True)
    # Cleanup executed_dt_transaction Column
    cleaned_df['executed_dt_transaction'] = pd.to_datetime(cleaned_df['executed_dt_transaction'])
    final_audittrail_df = cleaned_df.rename(columns={'Contract':'security_id','ExecID':'broker_reconciliation_id'})
    final_audittrail_df['commission'] = final_audittrail_df['execution_amount'].apply(get_commission)
    final_audittrail_df['dir'] = final_audittrail_df['Side'].apply(lambda x: 1 if x=='Buy' else -1)
    final_audittrail_df['execution_amount'] = final_audittrail_df['execution_amount']*final_audittrail_df['dir']
    final_audittrail_df['security_id'] = final_audittrail_df['security_id'].apply(Ticker)
    return(final_audittrail_df)

def get_commission(execution_amount):
    min_commission = get_config()["BrokerConnectionIB"]["min_commission_per_order"]
    if execution_amount == 0:
        commission = 0.0
    elif execution_amount>0:
        commission_per_share = get_config()["BrokerConnectionIB"]["commission_per_share_amount_for_longs"]
        commission = max(abs(execution_amount) * commission_per_share ,min_commission)
    else:
        commission_per_share = get_config()["BrokerConnectionIB"]["commission_per_share_amount_for_shorts"]
        commission = max(abs(execution_amount) * commission_per_share,min_commission)
    commission *= -1.0    
    return commission

def add_link_information(audittrail_df):
    engine = get_redshift_engine()
    table_name = 'subscriptions_client_brokers'
    
    filtered_df = audittrail_df[audittrail_df['AllocatedTo'].fillna("None").str.split(':').apply(len)==6].reset_index(drop=True)
    filtered_df[['acct_no','strategy','order_no','order_purpose','leg_id','order_id']] = filtered_df['AllocatedTo'].fillna("None").str.split(':', expand=True)

    #REMOVE reconciliation rows:
    filtered_df = filtered_df[filtered_df['strategy']!='reconciliation'].reset_index(drop=True)

    #LINK UID FETCH:
    filtered_df['link_uid'] = ""
    for account_no in filtered_df['acct_no'].unique():
        sql_text_query = f"SELECT link_uid FROM {table_name} WHERE account_id=\'{account_no}\' AND link_confirmed=True AND is_active=True"
        link_uid_df = pd.read_sql(sql_text_query, engine)
        #assert(len(link_uid_df)==1)
        filtered_df.loc[filtered_df[filtered_df['acct_no']==account_no].index,'link_uid']=link_uid_df.values[0][0]

    #ADDED TRADER COL:
    filtered_df['trader'] = filtered_df['link_uid']+ ':' + filtered_df['strategy']
    return filtered_df


if __name__=="__main__":
    #AUDITTRAIL_PATH="/tmp/audit7080863807154443513.tmps.html"
    AUDITTRAIL_PATH = "/home/dcmadmin/dcm-intuition/audit1079799941812631441.tmps.html"
    engine = get_redshift_engine()
    table_name = 'subscriptions_client_brokers'
    
    raw_df = scrape_audittrail(AUDITTRAIL_PATH)
    audittrail_df = cleanup_audittrail_filled_df(raw_df)
    filtered_df = add_link_information(audittrail_df)

    trading_unit_trader_list_dict = {link_uid: list(filtered_df[filtered_df['link_uid']==link_uid]['trader'].unique()) for link_uid in filtered_df['link_uid'].unique()}

    for strategy in filtered_df['trader'].unique():
        print(strategy)
    print(filtered_df)