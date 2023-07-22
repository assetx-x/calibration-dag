import requests
import jsonpickle
import pandas as pd
from commonlib import jsonpickle_customization

from bs4 import BeautifulSoup
from commonlib.subjects import Ticker
from ib_broker import BrokerConnectionIB
from ib_messages_buffer import IBMessageBuffer
from ib_security_master import SecurityMasterIB
from commonlib.market_timeline import marketTimeline
from ib_connection_manager import IBConnectionManager
from security_master import set_security_master, get_security_master
from commonlib.security_master_utils.identifier_utils import convert_isin_to_cusip
# from security_master_from_ca_files import get_ticker_list


def get_details_from_ib_web_portal(conid):
    url = "https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php?" \
          "action=Conid%20Info&wlId=IB&conid={conid}&lang=en".format(conid=conid)
    soup = BeautifulSoup(requests.get(url).content, "lxml")
    isin = str(soup.findAll("th", text="ISIN")[0].parent.find("td").text)

    # ticker = str(soup.findAll("th", text="Symbol")[0].parent.find("td").text)
    # currency = str(soup.findAll("th", text="Currency")[0].parent.find("td").text.split("(")[1].split(")")[0])

    stock_type = str(soup.findAll("th", text="Stock Type")[0].parent.find("td").text[0:-1])

    # tick_size = max([float(pd.parent.find("th", text="Increment").parent.findNextSibling().findAll("td")[1].text)
    #                 for pd in soup.findAll("th", text="Price Parameters")])

    lot_size = max([int(pd.parent.find("th", text="Increment").parent.findNextSibling().findAll("td")[1].text)
                    for pd in soup.findAll("th", text="Size Parameters")])
    cusip = convert_isin_to_cusip(isin)
    return (isin, cusip, stock_type, lot_size)


def get_sec_master_data(tradeable_instrument):
    sec_master_item = get_security_master().find_security_master_item(tradeable_instrument)
    item_info = sec_master_item.get_tradeable_instrument_info()
    brokerage_info = sec_master_item.get_brokerage_data()
    company_name = item_info["long_name"]
    ticker = item_info["market_name"]
    tick_size = item_info["min_tick_size"]
    industry = item_info["industry"]
    sector = item_info["sector"]
    currency = brokerage_info.m_currency
    main_exchange = brokerage_info.m_primaryExch
    sec_type = brokerage_info.m_secType
    conid = brokerage_info.m_conId

    isin, cusip, stock_type, lot_size = get_details_from_ib_web_portal(conid)
    data = [conid, ticker, isin, cusip, company_name, sector, industry, main_exchange, sec_type,
            stock_type, currency, tick_size, lot_size]
    print(data)


def populate_sec_master(ticker_list):
    set_security_master(SecurityMasterIB())
    broker_connection = BrokerConnectionIB(5)
    broker_connection.broker_connect()
    broker_connection.update_security_master(ticker_list)
    # broker_connection.broker_disconnect()


def _main():
    # from commonlib.config import load, set_config
    # set_config(load())
    # run_dt = marketTimeline.get_trading_day_using_offset(pd.Timestamp.today("US/Eastern"), -1)

    # ticker_list = map(Ticker, get_ticker_list(run_dt))[0:2]
    ticker_list = list(map(Ticker, ["GLD"]))
    populate_sec_master(ticker_list)

    from data_engine import DataEngine
    from ib_local_data_provider import InteractiveBrokersLocalEquityDataProvider

    provider = InteractiveBrokersLocalEquityDataProvider(provider_id="test")
    DataEngine(requirements, provider)

    sec_master_data = [get_sec_master_data(k) for k in ticker_list]
    print(sec_master_data)


if __name__=="__main__":
    _main()
