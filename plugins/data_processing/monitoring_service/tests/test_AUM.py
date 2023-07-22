from commonlib import jsonpickle_customization
import jsonpickle
from trader import Trader
from market import Market
from trading_unit import TraderSupervisor
from security_master import SecurityMaster, set_security_master, get_security_master
from communication_channel import set_channel, DequeCommunicationChannel, get_channel
from copy import deepcopy
import pytest
from dcm_trading_actor import ActorRegistry
from datetime import date
from trading_information import (TradingBook, OrderRequest, OrderRequestLeg, ExecutionDetailsSummary, TradingPosition,
                                 TradingPositionGroupAggregator, capital_basis_per_instrument,
                                 capital_basis_per_long_short)
from commonlib.subjects import Ticker
import pandas as pd
from commonlib.config import get_config, load
from os import path
from commonlib import actor_clock
import numpy as np
from collections import namedtuple, Counter
import pandas as pd
from itertools import product
from commonlib.util_functions import pickle_copy
from corporate_actions import CAType, DividendCorporateAction, SplitCorporateAction, ConversionCACash, \
    ConversionCAStock, CorporateActionBuilder
from execution_unit import ExecutionUnitLoopback

initial_order_info = namedtuple("initial_order_info", ["leg_id", "security_id", "requested_amount", "observed_price"])
position_result_info = namedtuple("position_result_info",
                                  ["security_id", "position_dt", "amount", "price_basis", "realized_gl",
                                   "realized_gl_ls"])
OrderTestCase = namedtuple("OrderTestCase", ["order1", "execution1", "state1", "order2", "execution2", "state2"])
# long+long, long+short(equal), long+short(smaller), long+short(larger),
# short+short, short+long(equal), short+long(smaller), short+long(larger)
test_ticker = Ticker("X")


@pytest.fixture
def order_template():
    executed_order_json = '{"py/object": "trading_information.OrderRequest", "requester_order_no": 1, "executor_id": "ExecutionUnit:NonGuaranteedComboLimitExecutionExpert", "executor_order_number": 1, "executed_dt": {"py/object": "pandas._libs.tslib.Timestamp", "tz": "US/Eastern", "__str_repr": "2015-08-24 09:32:00"}, "execution_memo": null, "pre_trade_risk_reviewer": "ExecutionUnit:ExecutionRM", "requester_memo": "L position on earnings for ABBV", "order_legs": {"json://2": {"py/object": "trading_information.OrderRequestLeg", "status": 3, "broker_reconciliation_id": "dd28c84323a3459188c1aad78ba28ba9", "executed_dt": {"py/id": 2}, "pre_trade_risk_approval": true, "leg_id": 2, "external_broker_order_id": "dd28c84323a3459188c1aad78ba28ba9", "observed_price": 50, "commission": -5.0, "amount": -600, "execution_price": 50, "execution_instructions": {"py/id": 7}, "created_dt": {"py/object": "pandas._libs.tslib.Timestamp", "tz": "US/Eastern", "__str_repr": "2015-08-24 09:38:00"}, "security_id": {"py/object": "commonlib.subjects.Ticker", "symbol": "AAPL"}, "execution_amount": -600}, "json://1": {"py/object": "trading_information.OrderRequestLeg", "status": 3, "broker_reconciliation_id": "6369a118d12f4da8b0ca867ddcf86b0b", "executed_dt": {"py/id": 2}, "pre_trade_risk_approval": true, "leg_id": 1, "external_broker_order_id": "6369a118d12f4da8b0ca867ddcf86b0b", "observed_price": 15, "commission": -10.0, "amount": 1500, "execution_price": 15.0, "execution_instructions": {"py/object": "trading_information.ExecutionInstructions", "instructions": {"timeLimit": {"py/object": "commonlib.util_classes.TradingBusinessMinute", "py/state": [34, 9, 30, 16, 0]}, "grouping": null}}, "created_dt": {"py/id": 1}, "security_id": {"py/object": "commonlib.subjects.Ticker", "symbol": "ABBV"}, "execution_amount": 1500}}, "executed_status": true, "execution_instructions": {"py/object": "trading_information.ExecutionInstructions", "instructions": {"limit_parameters": {"py/object": "trading_information.LimitOrderParams", "py/state": null, "py/newargs": {"py/tuple": [0, 0.01]}}, "timeLimit": {"py/object": "datetime.timedelta", "tot_secs": 3600.0}, "execution_style": "HedgeCombo", "grouping": {"json://2": 1, "json://1": 1}}}, "created_dt": {"py/object": "pandas._libs.tslib.Timestamp", "tz": "US/Eastern", "__str_repr": "2015-08-24 09:31:00"}, "order_purpose": 1, "realized_gl": 0.0, "pre_trade_risk_clearance": true, "requester_id": "EarningsUnit:AAPL_ABBV", "trading_lifecycle_record_id": 3, "supervisor_id": "EarningsUnit:SUP"}'
    executed_order = jsonpickle.loads(executed_order_json, keys=True)
    return executed_order


@pytest.fixture
def order_template2():
    executed_order_json = '{"py/object": "trading_information.OrderRequest", "requester_order_no": 1, "executor_id": "ExecutionUnit:NonGuaranteedComboLimitExecutionExpert", "executor_order_number": 1, "executed_dt": {"py/object": "pandas._libs.tslib.Timestamp", "tz": "US/Eastern", "__str_repr": "2015-08-24 09:32:00"}, "execution_memo": null, "pre_trade_risk_reviewer": "ExecutionUnit:ExecutionRM", "requester_memo": "L position on earnings for ABBV", "order_legs": {"json://2": {"py/object": "trading_information.OrderRequestLeg", "status": 3, "broker_reconciliation_id": "dd28c84323a3459188c1aad78ba28ba9", "executed_dt": {"py/id": 2}, "pre_trade_risk_approval": true, "leg_id": 2, "external_broker_order_id": "dd28c84323a3459188c1aad78ba28ba9", "observed_price": 50, "commission": -5.0, "amount": -600, "execution_price": 50, "execution_instructions": {"py/id": 7}, "created_dt": {"py/object": "pandas._libs.tslib.Timestamp", "tz": "US/Eastern", "__str_repr": "2015-08-24 09:38:00"}, "security_id": {"py/object": "commonlib.subjects.Ticker", "symbol": "AAPL"}, "execution_amount": -600}, "json://1": {"py/object": "trading_information.OrderRequestLeg", "status": 3, "broker_reconciliation_id": "6369a118d12f4da8b0ca867ddcf86b0b", "executed_dt": {"py/id": 2}, "pre_trade_risk_approval": true, "leg_id": 1, "external_broker_order_id": "6369a118d12f4da8b0ca867ddcf86b0b", "observed_price": 15, "commission": -10.0, "amount": 1500, "execution_price": 15.0, "execution_instructions": {"py/object": "trading_information.ExecutionInstructions", "instructions": {"timeLimit": {"py/object": "commonlib.util_classes.TradingBusinessMinute", "py/state": [34, 9, 30, 16, 0]}, "grouping": null}}, "created_dt": {"py/id": 1}, "security_id": {"py/object": "commonlib.subjects.Ticker", "symbol": "ABBV"}, "execution_amount": 1500}}, "executed_status": true, "execution_instructions": {"py/object": "trading_information.ExecutionInstructions", "instructions": {"limit_parameters": {"py/object": "trading_information.LimitOrderParams", "py/state": null, "py/newargs": {"py/tuple": [0, 0.01]}}, "timeLimit": {"py/object": "datetime.timedelta", "tot_secs": 3600.0}, "execution_style": "HedgeCombo", "grouping": {"json://2": 1, "json://1": 1}}}, "created_dt": {"py/object": "pandas._libs.tslib.Timestamp", "tz": "US/Eastern", "__str_repr": "2015-08-24 09:31:00"}, "order_purpose": 1, "realized_gl": 10.0, "pre_trade_risk_clearance": true, "requester_id": "EarningsUnit:AAPL_ABBV", "trading_lifecycle_record_id": 3, "supervisor_id": "EarningsUnit:SUP"}'
    executed_order = jsonpickle.loads(executed_order_json, keys=True)
    return executed_order


@pytest.fixture
def order_LS(order_template):
    new_order = pickle_copy(order_template)

    # Long Position Manipulation
    new_order.order_legs[1].amount = 100
    new_order.order_legs[1].execution_amount = 100

    new_order.order_legs[1].execution_price = 45
    new_order.order_legs[1].observed_price = 50

    # Short Position Manipulation
    new_order.order_legs[2].amount = -50
    new_order.order_legs[2].execution_amount = -50

    new_order.order_legs[2].execution_price = 10
    new_order.order_legs[2].observed_price = 15

    # manipulate fields here
    return new_order


@pytest.fixture
def order_LS_close_L(order_template):
    new_order = pickle_copy(order_template)
    # manipulate fields here
    return new_order


@pytest.fixture
def order_LS_close_S(order_template):
    new_order = pickle_copy(order_template)
    # manipulate fields here
    return new_order


@pytest.fixture
def comm_channel(prepared_config):
    new_comm_channel = DequeCommunicationChannel()
    set_channel(new_comm_channel)


@pytest.fixture
def supervisor(comm_channel):
    sup = TraderSupervisor("EarningsUnit:SUP", None, [])
    return sup


@pytest.yield_fixture
def prepared_config(request, reload_config):
    load(load_and_setup_logging=False)
    original_value_source = get_config()["corporate_actions"]["corporate_actions_source"]
    original_value_application = get_config()["corporate_actions"]["adjust_trading_books_for_corporate_actions"]
    input_file_path = path.join(path.dirname(__file__), "test_trading_book_inputs", "yahoo_ca_AAPL_ABBV.csv")
    original_value_verification = get_config()["corporate_actions"]["verify_corporate_actions_with_user"]
    get_config()["corporate_actions"]["corporate_actions_source"] = input_file_path
    get_config()["corporate_actions"]["adjust_trading_books_for_corporate_actions"] = True
    get_config()["corporate_actions"]["verify_corporate_actions_with_user"] = False

    def return_config_to_original_state():
        get_config()["corporate_actions"]["corporate_actions_source"] = original_value_source
        get_config()["corporate_actions"]["adjust_trading_books_for_corporate_actions"] = original_value_application
        get_config()["corporate_actions"]["verify_corporate_actions_with_user"] = original_value_verification

    request.addfinalizer(return_config_to_original_state)


@pytest.fixture
def approved_trading_lifecycle():
    approved_lifecycle_json = '{"py/object":"trading_information.TradingLifecycleRecord","commission_delta_from_last_value":null,"request_reviewer_id":"EarningsUnit:SUP","initial_reserved_funding_amount":40000.0,"commission_expense":0.0,"trader_id":"EarningsUnit:AAPL_ABBV","running_cost_delta_from_last_value":null,"requested_funding_amount":40000.0,"closing_dt":null,"request_dt":{"py/object":"pandas._libs.tslib.Timestamp","tz":"US/Eastern","__str_repr":"2015-08-24 09:31:00"},"request_review_dt":{"py/object":"pandas._libs.tslib.Timestamp","tz":"US/Eastern","__str_repr":"2015-08-24 09:31:00"},"request_status":true,"last_running_cost_dt":null,"additional_request_information":{"py/object":"trading_information.TradingLifecycleAuxiliaryInformation","trader_ranking":6183.0},"initial_execution_dt":null,"last_commission_dt":null,"request_id":3,"initial_utilized_funding_amount":null,"running_cost":null,"closing_proceeds":null,"stage":2}'
    approved_lifecycle = jsonpickle.loads(approved_lifecycle_json)
    return approved_lifecycle


@pytest.fixture(scope="module")
def actor_clock_initialized():
    new_clock = actor_clock.ActorClock()
    new_clock.update_time(pd.Timestamp("2015-08-25 09:32:00-04:00"))
    actor_clock.set_clock(new_clock)


@pytest.fixture
def trader(supervisor, approved_trading_lifecycle, actor_clock_initialized):
    trader = Trader("EarningsUnit:AAPL_ABBV")
    trader.stop_gl = (-0.4, 1.0)
    """scenarioA = TradingPosition(security_id='X',position_dt='2017-03-31', amount = 10, price_basis= 100, realized_gl=2.0)
    scenarioB = TradingPosition(security_id='X',position_dt='2017-03-31', amount = 10, price_basis= 100, realized_gl=2.0)
    case_scenario = scenarioA + scenarioB
    expected_scenario_results ={
    'amount': 20,
    'position_dt': '2017-03-31',
    'price_basis': 100.0,
    'realized_gl': 4.0,
    'security_id': 'X'}
    case_scenario_list = [i for i in [case_scenario.amount, case_scenario.position_dt,
                                      case_scenario.price_basis,case_scenario.realized_gl,
                                      case_scenario.security_id]]
    expected_scenario_list = [i for i in [expected_scenario_results['amount'], expected_scenario_results['position_dt'],
                                      expected_scenario_results['price_basis'],expected_scenario_results['realized_gl'],
                                      expected_scenario_results['security_id']]]"""
    trader.set_actor_for_role(ActorRegistry.ROLE_SUPERVISOR, supervisor.identity)
    trader.current_trading_lifecycle_record = approved_trading_lifecycle
    trader.market = Market()
    # create trader, register supervisor for it, setup tradinglifecycle record, register executed_order
    return trader


def test_trader(trader, order_LS, order_LS_close_L, order_LS_close_S):
    assert isinstance(trader, Trader)
    assert trader.get_current_trader_capital() == 40000
    trader.trading_book.open_orders.register_open_order(order_LS)
    trader.record_and_notify_supervisor_of_execution_result(order_LS)
    assert trader.get_current_trader_capital() == 40000


    # Stop here, even the capital amount will still be 40000 because there was no
    # realized_gl or unrealized_gl

    """trader.trading_book.open_orders.register_open_order(order_LS_close_L)
    trader.record_and_notify_supervisor_of_execution_result(order_LS_close_L)
    assert trader.get_current_trader_capital() == 45000
    trader.trading_book.open_orders.register_open_order(order_LS_close_S)
    trader.record_and_notify_supervisor_of_execution_result(order_LS_close_S)
    assert trader.get_current_trader_capital() == 35000"""
