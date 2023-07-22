import logging


log = logging.getLogger("run_logger")
log_order = logging.getLogger("order_logger")
log_transact = logging.getLogger("transaction_logger")
log_executor_transact = logging.getLogger("executor_transaction_logger")
log_port = logging.getLogger("portfolio_logger")
log_tlcr = logging.getLogger("trading_lifecycle_logger")
market_slicer_ib_logger = logging.getLogger("market_slicer_ib_message_logger")
market_slicer_data_logger = logging.getLogger("market_slicer_data_container_logger")
broker_ib_logger = logging.getLogger("broker_ib_message_logger")
reconciliation_logger = logging.getLogger("reconciliation_logger")

log_unit_exec = logging.getLogger("unit_execution_request_logger")
log_broker_exec = logging.getLogger("broker_ib_execution_logger")


def write_headers_for_loggers():
    transaction_headers = "leg_id,security_id,amount,status,order_broker_id,created_dt,executed_dt,"\
        "execution_price,execution_amount,observed_price,pre_trade_approval,commision,reconciliation_id,"\
        "target_transaction_capital,execution_transaction_capital"
    log_order.info("purpose,requester_id,requester_order_no,supervisor_id,lifecycle_id,created_dt,"
                   "executor_order_no,executor_id,realized_gl,executed_dt,status,pre-clearance,risk reviewer,"
                   "requester_memo,execution_memo,order_commission,target_order_capital,execution_order_capital")
    log_transact.info("executor_order_no,{0}".format(transaction_headers))
    log_port.info("total_unrealized_gL,total_realized_gL,total_long,total_short,total_commission,used_capital")
    log_tlcr.info("trader_id,requested_funding_amount,request_dt,request_reviewer_id,request_id,request_status,"
                  "initial_reserved_funding_amount,request_review_dt,initial_utilized_funding_amount,"
                  "initial_execution_dt,running_cost,last_running_cost_dt,closing_proceeds,closing_dt,duration,"
                  "realized_gl,total_commission")
    log_executor_transact.info("executor_order_no,{0}".format(transaction_headers))
    reconciliation_logger.info(transaction_headers)
