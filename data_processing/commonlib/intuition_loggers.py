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
holdings_logger = logging.getLogger("holdings_logger")

log_unit_exec = logging.getLogger("unit_execution_request_logger")
log_broker_exec = logging.getLogger("broker_ib_execution_logger")

log_flex_order = logging.getLogger("flex_order_logger")
log_flex_order_update = logging.getLogger("flex_order_update_logger")
log_flex_street_update = logging.getLogger("flex_street_update_logger")
log_flex_transact = logging.getLogger("flex_transaction_logger")

jump_recording_logger = logging.getLogger("jump_recorder")


transaction_headers = "leg_id,security_id,amount,status_transaction,external_broker_order_id,created_dt_transaction,"\
    "executed_dt_transaction,execution_price,execution_amount,observed_price,pre_trade_risk_approval,commision,"\
    "broker_reconciliation_id,target_transaction_capital,execution_transaction_capital"
full_transaction_headers = "requester_id,requester_order_no,{0}".format(transaction_headers)
order_headers = "purpose,requester_id,requester_order_no,supervisor_id,lifecycle_id,created_dt_order,"\
                "executor_order_no,executor_id,realized_gl,executed_dt_order,status_order,pre_trade_risk_clearance,"\
                "pre_trade_risk_reviewer,requester_memo,execution_memo,order_commission,target_order_capital,execution_order_capital"
tlcr_headers = "trader_id,stage,requested_funding_amount,request_dt,request_reviewer_id,request_id,request_status,"\
              "initial_reserved_funding_amount,request_review_dt,initial_utilized_funding_amount,"\
              "initial_execution_dt,running_cost,running_cost_delta_from_last_value,last_running_cost_dt,closing_proceeds,closing_dt,duration,"\
              "realized_gl_tlcr,total_commission,commission_delta_from_last_value,last_commission_dt,additional_request_information"
holdings_headers = "owner,book_realized_gl,book_commission,security_id,position_dt,amount,price_basis,"\
                     "position_realized_gl,position_realized_gl_long,position_realized_gl_short,position_last_known_price,"\
                     "position_last_known_price_date"


def write_headers_for_loggers():
    log_order.info(order_headers)
    log_transact.info(full_transaction_headers)
    log_port.info("total_unrealized_gL,total_realized_gL,total_long,total_short,total_commission,used_capital")
    log_tlcr.info(tlcr_headers)
    log_executor_transact.info(full_transaction_headers)
    reconciliation_logger.info(transaction_headers)
    holdings_logger.info(holdings_headers)

    log_flex_order.info("dcm_order_id,leg_id,symbol,order_type,side,price,quantity,strategy,substrategy,ai_trader,"
                        "time_in_force,broker,algo,fix_tags,notes")
    log_flex_order_update.info("dcm_order_id,leg_id,order_id,symbol,status,filled,avg_px,final,replaced,compliance")
    log_flex_street_update.info("dcm_order_id,leg_id,order_id,street_order_id,symbol,broker,status,qty,filled,avg_px,"
                                "commissions,fees,settle_date,replaced,replaced_ord_ids")
    log_flex_transact.info("dcm_order_id,leg_id,symbol,execution_time,execution_amount,execution_price,commission,"
                           "execution_id")
