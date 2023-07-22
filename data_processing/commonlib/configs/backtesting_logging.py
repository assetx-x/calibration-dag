# pylint: skip-file
# noinspection PyStatementEffect
{
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "root_formatter": {
            "format": "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "asofdate_and_message_log": {
            "format": "%(asofdt)s :: %(message)s"
        },
        "asofdate_and_message_csv": {
            "format": "%(asofdt)s,%(message)s"
        },
        "pretty_message": {
            "format": "%(pretty_message)s"
        },
        "pickled_message": {
            "format": "%(pickled_message)s"
        },
        "raw_message": {
            "format": "%(message)s"
        },
        "asofdate_actualdate_and_message_csv": {
            "format": "'%(asctime)s.%(msecs)03d','%(asofdt)s',%(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "slack_alert": {
            "format": "INTUITION-ALERT : %(asofdt)s :: %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "asofdate_and_message_log",
            "stream": "ext://sys.stdout",
            "level": "WARN"
        },
        "root_console": {
            "class": "logging.StreamHandler",
            "formatter": "root_formatter",
            "stream": "ext://sys.stdout",
            "level": "INFO"
        },
        "root_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "root_formatter",
            "level": "INFO",
            "filename": "{0}_{1}_root.log".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "info_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_log",
            "filename": "{0}_{1}_info.log".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "debug_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "asofdate_and_message_log",
            "filename": "{0}_{1}_debug.log".format(path.join(get_config()['logging']['logging_location'],
                                                             get_config()['run_description']['run_name']),
                                                   get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "full_message_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "raw_message",
            "filename": "{0}_{1}_message.log".format(path.join(get_config()['logging']['logging_location'],
                                                               get_config()['run_description']['run_name']),
                                                     get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "unit_execution_request_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_log",
            "filename": "{0}_{1}_unit_exec.log".format(path.join(get_config()['logging']['logging_location'],
                                                                 get_config()['run_description']['run_name']),
                                                       get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "order_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_orders.csv".format(path.join(get_config()['logging']['logging_location'],
                                                              get_config()['run_description']['run_name']),
                                                    get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "transaction_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_transact.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                get_config()['run_description']['run_name']),
                                                      get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "holdings_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_holdings.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                get_config()['run_description']['run_name']),
                                                      get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "reconciliation_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_reconciliation.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                      get_config()['run_description']['run_name']),
                                                            get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "executor_transaction_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_executor_transact.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                         get_config()['run_description']['run_name']),
                                                               get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "portfolio_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_port.csv".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "trading_lifecycle_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_tlcr.csv".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "configuration_json_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "pretty_message",
            "filename": "{0}_{1}_config.json".format(path.join(get_config()['logging']['logging_location'],
                                                               get_config()['run_description']['run_name']),
                                                     get_config()['run_description']['run_datetime']),
        },
        "configuration_pickle_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "pickled_message",
            "filename": "{0}_{1}_config.pickle".format(path.join(get_config()['logging']['logging_location'],
                                                                 get_config()['run_description']['run_name']),
                                                       get_config()['run_description']['run_datetime']),
        },
        "broker_ib_message_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_actualdate_and_message_csv",
            "filename": "{0}_{1}_ib.log".format(path.join(get_config()['logging']['logging_location'],
                                                          get_config()['run_description']['run_name']),
                                                get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "broker_ib_execution_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_actualdate_and_message_csv",
            "filename": "{0}_{1}_ib_exec.log".format(path.join(get_config()['logging']['logging_location'],
                                                               get_config()['run_description']['run_name']),
                                                     get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "market_slicer_ib_message_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_actualdate_and_message_csv",
            "filename": "{0}_{1}_data.csv".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "market_slicer_data_container_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_actualdate_and_message_csv",
            "filename": "{0}_{1}_slicers_data.log".format(path.join(get_config()['logging']['logging_location'],
                                                                    get_config()['run_description']['run_name']),
                                                          get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "pairtrader_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_pair_traders.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                    get_config()['run_description']['run_name']),
                                                          get_config()['run_description']['run_datetime']),
            "maxBytes": get_config()['logging']['max_file_size'],
            "backupCount": get_config()['logging']['backup_count']
        },
        "flex_order_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_flex_order.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                  get_config()['run_description']['run_name']),
                                                        get_config()['run_description']['run_datetime']),
        },
        "flex_order_update_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_flex_update.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                   get_config()['run_description']['run_name']),
                                                         get_config()['run_description']['run_datetime']),
        },
        "flex_street_update_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_flex_street.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                   get_config()['run_description']['run_name']),
                                                         get_config()['run_description']['run_datetime']),
        },
        "flex_transaction_csv_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_flex_transact.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                     get_config()['run_description']['run_name']),
                                                           get_config()['run_description']['run_datetime']),
        }
    },
    "filters": {
        "add_asofdate": {
            "()": AddAsOfDateFilter
        },
        "add_pickle_data": {
            "()": AddPickleAndPrettyPrintDataFilter
        }
    },
    "loggers": {
        "run_logger": {
            "level": "INFO",
            "handlers": ["console", "info_log_file", "debug_log_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "unit_execution_request_logger": {
            "level": "INFO",
            "handlers": ["unit_execution_request_log_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "order_logger": {
            "level": "INFO",
            "handlers": ["order_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "transaction_logger": {
            "level": "INFO",
            "handlers": ["transaction_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "holdings_logger": {
            "level": "INFO",
            "handlers": ["holdings_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "reconciliation_logger": {
            "level": "INFO",
            "handlers": ["reconciliation_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "executor_transaction_logger": {
            "level": "INFO",
            "handlers": ["executor_transaction_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "portfolio_logger": {
            "level": "INFO",
            "handlers": ["portfolio_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "trading_lifecycle_logger": {
            "level": "INFO",
            "handlers": ["trading_lifecycle_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "configuration_logger": {
            "level": "INFO",
            "filters": ["add_pickle_data"],
            "handlers": ["configuration_json_file", "configuration_pickle_file"],
            "propagate": False
        },
        "broker_ib_message_logger": {
            "level": "INFO",
            "handlers": ["broker_ib_message_log_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "broker_ib_execution_logger": {
            "level": "INFO",
            "handlers": ["broker_ib_execution_log_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "market_slicer_ib_message_logger": {
            "level": "INFO",
            "handlers": ["market_slicer_ib_message_log_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "market_slicer_data_container_logger": {
            "level": "INFO",
            "handlers": ["market_slicer_data_container_log_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "full_message_logger": {
            "level": "INFO",
            "handlers": ["full_message_log_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "pair_trader_logger": {
            "level": "INFO",
            "handlers": ["pairtrader_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "flex_order_logger": {
            "level": "INFO",
            "handlers": ["flex_order_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "flex_order_update_logger": {
            "level": "INFO",
            "handlers": ["flex_order_update_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "flex_street_update_logger": {
            "level": "INFO",
            "handlers": ["flex_street_update_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "flex_transaction_logger": {
            "level": "INFO",
            "handlers": ["flex_transaction_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["root_console", "root_log_file"]
    }
}
