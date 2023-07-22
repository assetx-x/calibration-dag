# pylint: skip-file
# noinspection PyStatementEffect
{
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "message_only": {
            "format": "%(message)s",
        },
        "root_formatter": {
            "format": "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "asofdate_and_wall_time": {
            "format": "%(asctime)s.%(msecs)03d (%(asofdt)s) %(levelname)s: %(message)s",
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
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "asofdate_and_wall_time",
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
            "maxBytes": 20485760,
            "backupCount": 10
        },
        "unit_execution_request_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_log",
            "filename": "{0}_{1}_info.log".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
            "maxBytes": 20485760,
            "backupCount": 10
        },
        "info_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_log",
            "filename": "{0}_{1}_info.log".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
            "maxBytes": 20485760,
            "backupCount": 10
        },
        "info_log_file_fluentd": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.info",
            "formatter": "asofdate_and_message_log"
        },
        "debug_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "asofdate_and_message_log",
            "filename": "{0}_{1}_debug.log".format(path.join(get_config()['logging']['logging_location'],
                                                             get_config()['run_description']['run_name']),
                                                   get_config()['run_description']['run_datetime']),
            "maxBytes": 20485760,
            "backupCount": 10
        },
        "full_message_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "raw_message",
            "filename": "{0}_{1}_message.log".format(path.join(get_config()['logging']['logging_location'],
                                                               get_config()['run_description']['run_name']),
                                                     get_config()['run_description']['run_datetime']),
            "maxBytes": 20485760,
            "backupCount": 10
        },
        "full_message_log_file_fluentd": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.full_message",
            "formatter": "raw_message"
        },
        "order_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_orders.csv".format(path.join(get_config()['logging']['logging_location'],
                                                              get_config()['run_description']['run_name']),
                                                    get_config()['run_description']['run_datetime']),
        },
        "order_csv_file_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.order_csv",
            "formatter": "asofdate_and_message_csv"
        },
        "transaction_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_transact.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                get_config()['run_description']['run_name']),
                                                      get_config()['run_description']['run_datetime']),
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
        "transaction_csv_file_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.transaction_csv",
            "formatter": "asofdate_and_message_csv"
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
        "reconciliation_csv_file_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.reconciliation_csv",
            "formatter": "asofdate_and_message_csv"
        },
        "executor_transaction_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_executor_transact.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                         get_config()['run_description']['run_name']),
                                                               get_config()['run_description']['run_datetime']),
        },
        "executor_transaction_csv_file_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.executor_transaction_csv",
            "formatter": "asofdate_and_message_csv"
        },
        "portfolio_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_port.csv".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
        },
        "portfolio_csv_file_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.portfolio_csv",
            "formatter": "asofdate_and_message_csv"
        },
        "trading_lifecycle_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_tlcr.csv".format(path.join(get_config()['logging']['logging_location'],
                                                            get_config()['run_description']['run_name']),
                                                  get_config()['run_description']['run_datetime']),
        },
        "trading_lifecycle_csv_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.trading_lifecycle_csv",
            "formatter": "asofdate_and_message_csv"
        },
        "configuration_json_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "pretty_message",
            "filename": "{0}_{1}_config.json".format(path.join(get_config()['logging']['logging_location'],
                                                               get_config()['run_description']['run_name']),
                                                     get_config()['run_description']['run_datetime']),
        },
        "configuration_json_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.configuration_json",
            "formatter": "pretty_message"
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
            "maxBytes": 20485760,
            "backupCount": 10
        },
        "broker_ib_message_log_file_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.broker_ib_message",
            "formatter": "asofdate_actualdate_and_message_csv"
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
            "maxBytes": 20485760,
            "backupCount": 10
        },
        "market_slicer_ib_message_log_file_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.market_slicer_ib",
            "formatter": "asofdate_actualdate_and_message_csv"
        },
        "market_slicer_data_container_log_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "asofdate_actualdate_and_message_csv",
            "filename": "{0}_{1}_slicers_data.log".format(path.join(get_config()['logging']['logging_location'],
                                                                    get_config()['run_description']['run_name']),
                                                          get_config()['run_description']['run_datetime']),
            "maxBytes": 20485760,
            "backupCount": 10
        },
        "market_slicer_data_container_fluent": {
            "class": "fluent.handler.FluentHandler",
            "level": "INFO",
            "host": "localhost",
            "port": 24224,
            "tag": "intuition.market_slicer_data_container",
            "formatter": "asofdate_actualdate_and_message_csv"
        },
        "flex_order_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_flex_order.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                  get_config()['run_description']['run_name']),
                                                        get_config()['run_description']['run_datetime']),
        },
        "flex_order_update_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_flex_update.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                   get_config()['run_description']['run_name']),
                                                         get_config()['run_description']['run_datetime']),
        },
        "flex_street_update_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_flex_street.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                   get_config()['run_description']['run_name']),
                                                         get_config()['run_description']['run_datetime']),
        },
        "flex_transaction_csv_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "asofdate_and_message_csv",
            "filename": "{0}_{1}_flex_transact.csv".format(path.join(get_config()['logging']['logging_location'],
                                                                     get_config()['run_description']['run_name']),
                                                           get_config()['run_description']['run_datetime']),
        },
        "jump_recording_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "message_only",
            "filename": "{0}_{1}_jump_recording.csv".format(path.join(get_config()['logging']['logging_location'],
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
            "handlers": ["console", "info_log_file", "info_log_file_fluentd", "root_log_file"],
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
            "handlers": ["order_csv_file", "order_csv_file_fluent"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "transaction_logger": {
            "level": "INFO",
            "handlers": ["transaction_csv_file", "transaction_csv_file_fluent"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "transaction_logger": {
            "level": "INFO",
            "handlers": ["holdings_csv_file"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "reconciliation_logger": {
            "level": "INFO",
            "handlers": ["reconciliation_csv_file", "reconciliation_csv_file_fluent"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "executor_transaction_logger": {
            "level": "INFO",
            "handlers": ["executor_transaction_csv_file", "executor_transaction_csv_file_fluent"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "portfolio_logger": {
            "level": "INFO",
            "handlers": ["portfolio_csv_file", "portfolio_csv_file_fluent"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "trading_lifecycle_logger": {
            "level": "INFO",
            "handlers": ["trading_lifecycle_csv_file", "trading_lifecycle_csv_fluent"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "configuration_logger": {
            "level": "INFO",
            "filters": ["add_pickle_data"],
            "handlers": ["configuration_json_file", "configuration_pickle_file", "configuration_json_fluent"],
            "propagate": False
        },
        "broker_ib_message_logger": {
            "level": "INFO",
            "handlers": ["broker_ib_message_log_file", "broker_ib_message_log_file_fluent"],
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
            "handlers": [],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "market_slicer_data_container_logger": {
            "level": "INFO",
            "handlers": ["market_slicer_data_container_log_file", "market_slicer_data_container_fluent"],
            "filters": ["add_asofdate"],
            "propagate": False
        },
        "full_message_logger": {
            "level": "INFO",
            "handlers": ["full_message_log_file", "full_message_log_file_fluentd"],
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
        },
        "jump_recorder": {
            "level": "INFO",
            "handlers": ["jump_recording_file"],
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["root_console", "root_log_file"]
    }
}
