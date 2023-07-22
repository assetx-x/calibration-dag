import traceback
import subprocess
from datetime import datetime, timedelta
from time import sleep
from sys import exit as sys_exit, exc_info as sys_exc_info
from os import path

import atexit
import psutil
from ib.opt import ibConnection, message

from config import get_config
from commonlib.intuition_loggers import *


class IBConnection:
    def __init__(self, clientId, register_callbacks_dict, register_all_callbacks_list):
        self.TIME_LIMIT_TO_RECONNECT = timedelta(seconds=get_config()["ibpy"]["IBConnection_time_limit_to_connect_in_seconds"])

        cb_wrapper = IBConnection._wrap_callback_with_sys_exit_on_exception
        self.register_callbacks_dict = {k:map(cb_wrapper, register_callbacks_dict[k]) for k in register_callbacks_dict}
        self.register_all_callbacks_list = [cb_wrapper(f) for f in register_all_callbacks_list]
        new_connection = ibConnection(clientId=clientId, host=get_config()['IBControllerWrapper']['host'],
                                      port=get_config()['IBControllerWrapper']['port'])
        new_connection.register(self._reconnect_on_connection_closed, message.connectionClosed)
        new_connection.dispatcher.enableLogging(False)

        for msg_type in self.register_callbacks_dict:
            for cb in self.register_callbacks_dict[msg_type]:
                new_connection.register(cb, msg_type)
        for cb in self.register_all_callbacks_list:
            new_connection.registerAll(cb)

        self.tws_connection = new_connection
        log.debug("IBConnection.__init__ : All callbacks for clientId %s are properly registered ", clientId)
        self.status = None
        self._attempt_connection()

    def _attempt_connection(self, is_reconnect=False):
        first_attempt_time = datetime.now()
        last_valid_attempt_time = first_attempt_time + self.TIME_LIMIT_TO_RECONNECT
        self.status = False
        if is_reconnect:
            while not self.status and datetime.now() <= last_valid_attempt_time:
                self.status = self.tws_connection.reconnect()
        else:
            while not self.status and datetime.now() <= last_valid_attempt_time:
                self.status = self.tws_connection.connect()


        if self.status:
            log.debug("IBConnection.__attempt_connection was able to connect tws_connection in less than %s seconds",
                      self.TIME_LIMIT_TO_RECONNECT)
        else:
            raise Exception("IBConnection.__attempt_connection: There was an issue in tws_connection. The "
                               "connection did not happen in %s seconds", self.TIME_LIMIT_TO_RECONNECT)

    def _reconnect_on_connection_closed(self, msg):
        self._attempt_connection(is_reconnect=True)

    @staticmethod
    def _wrap_callback_with_sys_exit_on_exception(cb):
        def cb_with_sys_exit(msg):
            try:
                cb(msg)
            except Exception as e:
                log.error("There was an error with the callback %s. Error message is %s", cb, e.message)
                exc_type, exc_value = sys_exc_info()[0:2]
                log.error("Full traceback follows \n: %s", traceback.format_exc())
                log.error("Exception type: %s \n Exception value: %s \n", exc_type, exc_value)
                sys_exit()
        return cb_with_sys_exit

    def dettach_all_callbacks(self):
        for msg_type in self.register_callbacks_dict:
            for cb in self.register_callbacks_dict[msg_type]:
                self.tws_connection.unregister(cb, msg_type)
        for cb in self.register_all_callbacks_list:
            self.tws_connection.unregisterAll(cb)
        log.debug("IBConnection.__init__ : All callbacks for clientId %s are properly unregistered ",
                  self.tws_connection.clientId)

    def disconnect_from_tws(self, req_global_cancel=True):
        self.tws_connection.reqGlobalCancel() if req_global_cancel else None
        return self.tws_connection.disconnect()

    def get_connection_status(self):
        return self.status

    def get_client_id(self):
        return self.tws_connection.clientId


class IBConnectionManager(object):
    IBController_pid = None
    tws_clients = None

    @classmethod
    def _connect_ibcontroller(cls):
        ib_connection_config = get_config()["IBControllerWrapper"]
        ib_command = ib_connection_config["IBController_command"]
        ib_controller_process_name = ib_connection_config["IBController_process_name"]
        if cls.IBController_pid is None:
            if not path.exists(ib_command):
                raise Exception("IBControllerWrapper.__connect_ibcontroller - the specified path to run IBController"
                                   " {0} does not exists. Verify your "
                                   "configuration".format(ib_command))
            IBController_subprocess = subprocess.Popen(ib_command)

            conn_time_limit = ib_connection_config["IBController_waitout_sleep_time_in_secs"]
            sleep(conn_time_limit)

            all_active_executable_processes = []
            for k in psutil.process_iter():
                try:
                    if len(k.cmdline()) != 0:
                        all_active_executable_processes.append((" ".join(k.cmdline()), k.pid))
                except psutil.AccessDenied:
                    pass
            ib_controller_process = [k for k in all_active_executable_processes
                                     if k[0].find(ib_controller_process_name) != -1]

            if not ib_controller_process:
                raise Exception("IBConnectionManager.__connect_ibcontroller - attempted to start the IB "
                                   "Controller process and failed; a search on the system for an existing process "
                                   "did not locate it either. Searched for {0}, started with {1}"
                                   .format(ib_controller_process_name, ib_command))
            cls.IBController_pid = ib_controller_process[0][1]

    @classmethod
    def _disconnect_ibcontroller(cls):
        if cls.IBController_pid:
            current_processes_list = psutil.Process(cls.IBController_pid).children(recursive=True)
            current_processes_list.append(psutil.Process(cls.IBController_pid))
            for current_child_process in current_processes_list:
                try:
                    current_child_process.kill()
                except psutil.NoSuchProcess:
                    pass
            sleep(30)
            # now let's check processes got killes; requesting process.status() should raise a psutil.NoSuchProcess
            # exception; if it does not get triggered the process did not got killed
            for current_child_process in current_processes_list:
                try:
                    if current_child_process.status() and current_child_process.status() != psutil.STATUS_ZOMBIE:
                        raise Exception("Could not kill the IBController process {0}".format(cls.IBController_pid))
                except psutil.NoSuchProcess:
                    pass
            log.debug("IBConnectionManager.__disconnect_ibcontroller could kill the IBController processes and all of "
                      "its children processes")
            cls.IBController_pid = None

    @classmethod
    def get_tws_connection(cls, clientId, register_callbacks_dict, register_all_callbacks_list):
        if not cls.tws_clients:
            cls.tws_clients = {}
            IBConnectionManager._connect_ibcontroller()
        if clientId in cls.tws_clients:
            raise Exception("Then clientId {0} already exists in the list of IBController.tws_clients. "
                               "IBConnectionManager.get_tws_connection is trying to initiate another tws_connection "
                               "using a similiar cliendId".format(clientId))
        requester_connection = IBConnection(clientId, register_callbacks_dict, register_all_callbacks_list)
        cls.tws_clients[clientId] = requester_connection
        return requester_connection

    @classmethod
    def dispose_tws_connection(cls, ib_connection, req_global_cancel=True):
        clientId = ib_connection.get_client_id()
        if clientId not in cls.tws_clients:
            raise Exception("Then clientId {0} does not exist in the list of IBController.tws_clients. "
                               "IBConnectionManager.dispose_tws_connection is trying to dispose an ib_connection related"
                               " to this cliendId".format(clientId))
        if cls.tws_clients[clientId] is not ib_connection:
            raise Exception("There is another connection with the same clinetId ({0}), "
                               "IBConnectionManager.dispose_tws_connection is trying to delete the connection correspoding"
                               " to this clientId, and it cannot.".format(clientId))
        disconnect_status = ib_connection.disconnect_from_tws(req_global_cancel)
        if not disconnect_status:
            log.error("The system could not disconnect from IB properly for clientId {0}".format(clientId))
        del cls.tws_clients[clientId]
        ib_connection = None
        if not cls.tws_clients:
            log.info("All of the tws_clients for IBConnectionManager are already disconnected. The dispose_tws_connection"
                     " is disconnecting the ib_controller")
            cls._disconnect_ibcontroller()
            cls.tws_clients = None
        return disconnect_status

    @classmethod
    def at_exit_disconnect_all(cls):
        if cls.tws_clients:
            for ib_connection in list(cls.tws_clients.values()):
                cls.dispose_tws_connection(ib_connection)
                log.debug("IBConnectionManager.at_exit_disconnect_all is disconnecting all ib_connections. The "
                          "connection for %s is already disconnected", ib_connection)

    @classmethod
    def is_ib_connected(cls):
        return cls.IBController_pid is None


atexit.register(lambda: IBConnectionManager.at_exit_disconnect_all())
