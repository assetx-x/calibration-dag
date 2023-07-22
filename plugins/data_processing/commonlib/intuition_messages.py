import os.path
import pandas as pd

from commonlib.intuition_base_objects  import DCMIntuitionObject, DCMException
from commonlib.config import get_config
from commonlib.actor_clock import get_clock
from commonlib.util_functions import create_directory_if_does_not_exists
from commonlib.util_functions import pickle_copy

class MessageObject(DCMIntuitionObject):
    # __metaclass__ = ObjectRepresenterMetaclass
    """ A parent class for all objects that are supposed to be send
    on the communication channel. For now, no implementation whatsoever
    """
    EXCLUDED_ATTRS_FOR_STATE = []
    def __getinitargs__(self):
        return ()
    pass


class WrappedMessage(DCMIntuitionObject):
    """ A warapper around the object to be sent out to actors.
    In the future we should check for type, for now disabled for performance
    Attributes:
        sender: A variable indicating sender of message
        receiver: A variable indicating receiver for message
        messageObject: A variable indicating received fields
        send_dt: Indicates date and time when the message was sent.
        received_dt: Indicates date and time when the message was received.

    """
    EXCLUDED_ATTRS_FOR_STATE = ["sender","receiver","message"]
    def __init__(self, sender, receiver, messageObject):
        if not isinstance(messageObject, MessageObject):
            raise DCMException("WrappedMessage.__init__ - the received message %s of type %s is not a subclass "
                               "of MessageObject" % (messageObject, type(messageObject)))
        self.send_dt = get_clock().get_time()
        self.received_dt = None
        self.sender = sender
        self.receiver = receiver
        self.message = messageObject

    def __getinitargs__(self):
        return (self.sender, self.receiver, self.message)

    def create_response_message(self, response):
        """ Provides a message object with receiver, sender and response  information.
        """
        return WrappedMessage(self.receiver, self.sender, response)

    def get_identity_and_topic_of_sender(self):
        pass


class WrappedMessageCache(DCMIntuitionObject):
    def __init__(self):
        DCMIntuitionObject.__init__(self)
        self.messages = []
        self.topics_config = get_config()["DCMTradingActor"]["topics_configuration"]

    def clear(self):
        self.messages = []

    def record_message(self, msg):
        receiver_identity, receiver_code, receiver_channel = self.__get_topic_code(msg.receiver)
        sender_identity, sender_code, sender_channel = self.__get_topic_code(msg.sender)
        msg_content = pickle_copy(msg.message)
        self.messages.append([msg.send_dt, sender_identity, sender_code, sender_channel, receiver_identity,
                              receiver_code, receiver_channel, msg_content])

    def __get_topic_code(self, topic_str):
        topic_str = topic_str or ""
        topic_keys = [self.topics_config[k] for k in self.topics_config
                      if topic_str.endswith(self.topics_config[k]["TOPIC_SUFFIX"])]
        if len(topic_keys) != 1:
            identity = pd.np.NaN
            code = -1
            channel = topic_str
        else:
            identity = topic_str[0:-len(topic_keys[0]["TOPIC_SUFFIX"])]
            code = topic_keys[0]["TOPIC_INDEX"]
            channel = pd.np.NaN
        return (identity, code, channel)

    def flush(self, filepath):
        if self.messages:
            data = pd.DataFrame(self.messages, columns=["timestamp", "sender", "sender_topic", "sender_channel",
                                                        "receiver", "receiver_topic", "receiver_channel", "message"])
            dir_path = os.path.dirname(filepath)
            create_directory_if_does_not_exists(dir_path)
            with pd.HDFStore(filepath, "w", 9, "blosc") as store:
                store["message_data"] = data
            self.clear()