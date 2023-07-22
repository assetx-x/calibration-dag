import pandas as pd
from commonlib.intuition_objects_representation import ObjectRepresenterMetaclass

class DCMIntuitionObject(object, metaclass=ObjectRepresenterMetaclass):
    EXCLUDED_ATTRS_FOR_STATE = []

    def __reduce__(self):
        return (self.__class__, self.__getinitargs__(), self.__getstate__(),)

    def __getinitargs__(self):
        raise DCMException("DCMIntuitionObject.__getinitargs__ needs to be overriden in derived class {0}"
                           .format(self.__class__))

    def __getstate__(self):
        attributes_to_save = set(self.__dict__.keys()).difference(set(self.__class__.EXCLUDED_ATTRS_FOR_STATE))
        state = {k:getattr(self, k) for k in attributes_to_save if not isinstance(getattr(self, k), property)}
        return state

    def __setstate__(self, state):
        self._pre_state_recovery_process()
        excluded_attributes = set(self.__class__.EXCLUDED_ATTRS_FOR_STATE)
        attributes_to_recover = set(state.keys()).difference(excluded_attributes)
        for k in attributes_to_recover:
            setattr(self, k, state[k])
        #self._post_state_recovery_process()

    def _pre_state_recovery_process(self):
        pass

    def _post_state_recovery_process(self):
        pass


class DCMException(Exception):
    """ A class for all objects that are an exception for DCM
    purposes. For now, no implementation whatsoever
    """
    def __init__(self, message, publish=True):
        #if publish==True: - Need to figure out solution if slack is bombarded with messages
        #                    above the acceptable rate where it may block us
        if False:
            try:
                from prod_tools.reporting_apps.slack_reporting import SlackReportingException
                SlackReportingException().publish_message([('Exception', message),
                                                       ('as_of_date', str(pd.Timestamp.now())), ])
            except:
                pass
        super().__init__(message)


class DCMActorTestingException(Exception):
    """ A class for all objects that are an exception for DCM unit testing
    purposes. For now, no implementation whatsoever
    """
    pass
