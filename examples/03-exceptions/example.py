import time

import kopf

E2E_TRACEBACKS = True
E2E_CREATE_TIME = 3.5
E2E_SUCCESS_COUNTS = {}
E2E_FAILURE_COUNTS = {'create_fn': 1}


class MyException(Exception):
    pass


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(retry, **kwargs):
    time.sleep(0.1)  # for different timestamps of the events
    if not retry:
        raise kopf.TemporaryError("First failure.", delay=1)
    elif retry == 1:
        raise MyException("Second failure.")
    else:
        raise kopf.PermanentError("Third failure, the final one.")
