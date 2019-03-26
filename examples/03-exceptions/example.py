import time

import kopf


class MyException(Exception):
    pass


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(retry, **kwargs):
    time.sleep(2)  # for different timestamps of the events
    if not retry:
        raise Exception("First failure.")
    elif retry == 1:
        raise MyException("Second failure.")
    else:
        pass
