import kopf

E2E_TRACEBACKS = True
E2E_CREATION_STOP_WORDS = ['Something has changed,']
E2E_SUCCESS_COUNTS = {'eventual_success_with_few_messages': 1}
E2E_FAILURE_COUNTS = {'eventual_failure_with_tracebacks': 1, 'instant_failure_with_traceback': 1, 'instant_failure_with_only_a_message': 1}


class MyException(Exception):
    pass


@kopf.on.create('kopfexamples')
def instant_failure_with_only_a_message(**kwargs):
    raise kopf.PermanentError("Fail once and for all.")


@kopf.on.create('kopfexamples')
def eventual_success_with_few_messages(retry, **kwargs):
    if retry < 3:  # 0, 1, 2, 3
        raise kopf.TemporaryError("Expected recoverable error.", delay=1.0)


@kopf.on.create('kopfexamples', retries=3, backoff=1.0)
def eventual_failure_with_tracebacks(**kwargs):
    raise MyException("An error that is supposed to be recoverable.")


@kopf.on.create('kopfexamples', errors=kopf.ErrorsMode.PERMANENT, backoff=1.0)
def instant_failure_with_traceback(**kwargs):
    raise MyException("An error that is supposed to be recoverable.")
