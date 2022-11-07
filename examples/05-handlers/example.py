import kopf


@kopf.on.delete('kopfexamples')
def delete_fn(retry, logger, **_):
    if retry < 3:
        raise kopf.TemporaryError("no yet", delay=5)
    logger.info('DELETED')
