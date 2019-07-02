import kopf


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(logger, **kwargs):
    logger.info("Something was logged here.")
