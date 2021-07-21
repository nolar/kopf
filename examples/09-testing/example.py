import kopf


@kopf.on.create('kopfexamples')
def create_fn(logger, **kwargs):
    logger.info("Something was logged here.")
