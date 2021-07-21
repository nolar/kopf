import kopf


@kopf.timer('kopfexamples', idle=5, interval=2)
def every_few_seconds_sync(spec, logger, **_):
    logger.info(f"Ping from a sync timer: field={spec['field']!r}")


@kopf.timer('kopfexamples', idle=10, interval=4)
async def every_few_seconds_async(spec, logger, **_):
    logger.info(f"Ping from an async timer: field={spec['field']!r}")
