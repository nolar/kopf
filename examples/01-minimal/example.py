import kopf


@kopf.on.create('kopfexamples')
@kopf.on.update('kopfexamples')
def create_fn(meta, spec, reason, logger, **kwargs):
    rv = meta.get('resourceVersion')
    logger.warning(f">>> {rv=} And here we are! {reason=}: {spec}")


# @kopf.on.create('kopfexamples')
# def create_fn2(spec, **kwargs):
#     print(f"And here we are! Creating2: {spec}")
