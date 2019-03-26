import kopf


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(**kwargs):
    pass
