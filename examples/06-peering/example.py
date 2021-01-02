import kopf


@kopf.on.create('kopfexamples')
def create_fn(**kwargs):
    pass
