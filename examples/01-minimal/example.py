import kopf


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(spec, **kwargs):
    print(f"And here we are! Creating: {spec}")
    return {'message': 'hello world'}  # will be the new status
