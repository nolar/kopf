import kopf


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn_1(**kwargs):
    print('CREATED 1st')


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn_2(**kwargs):
    print('CREATED 2nd')


@kopf.on.update('zalando.org', 'v1', 'kopfexamples')
def update_fn(old, new, diff, **kwargs):
    print('UPDATED')


@kopf.on.delete('zalando.org', 'v1', 'kopfexamples')
def delete_fn_1(**kwargs):
    print('DELETED 1st')


@kopf.on.delete('zalando.org', 'v1', 'kopfexamples')
def delete_fn_2(**kwargs):
    print('DELETED 2nd')


@kopf.on.field('zalando.org', 'v1', 'kopfexamples', field='spec.field')
def field_fn(old, new, **kwargs):
    print(f'FIELD CHANGED: {old} -> {new}')
