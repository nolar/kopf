import kopf


@kopf.on.resume('kopfexamples')
def resume_fn_1(**kwargs):
    print(f'RESUMED 1st')


@kopf.on.create('kopfexamples')
def create_fn_1(**kwargs):
    print('CREATED 1st')


@kopf.on.resume('kopfexamples')
def resume_fn_2(**kwargs):
    print(f'RESUMED 2nd')


@kopf.on.create('kopfexamples')
def create_fn_2(**kwargs):
    print('CREATED 2nd')


@kopf.on.update('kopfexamples')
def update_fn(old, new, diff, **kwargs):
    print('UPDATED')


@kopf.on.delete('kopfexamples')
def delete_fn_1(**kwargs):
    print('DELETED 1st')


@kopf.on.delete('kopfexamples')
def delete_fn_2(**kwargs):
    print('DELETED 2nd')


@kopf.on.field('kopfexamples', field='spec.field')
def field_fn(old, new, **kwargs):
    print(f'FIELD CHANGED: {old} -> {new}')
