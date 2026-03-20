from typing import Any

import kopf


@kopf.on.resume('kopfexamples')
def resume_fn_1(**_: Any) -> None:
    print(f'RESUMED 1st')


@kopf.on.create('kopfexamples')
def create_fn_1(**_: Any) -> None:
    print('CREATED 1st')


@kopf.on.resume('kopfexamples')
def resume_fn_2(**_: Any) -> None:
    print(f'RESUMED 2nd')


@kopf.on.create('kopfexamples')
def create_fn_2(**_: Any) -> None:
    print('CREATED 2nd')


@kopf.on.update('kopfexamples')
def update_fn(old: Any, new: Any, diff: kopf.Diff, **_: Any) -> None:
    print('UPDATED')


@kopf.on.delete('kopfexamples')
def delete_fn_1(**_: Any) -> None:
    print('DELETED 1st')


@kopf.on.delete('kopfexamples')
def delete_fn_2(**_: Any) -> None:
    print('DELETED 2nd')


@kopf.on.field('kopfexamples', field='spec.field')
def field_fn(old: Any, new: Any, **_: Any) -> None:
    print(f'FIELD CHANGED: {old} -> {new}')
