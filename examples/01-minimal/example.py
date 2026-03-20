from typing import Any

import kopf


@kopf.on.create('kopfexamples')
def create_fn(spec: kopf.Spec, **_: Any) -> Any:
    print(f"And here we are! Creating: {spec}")
    return {'message': 'hello world'}  # will be the new status
