from typing import Any

import kopf


@kopf.on.create('kopfexamples')
def create_fn(**_: Any) -> None:
    pass
