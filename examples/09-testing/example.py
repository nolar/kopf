from typing import Any

import kopf


@kopf.on.create('kopfexamples')
def create_fn(logger: kopf.Logger, **_: Any) -> None:
    logger.info("Something was logged here.")
