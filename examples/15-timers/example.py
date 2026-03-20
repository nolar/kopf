from typing import Any

import kopf


@kopf.timer('kopfexamples', idle=5, interval=2)
def every_few_seconds_sync(spec: kopf.Spec, logger: kopf.Logger, **_: Any) -> None:
    logger.info(f"Ping from a sync timer: field={spec['field']!r}")


@kopf.timer('kopfexamples', idle=10, interval=4)
async def every_few_seconds_async(spec: kopf.Spec, logger: kopf.Logger, **_: Any) -> None:
    logger.info(f"Ping from an async timer: field={spec['field']!r}")
