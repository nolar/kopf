from typing import Any

import kopf


@kopf.timer('kopfexamples', idle=5, interval=2)
def every_few_seconds_sync(spec: kopf.Spec, logger: kopf.Logger, **_: Any) -> None:
    logger.info(f"Ping from a sync timer: field={spec['field']!r}")


@kopf.timer('kopfexamples', idle=7, interval=4)
async def every_few_seconds_async(spec: kopf.Spec, logger: kopf.Logger, **_: Any) -> None:
    logger.info(f"Ping from an async timer: field={spec['field']!r}")


# Marks for the e2e tests (see tests/e2e/test_examples.py):
# the 1st timer triggers at 5, 7, 9 (next: 11); the 2nd timer triggers at 7 (next: 11).
E2E_CREATION_TIME_LIMIT = 10  # the whole test duration in this case
E2E_SUCCESS_COUNTS = {'every_few_seconds_sync': 3, 'every_few_seconds_async': 1}
