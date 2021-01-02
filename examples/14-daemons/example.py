import asyncio
import time

import kopf


# Sync daemons in threads are non-interruptable, they must check for the `stopped` flag.
# This daemon exits after 3 attempts and then 30 seconds of running (unless stopped).
@kopf.daemon('kopfexamples', backoff=3)
def background_sync(spec, stopped, logger, retry, patch, **_):
    if retry < 3:
        patch.status['message'] = f"Failed {retry+1} times."
        raise kopf.TemporaryError("Simulated failure.", delay=1)

    started = time.time()
    while not stopped and time.time() - started <= 30:
        logger.info(f"=> Ping from a sync daemon: field={spec['field']!r}, retry={retry}")
        stopped.wait(5.0)

    patch.status['message'] = "Accompanying is finished."


# Async daemons do not need the `stopped` signal, they can rely on `asyncio.CancelledError` raised.
# This daemon runs forever (until stopped, i.e. cancelled). Yet fails to start for 3 first times.
@kopf.daemon('kopfexamples', backoff=3, cancellation_backoff=1.0, cancellation_timeout=0.5,
             annotations={'someannotation': 'somevalue'})
async def background_async(spec, logger, retry, **_):
    if retry < 3:
        raise kopf.TemporaryError("Simulated failure.", delay=1)

    while True:
        logger.info(f"=> Ping from an async daemon: field={spec['field']!r}")
        await asyncio.sleep(5.0)


E2E_CREATION_STOP_WORDS = ["=> Ping from"]
E2E_DELETION_STOP_WORDS = ["'background_async' is cancelled", "'background_sync' is cancelled", "'background_async' has exited"]
