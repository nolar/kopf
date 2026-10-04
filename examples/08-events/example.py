from typing import Any

import kopf


@kopf.on.event('kopfexamples')
def event_fn_with_error(**_: Any) -> None:
    raise Exception("Oops!")


@kopf.on.event('kopfexamples')
def normal_event_fn(event: kopf.RawEvent, **_: Any) -> None:
    print(f"Event received: {event!r}")
