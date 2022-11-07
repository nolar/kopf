import asyncio

import logging

from unittest.mock import Mock

import pytest

import kopf
from kopf._core.intents.causes import Reason
from kopf._core.reactor.processing import process_resource_event


def test_(resource, registry, settings, handlers, caplog, cause_mock):
    caplog.set_level(logging.DEBUG)
    cause_mock.reason = Reason.CREATE

    event_queue = asyncio.Queue()
    rv = await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': '...', 'object': {}},
        event_queue=event_queue,
        # TODO: this is the target of the tests:
        #       consistency_time=None,              => it processes immediately
        #       consistency_time=in the past,       => it processes immediately (as if None)
        #       consistency_time=within the window, => it sleeps until the time, then processes
        #       consistency_time=after the window,  => it assumes the consistency, then processes
        #   ALSO:
        #       with/without change-detecting handlers.
        #       with/without event-watching handlers.
        #   ALSO:
        #       when awakened by a new event (stream pressure).
        consistency_time=None,
    )

    # TODO:
    #   And then, there will be separate splitting for:
    #       - watcher() -> processor() with proper/expected consistency_time,
    #       - processor() -> handlers() properly according to various consistency times.
    #   This leaks some abstractions of how consistency works to the tests, but can be tolerated
    #   due to complexity of units, with "consistency time" treated as a unit contract.
    #   In addition, the whole bundle can be tested:
    #       - watcher() -> handlers() -- i.e. a full simulation of the watch-stream.
