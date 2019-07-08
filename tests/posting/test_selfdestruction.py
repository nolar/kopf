import asyncio
import gc
import logging
import weakref

from kopf.engines.logging import ObjectLogger

OBJ1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}}


def test_garbage_collection_of_log_handlers():

    event_queue = asyncio.Queue()
    native_logger = logging.getLogger(f'kopf.objects.{id(event_queue)}')
    assert len(native_logger.handlers) == 0

    object_logger = ObjectLogger(body=OBJ1, event_queue=event_queue)
    assert object_logger.logger is native_logger
    assert len(native_logger.handlers) == 1

    object_logger_ref = weakref.ref(object_logger)
    del object_logger
    gc.collect()  # triggers ObjectLogger.__del__()

    assert object_logger_ref() is None  # garbage-collected indeed.
    assert len(native_logger.handlers) == 0
