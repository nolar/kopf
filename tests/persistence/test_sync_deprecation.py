import warnings

import pytest

from kopf._cogs.configs.diffbase import DiffBaseStorage
from kopf._cogs.configs.progress import ProgressStorage


def test_sync_progress_methods_are_deprecated():
    with pytest.warns(FutureWarning) as record:
        class SyncProgressStorage(ProgressStorage):
            def fetch(self, **_): ...
            def store(self, **_): ...
            def purge(self, **_): ...
            def flush(self): ...

    messages = [str(w.message) for w in record]
    assert len(messages) == 4
    assert any("SyncProgressStorage.fetch() is not async;" in m for m in messages)
    assert any("SyncProgressStorage.store() is not async;" in m for m in messages)
    assert any("SyncProgressStorage.purge() is not async;" in m for m in messages)
    assert any("SyncProgressStorage.flush() is not async;" in m for m in messages)


def test_sync_diffbase_methods_are_deprecated():
    with pytest.warns(FutureWarning) as record:
        class SyncDiffBaseStorage(DiffBaseStorage):
            def fetch(self, **_): ...
            def store(self, **_): ...

    messages = [str(w.message) for w in record]
    assert len(messages) == 2
    assert any("SyncDiffBaseStorage.fetch() is not async;" in m for m in messages)
    assert any("SyncDiffBaseStorage.store() is not async;" in m for m in messages)


def test_async_progress_methods_are_not_deprecated():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        class AsyncProgressStorage(ProgressStorage):
            async def fetch(self, **_): ...
            async def store(self, **_): ...
            async def purge(self, **_): ...
            async def flush(self): ...


def test_async_diffbase_methods_are_not_deprecated():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        class AsyncDiffBaseStorage(DiffBaseStorage):
            async def fetch(self, **_): ...
            async def store(self, **_): ...


def test_inherited_progress_methods_are_not_deprecated():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        class DerivedProgressStorage(ProgressStorage):
            pass


def test_inherited_diffbase_methods_are_not_deprecated():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        class DerivedDiffBaseStorage(DiffBaseStorage):
            pass


def test_kopf_module_progress_storages_are_not_deprecated():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        type('KopfProgressStorage', (ProgressStorage,), {
            '__module__': 'kopf._cogs.configs.custom',
            'fetch': lambda self, **_: None,
            'store': lambda self, **_: None,
            'purge': lambda self, **_: None,
            'flush': lambda self: None,
        })


def test_kopf_module_diffbase_storages_are_not_deprecated():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        type('KopfDiffBaseStorage', (DiffBaseStorage,), {
            '__module__': 'kopf._cogs.configs.custom',
            'fetch': lambda self, **_: None,
            'store': lambda self, **_: None,
        })
