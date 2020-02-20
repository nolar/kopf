import gc
import os.path

from kopf.clients.auth import _TempFiles


def test_created():
    tempfiles = _TempFiles()
    path = tempfiles[b'hello']
    assert os.path.isfile(path)
    with open(path, 'rb') as f:
        assert f.read() == b'hello'


def test_reused():
    tempfiles = _TempFiles()
    path1 = tempfiles[b'hello']
    path2 = tempfiles[b'hello']
    assert path1 == path2


def test_differs():
    tempfiles = _TempFiles()
    path1 = tempfiles[b'hello']
    path2 = tempfiles[b'world']
    assert path1 != path2


def test_purged():
    tempfiles = _TempFiles()
    path1 = tempfiles[b'hello']
    path2 = tempfiles[b'world']
    assert os.path.isfile(path1)
    assert os.path.isfile(path2)

    tempfiles.purge()

    assert not os.path.isfile(path1)
    assert not os.path.isfile(path2)


def test_garbage_collected():
    tempfiles = _TempFiles()
    path1 = tempfiles[b'hello']
    path2 = tempfiles[b'world']
    assert os.path.isfile(path1)
    assert os.path.isfile(path2)

    del tempfiles
    gc.collect()
    gc.collect()
    gc.collect()

    assert not os.path.isfile(path1)
    assert not os.path.isfile(path2)
