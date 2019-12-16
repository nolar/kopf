import asynctest
from kopf.clients.watching import _iter_lines


async def test_empty_content():
    async def iter_chunks():
        if False:  # to make this function a generator
            yield b''

    content = asynctest.Mock(iter_chunks=iter_chunks)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == []


async def test_empty_chunk():
    async def iter_chunks():
        yield (b'', False)

    content = asynctest.Mock(iter_chunks=iter_chunks)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'']


async def test_one_chunk_one_line():
    async def iter_chunks():
        yield (b'hello', False)

    content = asynctest.Mock(iter_chunks=iter_chunks)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'hello']


async def test_one_chunk_two_lines():
    async def iter_chunks():
        yield (b'hello\nworld', False)

    content = asynctest.Mock(iter_chunks=iter_chunks)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'hello', b'world']


async def test_one_chunk_empty_lines():
    async def iter_chunks():
        yield (b'\nhello\nworld\n', False)

    content = asynctest.Mock(iter_chunks=iter_chunks)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'', b'hello', b'world', b'']


async def test_few_chunks_split():
    async def iter_chunks():
        yield (b'\nhel', False)
        yield (b'lo\nwo', False)
        yield (b'rld\n', False)

    content = asynctest.Mock(iter_chunks=iter_chunks)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'', b'hello', b'world', b'']
