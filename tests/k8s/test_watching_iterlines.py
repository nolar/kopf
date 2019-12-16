import asynctest
from kopf.clients.watching import _iter_lines


async def test_empty_content():
    async def iter_chunked(n: int):
        if False:  # to make this function a generator
            yield b''

    content = asynctest.Mock(iter_chunked=iter_chunked)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == []


async def test_empty_chunk():
    async def iter_chunked(n: int):
        yield b''

    content = asynctest.Mock(iter_chunked=iter_chunked)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'']


async def test_one_chunk_one_line():
    async def iter_chunked(n: int):
        yield b'hello'

    content = asynctest.Mock(iter_chunked=iter_chunked)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'hello']


async def test_one_chunk_two_lines():
    async def iter_chunked(n: int):
        yield b'hello\nworld'

    content = asynctest.Mock(iter_chunked=iter_chunked)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'hello', b'world']


async def test_one_chunk_empty_lines():
    async def iter_chunked(n: int):
        yield b'\nhello\nworld\n'

    content = asynctest.Mock(iter_chunked=iter_chunked)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'', b'hello', b'world', b'']


async def test_few_chunks_split():
    async def iter_chunked(n: int):
        yield b'\nhel'
        yield b'lo\nwo'
        yield b'rld\n'

    content = asynctest.Mock(iter_chunked=iter_chunked)
    lines = []
    async for line in _iter_lines(content):
        lines.append(line)

    assert lines == [b'', b'hello', b'world', b'']
