import functools
import sys

import click.testing
import kubernetes
import pytest

import kopf
from kopf.cli import main

SCRIPT1 = """
import kopf

@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(spec, **_):
    print('Hello from create_fn!')
    print(repr(spec))
"""

SCRIPT2 = """
import kopf

@kopf.on.update('zalando.org', 'v1', 'kopfexamples')
def update_fn(spec, **_):
    print('Hello from create_fn!')
    print(repr(spec))
"""


@pytest.fixture(autouse=True)
def srcdir(tmpdir):
    tmpdir.join('handler1.py').write(SCRIPT1)
    tmpdir.join('handler2.py').write(SCRIPT2)
    pkgdir = tmpdir.mkdir('package')
    pkgdir.join('__init__.py').write('')
    pkgdir.join('module_1.py').write(SCRIPT1)
    pkgdir.join('module_2.py').write(SCRIPT2)

    sys.path.insert(0, str(tmpdir))
    try:
        with tmpdir.as_cwd():
            yield tmpdir
    finally:
        sys.path.remove(str(tmpdir))


@pytest.fixture(autouse=True)
def clean_default_registry():
    registry = kopf.get_default_registry()
    kopf.set_default_registry(kopf.GlobalRegistry())
    try:
        yield
    finally:
        kopf.set_default_registry(registry)


@pytest.fixture(autouse=True)
def clean_modules_cache():
    # Otherwise, the first loaded test-modules remain there forever,
    # preventing 2nd and further tests from passing.
    for key in list(sys.modules.keys()):
        if key.startswith('package'):
            del sys.modules[key]


@pytest.fixture(autouse=True)
def clean_kubernetes_client():
    kubernetes.client.configuration.Configuration.set_default(None)



@pytest.fixture()
def runner():
    runner = click.testing.CliRunner()
    return runner


@pytest.fixture()
def invoke(runner):
    return functools.partial(runner.invoke, main)


@pytest.fixture()
def login(mocker):
    return mocker.patch('kopf.clients.auth.login')


@pytest.fixture()
def preload(mocker):
    return mocker.patch('kopf.utilities.loaders.preload')


@pytest.fixture()
def real_run(mocker):
    return mocker.patch('kopf.reactor.queueing.run')
