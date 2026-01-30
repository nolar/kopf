import asyncio
import dataclasses
import importlib
import inspect
import io
import json
import logging
import re
import sys
from unittest.mock import AsyncMock, Mock

import aiohttp.web
import pytest
import pytest_asyncio

import kopf
from kopf._cogs.clients.auth import APIContext
from kopf._cogs.configs.configuration import OperatorSettings
from kopf._cogs.structs.credentials import ConnectionInfo, Vault, VaultKey
from kopf._cogs.structs.references import Resource, Selector
from kopf._core.actions.loggers import ObjectPrefixingTextFormatter, configure
from kopf._core.engines.posting import settings_var
from kopf._core.intents.registries import OperatorRegistry
from kopf._core.reactor.inventory import ResourceMemories


def pytest_configure(config):
    config.addinivalue_line('markers', "e2e: end-to-end tests with real operators.")

    # Unexpected warnings should fail the tests. Use `-Wignore` to explicitly disable it.
    config.addinivalue_line('filterwarnings', 'error')

    # Warnings from the testing tools out of our control should not fail the tests.
    config.addinivalue_line('filterwarnings', 'ignore:The loop argument:DeprecationWarning:aiohttp')
    config.addinivalue_line('filterwarnings', 'ignore:The loop argument:DeprecationWarning:asyncio')
    config.addinivalue_line('filterwarnings', 'ignore:is deprecated, use current_thread:DeprecationWarning:threading')

    # TODO: Remove when fixed in https://github.com/pytest-dev/pytest-asyncio/issues/460:
    config.addinivalue_line('filterwarnings', 'ignore:There is no current event loop:DeprecationWarning:pytest_asyncio')

    # Aresponses uses the legacy function that is deprecated since 3.14 and will be removed in 3.16.
    config.addinivalue_line('filterwarnings', "ignore:'asyncio.iscoroutinefunction' is deprecated:DeprecationWarning:aresponses")

    # Python 3.12 transitional period:
    config.addinivalue_line('filterwarnings', 'ignore:datetime*:DeprecationWarning:dateutil')
    config.addinivalue_line('filterwarnings', 'ignore:datetime*:DeprecationWarning:freezegun')
    config.addinivalue_line('filterwarnings', 'ignore:.*:DeprecationWarning:_pydevd_.*')


def pytest_addoption(parser):
    parser.addoption("--only-e2e", action="store_true", help="Execute end-to-end tests only.")
    parser.addoption("--with-e2e", action="store_true", help="Include end-to-end tests.")


# This logic is not applied if pytest is started explicitly on ./examples/.
# In that case, regular pytest behaviour applies -- this is intended.
def pytest_collection_modifyitems(config, items):

    # Put all e2e tests to the end, as they are assumed to be slow.
    def _is_e2e(item):
        path = item.location[0]
        return path.startswith('tests/e2e/') or path.startswith('examples/')
    etc = [item for item in items if not _is_e2e(item)]
    e2e = [item for item in items if _is_e2e(item)]

    # Mark all e2e tests, no matter how they were detected. Just for filtering.
    mark_e2e = pytest.mark.e2e
    for item in e2e:
        item.add_marker(mark_e2e)

    # Minikube tests are heavy and require a cluster. Skip them by default,
    # so that the contributors can run pytest without initial tweaks.
    mark_skip = pytest.mark.skip(reason="E2E tests are not enabled. "
                                        "Use --with-e2e/--only-e2e to enable.")
    if not config.getoption('--with-e2e') and not config.getoption('--only-e2e'):
        for item in e2e:
            item.add_marker(mark_skip)

    # Minify the test-plan if only e2e are requested (all other should be skipped).
    if config.getoption('--only-e2e'):
        items[:] = e2e
    else:
        items[:] = etc + e2e


@pytest.fixture(params=[
    ('kopf.dev', 'v1', 'kopfpeerings', True),
    ('zalando.org', 'v1', 'kopfpeerings', True),
], ids=['kopf-dev-namespaced', 'zalando-org-namespaced'])
def namespaced_peering_resource(request):
    return Resource(*request.param[:3], namespaced=request.param[3])


@pytest.fixture(params=[
    ('kopf.dev', 'v1', 'clusterkopfpeerings', False),
    ('zalando.org', 'v1', 'clusterkopfpeerings', False),
], ids=['kopf-dev-cluster', 'zalando-org-cluster'])
def cluster_peering_resource(request):
    return Resource(*request.param[:3], namespaced=request.param[3])


@pytest.fixture(params=[
    ('kopf.dev', 'v1', 'clusterkopfpeerings', False),
    ('zalando.org', 'v1', 'clusterkopfpeerings', False),
    ('kopf.dev', 'v1', 'kopfpeerings', True),
    ('zalando.org', 'v1', 'kopfpeerings', True),
], ids=['kopf-dev-cluster', 'zalando-org-cluster', 'kopf-dev-namespaced', 'zalando-org-namespaced'])
def peering_resource(request):
    return Resource(*request.param[:3], namespaced=request.param[3])


@pytest.fixture()
def namespaced_resource():
    """ The resource used in the tests. Usually mocked, so it does not matter. """
    return Resource('kopf.dev', 'v1', 'kopfexamples', namespaced=True)


@pytest.fixture()
def cluster_resource():
    """ The resource used in the tests. Usually mocked, so it does not matter. """
    return Resource('kopf.dev', 'v1', 'kopfexamples', namespaced=False)


@pytest.fixture(params=[True, False], ids=['namespaced', 'cluster'])
def resource(request):
    """ The resource used in the tests. Usually mocked, so it does not matter. """
    return Resource('kopf.dev', 'v1', 'kopfexamples', namespaced=request.param)


@pytest.fixture()
def selector(resource):
    """ The selector used in the tests. Usually mocked, so it does not matter. """
    return Selector(group=resource.group, version=resource.version, plural=resource.plural)


@pytest.fixture()
def peering_namespace(peering_resource):
    return 'ns' if peering_resource.namespaced else None


@pytest.fixture()
def namespace(resource):
    return 'ns' if resource.namespaced else None


@pytest.fixture()
def settings():
    return OperatorSettings()


@pytest.fixture()
def memories():
    return ResourceMemories()


@pytest.fixture()
def logger():
    return logging.getLogger('fake-logger')


@pytest.fixture()
def settings_via_contextvar(settings):
    token = settings_var.set(settings)
    try:
        yield
    finally:
        settings_var.reset(token)


#
# Mocks for Kopf's internal but global variables.
#


@pytest.fixture
def registry_factory():
    # For most tests: not SmartOperatorRegistry, but the empty one!
    # For e2e tests: overridden to SmartOperatorRegistry.
    return OperatorRegistry


@pytest.fixture(autouse=True)
def registry(registry_factory):
    """
    Ensure that the tests have a fresh new global (not re-used) registry.
    """
    old_registry = kopf.get_default_registry()
    new_registry = registry_factory()
    kopf.set_default_registry(new_registry)
    yield new_registry
    kopf.set_default_registry(old_registry)


#
# Mocks for Kubernetes API clients (any of them). Reasons:
# 1. We do not test the clients, we test the layers on top of them,
#    so everything low-level should be mocked and assumed to be functional.
# 2. No external calls must be made under any circumstances.
#    The unit-tests must be fully isolated from the environment.
#


@dataclasses.dataclass(frozen=True, eq=False)
class K8sMocks:
    get: Mock
    post: Mock
    patch: Mock
    delete: Mock
    stream: Mock


@pytest.fixture()
def k8s_mocked(mocker, resp_mocker):
    # We mock on the level of our own K8s API wrappers, not the HTTP client.
    mocker.patch('kopf._cogs.clients.api.request', side_effect=Exception('Must not be called!'))

    async def itr(*_, **__):
        await asyncio.sleep(0)  # give up control
        if False:  # make it an iterator without yielding anything.
            yield

    return K8sMocks(
        get=mocker.patch('kopf._cogs.clients.api.get', return_value={}),
        post=mocker.patch('kopf._cogs.clients.api.post', return_value={}),
        patch=mocker.patch('kopf._cogs.clients.api.patch', return_value={}),
        delete=mocker.patch('kopf._cogs.clients.api.delete', return_value={}),
        stream=mocker.patch('kopf._cogs.clients.api.stream', side_effect=itr),
    )


@pytest.fixture()
async def enforced_context(fake_vault, mocker):
    """
    Patchable context/session for some tests, e.g. with local exceptions.

    The local exceptions are supposed to simulate either the code issues,
    or the connection issues. ``aresponses`` does not allow to raise arbitrary
    exceptions on the client side, but only to return the erroneous responses.

    This test forces the re-authenticating decorators to always use one specific
    session for the duration of the test, so that the patches would have effect.
    """
    _, item = fake_vault.select()
    context = APIContext(item.info)
    mocker.patch(f'{APIContext.__module__}.{APIContext.__name__}', return_value=context)
    async with context.session:
        yield context


@pytest.fixture()
async def enforced_session(enforced_context: APIContext):
    yield enforced_context.session


# Note: Unused `fake_vault` is to ensure that the client wrappers have the credentials.
# Note: Unused `enforced_session` is to ensure that the session is closed for every test.
@pytest.fixture()
def resp_mocker(fake_vault, enforced_session, aresponses):
    """
    A factory of server-side callbacks for ``aresponses`` with mocking/spying.

    The value of the fixture is a function, which returns a coroutine mock.
    That coroutine mock should be passed to ``aresponses.add()`` as a response
    callback function. When called, it calls the mock defined by the function's
    arguments (specifically, return_value or side_effects).

    The difference from passing the responses directly to ``aresponses.add()``
    is that it is possible to assert on whether the response was handled
    by that callback at all (i.e. HTTP URL & method matched), especially
    if there are multiple responses registered.

    Sample usage:

    .. code-block:: python

        import aiohttp.web

        def test_me(aresponses, resp_mocker):
            response = aiohttp.web.json_response({'a': 'b'})
            callback = resp_mocker(return_value=response)
            aresponses.add(hostname, '/path/', 'get', callback)
            do_something()
            assert callback.called
            assert callback.call_count == 1
    """
    def resp_maker(*args, **kwargs):
        actual_response = AsyncMock(*args, **kwargs)
        async def resp_mock_effect(request):
            nonlocal actual_response

            # The request's content can be read inside of the handler only. We preserve
            # the data into a conventional field, so that they could be asserted later.
            try:
                request.data = await request.json()
            except json.JSONDecodeError:
                request.data = await request.text()

            # Get a response/error as it was intended (via return_value/side_effect).
            response = actual_response()
            if inspect.isawaitable(response):
                response = await response
            elif inspect.iscoroutinefunction(response):
                response = await response
            return response

        return AsyncMock(side_effect=resp_mock_effect)
    return resp_maker


@pytest.fixture()
def version_api(resp_mocker, aresponses, hostname, resource):
    result = {'resources': [{
        'name': resource.plural,
        'namespaced': True,
    }]}
    version_url = resource.get_url().rsplit('/', 1)[0]  # except the plural name
    list_mock = resp_mocker(return_value=aiohttp.web.json_response(result))
    aresponses.add(hostname, version_url, 'get', list_mock)


@pytest.fixture()
def stream(fake_vault, resp_mocker, aresponses, hostname, resource, version_api):
    """ A mock for the stream of events as if returned by K8s client. """

    def feed(*args, namespace: str | None):
        for arg in args:

            # Prepare the stream response pre-rendered (for simplicity, no actual streaming).
            if isinstance(arg, (list, tuple)):
                stream_text = '\n'.join(json.dumps(event) for event in arg)
                stream_resp = aresponses.Response(text=stream_text)
            else:
                stream_resp = arg

            # List is requested for every watch, so we simulate it empty.
            list_data = {'items': [], 'metadata': {'resourceVersion': '0'}}
            list_resp = aiohttp.web.json_response(list_data)
            list_url = resource.get_url(namespace=namespace)

            # The stream is not empty, but is as fed.
            stream_query = {'watch': 'true', 'resourceVersion': '0'}
            stream_url = resource.get_url(namespace=namespace, params=stream_query)

            # Note: `aresponses` excludes a response once it is matched (side-effect-like).
            # So we just accumulate them there, as many as needed.
            aresponses.add(hostname, stream_url, 'get', stream_resp, match_querystring=True)
            aresponses.add(hostname, list_url, 'get', list_resp, match_querystring=True)

    # TODO: One day, find a better way to terminate a ``while-true`` reconnection cycle.
    def close(*, namespace: str | None):
        """
        A way to stop the stream from reconnecting: say it that the resource version is gone
        (we know a priori that it stops on this condition, and escalates to ``infinite_stream``).
        """
        feed([{'type': 'ERROR', 'object': {'code': 410}}], namespace=namespace)

    return Mock(spec_set=['feed', 'close'], feed=feed, close=close)


#
# Mocks for login & checks. Used in specifialised login tests,
# and in all CLI tests (since login is implicit with CLI commands).
#

@pytest.fixture()
def hostname():
    """ A fake hostname to be used in all aiohttp/aresponses tests. """
    return 'fake-host'


@dataclasses.dataclass(frozen=True, eq=False, order=False)
class LoginMocks:
    pykube_in_cluster: Mock = None
    pykube_from_file: Mock = None
    pykube_from_env: Mock = None
    client_in_cluster: Mock = None
    client_from_file: Mock = None


@pytest.fixture()
def login_mocks(mocker):
    """
    Make all client libraries potentially optional, but do not skip the tests:
    skipping the tests is the tests' decision, not this mocking fixture's one.
    """
    kwargs = {}
    try:
        import pykube
    except ImportError:
        pass
    else:
        cfg = pykube.KubeConfig({
            'current-context': 'self',
            'clusters': [{'name': 'self',
                          'cluster': {'server': 'localhost'}}],
            'contexts': [{'name': 'self',
                          'context': {'cluster': 'self', 'namespace': 'default'}}],
        })
        kwargs |= dict(
            pykube_in_cluster=mocker.patch.object(pykube.KubeConfig, 'from_service_account', return_value=cfg),
            pykube_from_file=mocker.patch.object(pykube.KubeConfig, 'from_file', return_value=cfg),
            pykube_from_env=mocker.patch.object(pykube.KubeConfig, 'from_env', return_value=cfg),
        )
    try:
        import kubernetes
    except ImportError:
        pass
    else:
        kwargs |= dict(
            client_in_cluster=mocker.patch.object(kubernetes.config, 'load_incluster_config'),
            client_from_file=mocker.patch.object(kubernetes.config, 'load_kube_config'),
        )
    return LoginMocks(**kwargs)


@pytest.fixture(autouse=True)
def clean_kubernetes_client():
    try:
        import kubernetes
    except ImportError:
        pass  # absent client is already "clean" (or not "dirty" at least).
    else:
        kubernetes.client.configuration.Configuration.set_default(None)


# Aresponses/aiohttp must be closed strictly after the vault. See the docstring.
@pytest.fixture()
async def _fake_vault(mocker, hostname, aresponses):
    """
    A hack around pytest's internal flaw in order to close the vault in the end.

    We cannot keep both the ContextVar and vault closing in the same fixture.
    Pytest runs every async setup and every async teardown in a separate task
    (a separate ``run_until_complete()``). The ``vault_var`` remains invisible
    to tests (with API calls) and even to the fixture's finalizing part.
    Sync (global) context vars do work and propagate fine — hence 2 fixtures.

    Without the proper vault finalization, the cached TCP sessions/connections
    remain open, so the aresponses/aiohttp test server takes time before exiting
    (15 seconds of keep-alive timeout by default).
    """
    key = VaultKey('fixture')
    info = ConnectionInfo(server=f'https://{hostname}')
    vault = Vault({key: info})
    mocker.patch.object(vault._guard, 'wait_for')
    try:
        yield vault
    finally:
        await vault.close()


@pytest.fixture()
def fake_vault(_fake_vault):
    """
    Provide a freshly created and populated authentication vault for every test.

    Most of the tests expect some credentials to be at least provided
    (even if not used). So, we create and set the vault as if every coroutine
    is invoked from the central :func:`operator` method (where it is set).

    Any blocking activities are mocked, so that the tests do not hang.
    """
    from kopf._cogs.clients import auth

    token = auth.vault_var.set(_fake_vault)
    try:
        yield _fake_vault
    finally:
        auth.vault_var.reset(token)

#
# Simulating that Kubernetes client libraries are not installed.
#

def _with_module_present(name: str):
    # If the module is required, it should either be installed,
    # or skip the test: we cannot simulate its presence (unlike its absence).
    yield pytest.importorskip(name)


def _with_module_absent(name: str):

    class ProhibitedImportFinder:
        def find_spec(self, fullname, path, target=None):
            if fullname.split('.')[0] == name:
                raise ImportError("Import is prohibited for tests.")

    try:
        mod_before = importlib.import_module(name)
    except ImportError:
        yield
        return  # nothing to patch & restore.

    # Remove any cached modules.
    preserved = {}
    for fullname, mod in list(sys.modules.items()):
        if fullname.split('.')[0] == name:
            preserved[fullname] = mod
            del sys.modules[fullname]

    # Inject the prohibition for loading this module. And restore when done.
    finder = ProhibitedImportFinder()
    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        sys.meta_path.remove(finder)
        sys.modules |= preserved

        # Verify if it works and that we didn't break the importing machinery.
        mod_after = importlib.import_module(name)
        assert mod_after is mod_before


@pytest.fixture(params=[True], ids=['with-client'])  # for hinting suffixes
def kubernetes():
    yield from _with_module_present('kubernetes')


@pytest.fixture(params=[False], ids=['no-client'])  # for hinting suffixes
def no_kubernetes():
    yield from _with_module_absent('kubernetes')


@pytest.fixture(params=[False, True], ids=['no-client', 'with-client'])
def any_kubernetes(request):
    if request.param:
        yield from _with_module_present('kubernetes')
    else:
        yield from _with_module_absent('kubernetes')


@pytest.fixture(params=[True], ids=['with-pykube'])  # for hinting suffixes
def pykube():
    yield from _with_module_present('pykube')


@pytest.fixture(params=[False], ids=['no-pykube'])  # for hinting suffixes
def no_pykube():
    yield from _with_module_absent('pykube')


@pytest.fixture(params=[False, True], ids=['no-pykube', 'with-pykube'])
def any_pykube(request):
    if request.param:
        yield from _with_module_present('pykube')
    else:
        yield from _with_module_absent('pykube')


@pytest.fixture()
def no_pyngrok():
    yield from _with_module_absent('pyngrok')


@pytest.fixture()
def no_oscrypto():
    yield from _with_module_absent('oscrypto')


@pytest.fixture()
def no_certbuilder():
    yield from _with_module_absent('certbuilder')


@pytest.fixture()
def no_certvalidator():
    yield from _with_module_absent('certvalidator')


#
# Helpers for the logging checks.
#


@pytest.fixture()
def logstream(caplog):
    """ Prefixing is done at the final output. We have to intercept it. """

    logger = logging.getLogger()
    handlers = list(logger.handlers)

    # Setup all log levels of sub-libraries. A sife-effect: the handlers are also added.
    configure(verbose=True)

    # Remove any stream handlers added in the step above. But keep the caplog's handlers.
    for handler in list(logger.handlers):
        if isinstance(handler, logging.StreamHandler) and handler.stream is sys.stderr:
            logger.removeHandler(handler)

    # Inject our stream-intercepting handler.
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    formatter = ObjectPrefixingTextFormatter('prefix %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    try:
        with caplog.at_level(logging.DEBUG):
            yield stream
    finally:
        logger.removeHandler(handler)
        logger.handlers[:] = handlers  # undo `configure()`


@pytest.fixture()
def assert_logs(caplog):
    """
    A function to assert the logs are present (by pattern).

    The listed message patterns MUST be present, in the order specified.
    Some other log messages can also be present, but they are ignored.
    """
    caplog.set_level(logging.DEBUG)

    def assert_logs_fn(patterns=[], prohibited=[], strict=False):
        __traceback_hide__ = True
        remaining_patterns = list(patterns)
        for message in caplog.messages:
            # The expected pattern is at position 0.
            # Looking-ahead: if one of the following patterns matches, while the
            # 0th does not, then the log message is missing, and we fail the test.
            for idx, pattern in enumerate(remaining_patterns):
                m = re.search(pattern, message)
                if m:
                    if idx == 0:
                        remaining_patterns[:1] = []
                        break  # out of `remaining_patterns` cycle
                    else:
                        skipped_patterns = remaining_patterns[:idx]
                        raise AssertionError(f"Several patterns were skipped: {skipped_patterns!r}")
                elif strict:
                    raise AssertionError(f"Unexpected log message: {message!r}")

            # Check that the prohibited patterns do not appear in any message.
            for pattern in prohibited:
                m = re.search(pattern, message)
                if m:
                    raise AssertionError(f"Prohibited log pattern found: {message!r} ~ {pattern!r}")

        # If all patterns have been matched in order, we are done.
        # if some are left, but the messages are over, then we fail.
        if remaining_patterns:
            raise AssertionError(f"Several patterns were missed: {remaining_patterns!r}")

    return assert_logs_fn


#
# Helpers for asyncio checks.
#
@pytest_asyncio.fixture(autouse=True)
def _no_asyncio_pending_tasks(request: pytest.FixtureRequest):
    """
    Ensure there are no unattended asyncio tasks after the test.

    It looks  both in the test's main event-loop, and in all other event-loops,
    such as the background thread of :class:`KopfRunner` (used in e2e tests).

    Current solution uses some internals of asyncio, since there is no public
    interface for that. The warnings are printed only at the end of pytest.

    An alternative way: set the event-loop's exception handler, force garbage
    collection on every test, and check messages from ``asyncio.Task.__del__``.
    This, however, requires intercepting all event-loop creation in the code.
    """
    before = _get_all_tasks()

    # Run the test.
    yield

    # Let the pytest-asyncio's async2sync wrapper to finish all callbacks. Otherwise, it raises:
    #   <Task pending name='Task-2' coro=<<async_generator_athrow without __name__>()>>
    # We don't know which loops were used in the test & fixtures, so we wait on all of them.
    for fixture_name, fixture_value in request.node.funcargs.items():
        if isinstance(fixture_value, asyncio.BaseEventLoop):
            fixture_value.run_until_complete(asyncio.sleep(0))

        # Safe-guards for Python 3.10 until deprecated in ≈Oct'2026 (not needed for 3.11+).
        try:
            from asyncio import Runner as stdlib_Runner  # python >= 3.11 (absent in 3.10)
        except ImportError:
            pass
        else:
            if isinstance(fixture_value, stdlib_Runner):
                fixture_value.get_loop().run_until_complete(asyncio.sleep(0))

        # In case pytest's asyncio libraries use the backported runners in Python 3.10.
        try:
            from backports.asyncio.runner import Runner as backported_Runner
        except ImportError:
            pass
        else:
            if isinstance(fixture_value, backported_Runner):
                fixture_value.get_loop().run_until_complete(asyncio.sleep(0))

    # Detect all leftover tasks.
    after = _get_all_tasks()
    remains = after - before
    if remains:
        pytest.fail(f"Unattended asyncio tasks detected: {remains!r}")


def _get_all_tasks() -> set[asyncio.Task]:
    """Similar to :func:`asyncio.all_tasks`, but for all event loops at once."""
    i = 0
    while True:
        try:
            if sys.version_info >= (3, 12):
                tasks = asyncio.tasks._eager_tasks | set(asyncio.tasks._scheduled_tasks)
            else:
                tasks = list(asyncio.tasks._all_tasks)
        except RuntimeError:
            i += 1
            if i >= 1000:
                raise  # we are truly unlucky today; try again tomorrow
        else:
            break
    return {t for t in tasks if not t.done()}


@pytest.fixture()
def loop():
    """Sync aiohttp's server-side timeline with kopf's client-side timeline."""
    return asyncio.get_running_loop()
