import ast
import collections
import dataclasses
import re
import subprocess
import time
from collections.abc import Sequence

import astpath
import pytest
from lxml import etree

from kopf.testing import KopfRunner


def test_all_examples_are_runnable(settings, with_crd, exampledir, caplog):

    # If the example has its own opinion on the timing, try to respect it.
    # See e.g. /examples/99-all-at-once/example.py.
    example_py = exampledir / 'example.py'
    parser = E2EParser(str(example_py))
    config = CONFIGS.get(str(example_py), E2EConfig())

    # Skip the e2e test if the framework-optional but test-required library is missing.
    if parser.imports_kubernetes:
        pytest.importorskip('kubernetes')

    # To prevent lengthy sleeps on the simulated retries.
    settings.execution.default_backoff = 1

    # To prevent lengthy threads in the loop executor when the process exits.
    settings.watching.server_timeout = 10

    # Run an operator and simulate some activity with the operated resource.
    with KopfRunner(
        ['run', '--all-namespaces', '--standalone', '--verbose', str(example_py)],
        timeout=60, settings=settings,
    ) as runner:

        # Give it some time to start.
        _sleep_till_stopword(caplog=caplog,
                             delay=config.startup_time_limit,
                             patterns=config.startup_stop_words or ['Client is configured'])

        # Trigger the reaction. Give it some time to react and to sleep and to retry.
        subprocess.run("kubectl apply -f examples/obj.yaml",
                       shell=True, check=True, timeout=10, capture_output=True)
        _sleep_till_stopword(caplog=caplog,
                             delay=config.creation_time_limit,
                             patterns=config.creation_stop_words)

        # Trigger the reaction. Give it some time to react.
        subprocess.run("kubectl delete -f examples/obj.yaml",
                       shell=True, check=True, timeout=10, capture_output=True)
        _sleep_till_stopword(caplog=caplog,
                             delay=config.deletion_time_limit,
                             patterns=config.deletion_stop_words)

    # Give it some time to finish.
    _sleep_till_stopword(caplog=caplog,
                         delay=config.cleanup_time_limit,
                         patterns=config.cleanup_stop_words or ['Hung tasks', 'Root tasks'])

    # Verify that the operator did not die on start, or during the operation.
    assert runner.exception is None
    assert runner.exit_code == 0

    # There are usually more than these messages, but we only check for the certain ones.
    # This just shows us that the operator is doing something, it is alive.
    if parser.has_mandatory_on_delete:
        assert '[default/kopf-example-1] Adding the finalizer' in runner.output
    if parser.has_on_create:
        assert '[default/kopf-example-1] Creation is in progress:' in runner.output
    if parser.has_mandatory_on_delete:
        assert '[default/kopf-example-1] Deletion is in progress:' in runner.output
    if parser.has_changing_handlers:
        assert '[default/kopf-example-1] Deleted, really deleted' in runner.output
    if not config.allow_tracebacks:
        assert 'Traceback (most recent call last):' not in runner.output

    # Verify that once a handler succeeds, it is never re-executed again.
    handler_names = re.findall(r"'(.+?)' succeeded", runner.output)
    if config.success_counts is not None:
        checked_names = [name for name in handler_names if name in config.success_counts]
        name_counts = collections.Counter(checked_names)
        assert name_counts == config.success_counts
    else:
        name_counts = collections.Counter(handler_names)
        assert set(name_counts.values()) == {1}

    # Verify that once a handler fails, it is never re-executed again.
    handler_names = re.findall(r"'(.+?)' failed (?:permanently|with an exception and will stop)", runner.output)
    if config.failure_counts is not None:
        checked_names = [name for name in handler_names if name in config.failure_counts]
        name_counts = collections.Counter(checked_names)
        assert name_counts == config.failure_counts
    else:
        name_counts = collections.Counter(handler_names)
        assert not name_counts


def _sleep_till_stopword(
        caplog,
        delay: float | None = None,
        patterns: Sequence[str] | None = None,
        *,
        interval: float | None = None,
) -> bool:
    patterns = list(patterns or [])
    delay = delay or (10.0 if patterns else 3.0)
    interval = interval or min(1.0, max(0.1, delay / 10.))
    started = time.perf_counter()
    found = False
    while not found and time.perf_counter() - started < delay:
        for message in list(caplog.messages):
            if any(re.search(pattern, message) for pattern in patterns or []):
                found = True
                break
        else:
            time.sleep(interval)
    return found


class E2EParser:
    """
    An AST-based parser of examples' codebase.

    The parser retrieves the information about the example without executing
    the whole example (which can have side-effects). Some snippets are still
    executed: e.g. values of some decorators' kwargs.
    """
    xml2ast: dict[etree._Element, ast.AST]
    xtree: etree._Element

    def __init__(self, path: str) -> None:
        super().__init__()

        with open(path, encoding='utf-8') as f:
            self.path = path
            self.text = f.read()

        self.xml2ast = {}
        self.xtree = astpath.file_contents_to_xml_ast(self.text, node_mappings=self.xml2ast)

    @property
    def imports_kubernetes(self) -> bool:
        # In English: all forms of `import kubernetes[.blah]`, `import kubernetes[.blah] as x`,
        # so as `from kubernetes[.blah] import x as y`.
        return bool(self.xtree.xpath('''
            //Import/names/alias[@name="kubernetes" or starts-with(@name, "kubernetes.")] |
            //ImportFrom[@module="kubernetes" or starts-with(@module, "kubernetes.")]
        '''))

    def has_handler(self, name: str) -> bool:
        # In English: any decorators that look like `@kopf.on.{name}(...)` or `@kopf.{name}(...)`.
        return bool(self.xtree.xpath(f'''
            (//FunctionDef | //AsyncFunctionDef)/decorator_list/Call[
                (
                    func/Attribute/value/Attribute/value/Name/@id="kopf" and
                    func/Attribute/value/Attribute/@attr="on" and
                    func/Attribute/@attr={name!r}
                ) or (
                    func/Attribute/value/Name/@id="kopf" and
                    func/Attribute/@attr={name!r}
                )
            ]
        '''))

    @property
    def has_on_create(self) -> bool:
        return self.has_handler('create')

    @property
    def has_on_update(self) -> bool:
        return self.has_handler('update')

    @property
    def has_on_delete(self) -> bool:
        return self.has_handler('delete')

    @property
    def has_changing_handlers(self) -> bool:
        return any(self.has_handler(name) for name in ['create', 'update', 'delete'])

    @property
    def has_mandatory_on_delete(self) -> bool:
        # In English: `optional=...` kwargs of `@kopf.on.delete(...)` decorators, if any.
        optional_kwargs = self.xtree.xpath('''
            (//FunctionDef | //AsyncFunctionDef)/decorator_list/Call[
                func/Attribute/value/Attribute/value/Name/@id="kopf" and
                func/Attribute/value/Attribute/@attr="on" and
                func/Attribute/@attr="delete"
            ]/keywords/keyword[@arg="optional"]
        ''')
        return (self.has_on_delete and
                not any(ast.literal_eval(self.xml2ast[kwarg].value) for kwarg in optional_kwargs))


@dataclasses.dataclass(kw_only=True)
class E2EConfig:
    allow_tracebacks: bool = False
    startup_time_limit: float | None = None
    cleanup_time_limit: float | None = None
    creation_time_limit: float | None = None
    deletion_time_limit: float | None = None
    startup_stop_words: list[str] | None = None
    cleanup_stop_words: list[str] | None = None
    creation_stop_words: list[str] | None = None
    deletion_stop_words: list[str] | None = None
    success_counts: dict[str, int] | None = None
    failure_counts: dict[str, int] | None = None


CONFIGS: dict[str, E2EConfig] = {
    'examples/03-exceptions/example.py': E2EConfig(
        allow_tracebacks=True,
        creation_stop_words=['Something has changed,'],
        success_counts={'eventual_success_with_a_few_messages': 1},
        failure_counts={'eventual_failure_with_tracebacks': 1,
                        'instant_failure_with_traceback': 1,
                        'instant_failure_with_only_a_message': 1},
    ),
    'examples/08-events/example.py': E2EConfig(
        allow_tracebacks=True,
        success_counts={'normal_event_fn': 2},
    ),
    'examples/13-hooks/example.py': E2EConfig(
        startup_stop_words=['Served by the background task.'],
        cleanup_stop_words=['Hung tasks', 'Root tasks'],
        success_counts={'startup_fn_simple': 1, 'startup_fn_retried': 1, 'cleanup_fn': 1},
    ),
    'examples/14-daemons/example.py': E2EConfig(
        creation_stop_words=["=> Ping from"],
        deletion_stop_words=["'background_async' is cancelled",
                             "'background_sync' is cancelled",
                             "'background_async' has exited"],
    ),
    'examples/16-indexing/example.py': E2EConfig(
        success_counts={},  # we do not care: pods can have 6-10 updates
    ),
    'examples/17-admission/example.py': E2EConfig(
        success_counts={},  # we do not care: pods can have 6-10 updates
    ),
    'examples/99-all-at-once/example.py': E2EConfig(
        allow_tracebacks=True,
        startup_stop_words=['Served by the background task.'],
        cleanup_stop_words=['Hung tasks', 'Root tasks'],
        creation_stop_words=['Creation is processed:'],
        deletion_stop_words=['Deleted, really deleted'],
        success_counts = {'create_1': 1, 'create_2': 1, 'create_pod': 1, 'delete': 1,
                          'startup_fn_simple': 1, 'startup_fn_retried': 1, 'cleanup_fn': 1},
    ),
}
