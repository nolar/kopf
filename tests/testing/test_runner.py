import textwrap

import pytest

import kopf
from kopf._cogs.configs.configuration import OperatorSettings
from kopf._core.intents.registries import OperatorRegistry
from kopf.testing import KopfRunner


@pytest.fixture(autouse=True)
def no_config_needed(login_mocks):
    pass


def test_command_invocation_works():
    with KopfRunner(['--help']) as runner:
        pass
    assert runner.exc_info
    assert runner.exc_info[0] is SystemExit
    assert runner.exc_info[1].code == 0
    assert runner.exit_code == 0
    assert runner.exception is None
    assert runner.output.startswith("Usage:")
    assert runner.stdout.startswith("Usage:")
    assert runner.stdout_bytes.startswith(b"Usage:")
    # Note: stderr is not captured, it is mixed with stdout.


def test_registry_and_settings_are_propagated(mocker):
    operator_mock = mocker.patch('kopf._core.reactor.running.operator')
    registry = OperatorRegistry()
    settings = OperatorSettings()
    with KopfRunner(['run', '--standalone'], registry=registry, settings=settings) as runner:
        pass
    assert runner.exit_code == 0
    assert runner.exception is None
    assert operator_mock.called
    assert operator_mock.call_args.kwargs['registry'] is registry
    assert operator_mock.call_args.kwargs['settings'] is settings


def test_runner_is_isolated_from_caller(registry, mocker, tmp_path):
    operator_mock = mocker.patch('kopf._core.reactor.running.operator')

    # Enforce known ids instead of the default closures and nested functions.
    @kopf.on.startup(registry=registry, id='caller_handler')
    def caller_handler(**_):
        pass

    handler_file = tmp_path / "handlers.py"
    handler_file.write_text(textwrap.dedent("""
        import kopf

        @kopf.on.startup(id='file_handler')
        def file_handler(**_):
            pass
    """))

    with KopfRunner(['run', str(handler_file), '--standalone']) as runner:
        pass
    assert runner.exit_code == 0
    assert runner.exception is None

    # The operator must be called with a fresh registry, not the caller's.
    used_registry = operator_mock.call_args.kwargs['registry']
    assert used_registry is not registry

    # The file's handler must be in the operator's registry.
    caller_handler_ids = [h.id for h in registry._activities.get_all_handlers()]
    runner_handler_ids = [h.id for h in used_registry._activities.get_all_handlers()]
    assert 'caller_handler' not in runner_handler_ids
    assert 'file_handler' in runner_handler_ids
    assert 'file_handler' not in caller_handler_ids
    assert 'caller_handler' in caller_handler_ids


def test_runner_is_isolated_from_sibling_runners(mocker, tmp_path):
    operator_mock = mocker.patch('kopf._core.reactor.running.operator')

    handler_file_a = tmp_path / "handlers_a.py"
    handler_file_a.write_text(textwrap.dedent("""
        import kopf

        @kopf.on.startup(id='handler_a')
        def handler_a(**_):
            pass
    """))

    handler_file_b = tmp_path / "handlers_b.py"
    handler_file_b.write_text(textwrap.dedent("""
        import kopf

        @kopf.on.startup(id='handler_b')
        def handler_b(**_):
            pass
    """))

    with KopfRunner(['run', str(handler_file_a), '--standalone']) as runner_a:
        pass
    assert runner_a.exit_code == 0
    assert runner_a.exception is None

    with KopfRunner(['run', str(handler_file_b), '--standalone']) as runner_b:
        pass
    assert runner_b.exit_code == 0
    assert runner_b.exception is None

    # Each runner must use its own fresh registry.
    used_registry_a = operator_mock.call_args_list[0].kwargs['registry']
    used_registry_b = operator_mock.call_args_list[1].kwargs['registry']
    assert used_registry_a is not used_registry_b

    # The first runner's handler must NOT be in the second runner's registry.
    handler_ids_a = [h.id for h in used_registry_a._activities.get_all_handlers()]
    handler_ids_b = [h.id for h in used_registry_b._activities.get_all_handlers()]
    assert 'handler_a' in handler_ids_a
    assert 'handler_b' not in handler_ids_a
    assert 'handler_b' in handler_ids_b
    assert 'handler_a' not in handler_ids_b


def test_exception_from_runner_suppressed_with_no_reraise():
    with KopfRunner(['run', 'non-existent.py', '--standalone'], reraise=False) as runner:
        pass
    assert runner.exception is not None
    assert isinstance(runner.exception, FileNotFoundError)


def test_exception_from_runner_escalates_with_reraise():
    with pytest.raises(FileNotFoundError):
        with KopfRunner(['run', 'non-existent.py', '--standalone'], reraise=True):
            pass


def test_exception_from_runner_escalates_by_default():
    with pytest.raises(FileNotFoundError):
        with KopfRunner(['run', 'non-existent.py', '--standalone']):
            pass


@pytest.mark.parametrize('kwargs',[
    dict(reraise=False),
    dict(reraise=True),
    dict(),
], ids=['reraise=False', 'reraise=True', 'no-reraise'])
def test_exception_from_invoke_escalates(mocker, kwargs):
    class SampleError(Exception): pass
    mocker.patch('click.testing.CliRunner.invoke', side_effect=SampleError)

    with pytest.raises(SampleError):
        with KopfRunner(['run', 'non-existent.py', '--standalone'], **kwargs):
            pass


def test_wrong_command_fails():
    with pytest.raises(SystemExit) as e:
        with KopfRunner(['unexistent-command']):
            pass
    assert e.value.code == 2


def test_absent_file_fails():
    with pytest.raises(FileNotFoundError):
        with KopfRunner(['run', 'non-existent.py', '--standalone']):
            pass


def test_bad_syntax_file_fails(tmpdir):
    path = tmpdir.join('handlers.py')
    path.write("""This is a Python syntax error!""")
    with pytest.raises((IndentationError, SyntaxError)):
        with KopfRunner(['run', str(path), '--standalone']):
            pass
