import pytest

from kopf.testing import KopfRunner


@pytest.fixture(autouse=True)
def no_config_needed(mocker):
    mocker.patch('kubernetes.config.load_incluster_config')
    mocker.patch('kubernetes.config.load_kube_config')
    mocker.patch('kubernetes.client.CoreApi')  # for login self-check


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
