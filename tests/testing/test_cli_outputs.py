import signal
import textwrap
import time

import pytest

from kopf.testing import KopfCLI

# KopfCLI spawns a real child process via multiprocessing with 'spawn' start method.
# Each test also waits on subprocess sleeps so that assertions observe the state
# while the subprocess is alive. The repo-wide timeout (now: 2s) can be far too tight.
pytestmark = pytest.mark.timeout(10)

# Maximum overhead we tolerate on top of a declared timeout or zero-wait deadline.
CODE_OVERHEAD = 0.2


def test_empty_before_any_output(tmp_path):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        time.sleep(0.5)
        print("NEVER_SEEN")
        sys.exit(0)
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner:
        assert runner.buffer == b''
        assert runner.output == ''
    assert runner.buffer == b''
    assert runner.output == ''


def test_mixes_stdout_and_stderr(tmp_path):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys
        print("STDOUT")
        print("STDERR", file=sys.stderr)
        sys.exit(0)
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner:
        time.sleep(CODE_OVERHEAD)  # let it run a bit
    assert runner.buffer == b'STDOUT\nSTDERR\n'
    assert runner.output == 'STDOUT\nSTDERR\n'


def test_partial_line_during_run_is_in_buffer_but_withheld_from_output(tmp_path):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        print("COMPLETE")
        print("PARTIAL", end="")
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner:
        runner.wait_for(b'PARTIAL', timeout=5)  # sync on data arrival
        assert runner.buffer == b'COMPLETE\nPARTIAL'
        assert runner.output == 'COMPLETE\n'
    assert runner.buffer == b'COMPLETE\nPARTIAL'
    assert runner.output == 'COMPLETE\nPARTIAL'


def test_wait_for_bytes_matches_partial_lines(tmp_path, chronometer):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        print("PARTIAL", end="")
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for(b'PARTIAL', timeout=5)
    assert matched is True
    assert chronometer.seconds <= CODE_OVERHEAD


def test_wait_for_str_matches_whole_lines(tmp_path, chronometer):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        print("NEEDLE")
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for('NEEDLE', timeout=5)
    assert matched is True
    assert chronometer.seconds <= CODE_OVERHEAD


def test_wait_for_str_ignores_partial_lines_while_running(tmp_path, chronometer):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        print("PARTIAL", end="")
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for('PARTIAL', timeout=0.2)
    assert matched is False
    assert 0.2 <= chronometer.seconds <= 0.2 + CODE_OVERHEAD


def test_wait_for_regex_matches(tmp_path, chronometer):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        print("ready-42")
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for(r'ready-\d+', timeout=5)
    assert matched is True
    assert chronometer.seconds <= CODE_OVERHEAD


@pytest.mark.parametrize('pattern', [
    pytest.param([b'BYTES', 'STRING'], id='list'),
    pytest.param({b'BYTES', 'STRING'}, id='set'),
])
def test_wait_for_collection_matches_all(tmp_path, chronometer, pattern):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        print("BYTES")
        print("STRING")
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for(pattern, timeout=5)
    assert matched is True
    assert chronometer.seconds <= CODE_OVERHEAD


@pytest.mark.parametrize('pattern', [
    pytest.param(['COMPLETE', b'PARTIAL'], id='list'),
    pytest.param({'COMPLETE', b'PARTIAL'}, id='set'),
])
def test_wait_for_collection_mixes_output_and_buffer_scopes(tmp_path, chronometer, pattern):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        print("COMPLETE")
        print("PARTIAL", end="")
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for(pattern, timeout=5)
    assert matched is True
    assert chronometer.seconds <= CODE_OVERHEAD


def test_wait_for_collection_fails_if_any_pattern_missing(tmp_path, chronometer):
    # One pattern is present, the other never appears — all() requires both.
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        print("PRESENT")
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for(['PRESENT', 'ABSENT'], timeout=0.2)
    assert matched is False
    assert 0.2 <= chronometer.seconds <= 0.2 + CODE_OVERHEAD


def test_wait_for_collection_empty_matches_immediately(tmp_path, chronometer):
    # Vacuous truth: "wait until all of [] match" is trivially satisfied at call time.
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for([], timeout=5)
    assert matched is True
    assert chronometer.seconds <= CODE_OVERHEAD


def test_wait_for_callable_predicate(tmp_path, chronometer):
    # Three prints spaced by 0.3s. No strict lower bound: reader-thread scheduling
    # can batch all three writes into a single os.read and deliver them together,
    # in which case the predicate is already satisfied when the wait begins.
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        for _ in range(3):
            print("COMPLETE")
            time.sleep(0.3)
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for(lambda: runner.buffer.count(b'COMPLETE') >= 3, timeout=5)
    assert matched is True
    assert 0.6 <= chronometer.seconds <= 0.6 + CODE_OVERHEAD


def test_wait_for_unsupported_type_raises(tmp_path, chronometer):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys
        sys.exit(0)
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        with pytest.raises(ValueError, match='Unsupported pattern type'):
            runner.wait_for(object(), timeout=5)
    assert chronometer.seconds <= CODE_OVERHEAD


def test_wait_for_on_exit_without_match(tmp_path, chronometer):
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys
        print("DIFFERENT")
        sys.exit(0)
    """))
    with KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner, chronometer:
        matched = runner.wait_for('NEEDLE', timeout=5)
    assert matched is False
    assert chronometer.seconds <= 1.0 + CODE_OVERHEAD  # observed termination delay
