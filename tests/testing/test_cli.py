import signal
import textwrap
import time

import pytest

from kopf.testing import KopfCLI

# KopfCLI spawns a real child process via multiprocessing with 'spawn' start method.
# Process startup is slow compared to threads, so we need generous timeouts.
pytestmark = pytest.mark.timeout(10)

# A typical overhead for code execution.
CODE_OVERHEAD = 0.2

# Minimum allowance for subprocess startup under spawn + module imports.
# Any chronometer upper bound must include at least this much slack.
SPAWN_OVERHEAD = 2


def test_help_command():
    with KopfCLI(['--help']) as runner:
        pass
    assert runner.exit_code == 0
    assert 'Usage:' in runner.output


def test_run_subcommand_help():
    with KopfCLI(['run', '--help']) as runner:
        pass
    assert runner.exit_code == 0
    assert 'Usage:' in runner.output


def test_string_args_parsed():
    with KopfCLI('--help') as runner:
        pass
    assert runner.exit_code == 0
    assert 'Usage:' in runner.output


def test_bad_command_raises_with_reraise():
    with pytest.raises(RuntimeError, match="exited with code"):
        with KopfCLI(['nonexistent-command']):
            pass


def test_bad_command_suppressed_without_reraise():
    with KopfCLI(['nonexistent-command'], reraise=False) as runner:
        pass
    assert runner.exit_code == 2
    assert 'No such command' in runner.output


def test_reraise_chains_onto_block_exception():
    with pytest.raises(RuntimeError, match="exited with code") as exc_info:
        with KopfCLI(['nonexistent-command']):
            raise ValueError("block error")
    assert exc_info.value.__cause__ is not None
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_block_exception_propagates_on_normal_exit():
    with pytest.raises(ValueError, match="block error"):
        with KopfCLI(['--help']):
            raise ValueError("block error")


def test_bad_syntax_file_fails(tmp_path):
    path = tmp_path / 'handlers.py'
    path.write_text("""This is a Python syntax error!""")
    with KopfCLI(['run', str(path)], reraise=False) as runner:
        pass
    assert runner.exit_code == 1
    assert 'SyntaxError' in runner.output


def test_timeout_forces_kill_and_raises(tmp_path, chronometer):
    path = tmp_path / 'handlers.py'
    path.write_text("import time; time.sleep(30)")
    with pytest.raises(RuntimeError, match="did not exit gracefully"):
        with chronometer, KopfCLI(['run', str(path)], timeout=0.5) as runner:
            pass
    assert runner.exit_code == -signal.SIGKILL
    assert 0.5 <= chronometer.seconds <= 0.5 + SPAWN_OVERHEAD  # but not 30


def test_timeout_forces_kill_without_reraise(tmp_path, chronometer):
    path = tmp_path / 'handlers.py'
    path.write_text("import time; time.sleep(30)")
    with chronometer, KopfCLI(['run', str(path)], timeout=0.5, reraise=False) as runner:
        pass
    assert runner.exit_code == -signal.SIGKILL
    assert 0.5 <= chronometer.seconds <= 0.5 + SPAWN_OVERHEAD  # but not 30


def test_spawn_timeout_raises_on_slow_spawn(chronometer):
    # `spawn_timeout` shorter than any real Python spawn: the child has no chance
    # of signalling readiness in time, so __enter__ must clean up and raise.
    with pytest.raises(RuntimeError, match="did not finish spawning"):
        with chronometer, KopfCLI(['--help'], spawn_timeout=0.000001):
            pass
    assert chronometer.seconds <= CODE_OVERHEAD


def test_signal_sigterm_graceful_exit(tmp_path, chronometer):
    # Handler installs a SIGTERM handler that exits cleanly with code 0.
    # Verifies the production shutdown path (SIGTERM → clean exit).
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import signal, sys, time
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with chronometer, KopfCLI(['run', str(path)], signal=signal.SIGTERM) as runner:
        time.sleep(CODE_OVERHEAD)  # let it install the handlers
    assert runner.exit_code == 0
    assert chronometer.seconds <= SPAWN_OVERHEAD


def test_signal_sigterm_uncaught_is_normal(tmp_path, chronometer):
    # No handler: SIGTERM's default action terminates the process with -SIGTERM.
    # The runner must classify that as normal because we sent the signal ourselves.
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import sys, time
        time.sleep(9)
        sys.exit(123)  # never happens
    """))
    with chronometer, KopfCLI(['run', str(path)], signal=signal.SIGTERM) as runner:
        time.sleep(CODE_OVERHEAD)  # let it reach the inner sleep
    assert runner.exit_code == -signal.SIGTERM
    assert chronometer.seconds <= SPAWN_OVERHEAD


def test_signal_sigkill_is_normal(tmp_path, chronometer):
    # SIGKILL cannot be caught, and the process dies with -SIGKILL (-9).
    # The runner must classify that as normal because signal=SIGKILL was requested.
    path = tmp_path / 'handlers.py'
    path.write_text("import time; time.sleep(30)")
    with chronometer, KopfCLI(['run', str(path)], signal=signal.SIGKILL) as runner:
        pass
    assert runner.exit_code == -signal.SIGKILL
    assert chronometer.seconds <= SPAWN_OVERHEAD  # not 30


def test_signal_ignored_then_timeout_force_kills(tmp_path, chronometer):
    # Handler swallows SIGTERM, then keeps pausing. The runner sends SIGTERM,
    # waits `timeout`, then force-kills via SIGKILL and raises "did not exit gracefully".
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import signal
        signal.signal(signal.SIGTERM, lambda *_: None)
        while True:
            signal.pause()
    """))
    with pytest.raises(RuntimeError, match="did not exit gracefully"):
        with chronometer, KopfCLI(['run', str(path)], signal=signal.SIGTERM, timeout=0.5) as runner:
            time.sleep(CODE_OVERHEAD)  # let it install the handlers
    assert runner.exit_code == -signal.SIGKILL
    assert 0.5 + CODE_OVERHEAD <= chronometer.seconds <= 0.5 + CODE_OVERHEAD + SPAWN_OVERHEAD


def test_external_signal_before_exit_is_abnormal(tmp_path, chronometer):
    # Handler self-signals with SIGTERM before we ever send it. Even though
    # signal=SIGTERM is requested, the runner must NOT treat -SIGTERM as normal
    # because we did not send the signal — it came from the operator itself.
    path = tmp_path / 'handlers.py'
    path.write_text(textwrap.dedent("""
        import os, signal, time
        print("STARTED")
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(10)  # never reached
    """))
    with pytest.raises(RuntimeError, match="exited with code"):
        with chronometer, KopfCLI(['run', str(path)], signal=signal.SIGTERM) as runner:
            runner.wait_for(b"STARTED", timeout=CODE_OVERHEAD)
            time.sleep(0.1)  # let the child self-signal before we send our own
    assert runner.exit_code == -signal.SIGTERM
    assert 0.1 <= chronometer.seconds <= 0.5 + SPAWN_OVERHEAD
