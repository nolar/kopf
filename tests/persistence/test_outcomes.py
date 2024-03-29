from kopf._core.actions.execution import Outcome, Result


def test_creation_for_ignored_handlers():
    outcome = Outcome(final=True)
    assert outcome.final
    assert outcome.delay is None
    assert outcome.result is None
    assert outcome.exception is None
    assert not outcome.subrefs


def test_creation_for_results():
    result = Result(object())
    outcome = Outcome(final=True, result=result)
    assert outcome.final
    assert outcome.delay is None
    assert outcome.result is result
    assert outcome.exception is None
    assert not outcome.subrefs


def test_creation_for_permanent_errors():
    error = Exception()
    outcome = Outcome(final=True, exception=error)
    assert outcome.final
    assert outcome.delay is None
    assert outcome.result is None
    assert outcome.exception is error
    assert not outcome.subrefs


def test_creation_for_temporary_errors():
    error = Exception()
    outcome = Outcome(final=False, exception=error, delay=123)
    assert not outcome.final
    assert outcome.delay == 123
    assert outcome.result is None
    assert outcome.exception is error
    assert not outcome.subrefs


def test_creation_with_subrefs():
    outcome = Outcome(final=True, subrefs=['sub1', 'sub2'])
    assert outcome.subrefs == ['sub1', 'sub2']
