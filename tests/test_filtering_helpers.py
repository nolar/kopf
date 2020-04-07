import kopf


def _never1(*_, **__):
    return False


def _never2(*_, **__):
    return False


def _always1(*_, **__):
    return True


def _always2(*_, **__):
    return True


def test_notfn_when_true():
    combined = kopf.not_(_always1)
    result = combined()
    assert result is False


def test_notfn_when_false():
    combined = kopf.not_(_never1)
    result = combined()
    assert result is True


def test_allfn_when_all_are_true():
    combined = kopf.all_([_always1, _always2])
    result = combined()
    assert result is True


def test_allfn_when_one_is_false():
    combined = kopf.all_([_always1, _never1])
    result = combined()
    assert result is False


def test_allfn_when_all_are_false():
    combined = kopf.all_([_never1, _never2])
    result = combined()
    assert result is False


def test_allfn_when_no_functions():
    combined = kopf.all_([])
    result = combined()
    assert result is True


def test_anyfn_when_all_are_true():
    combined = kopf.any_([_always1, _always2])
    result = combined()
    assert result is True


def test_anyfn_when_one_is_false():
    combined = kopf.any_([_always1, _never1])
    result = combined()
    assert result is True


def test_anyfn_when_all_are_false():
    combined = kopf.any_([_never1, _never2])
    result = combined()
    assert result is False


def test_anyfn_when_no_functions():
    combined = kopf.any_([])
    result = combined()
    assert result is False


def test_nonefn_when_all_are_true():
    combined = kopf.none_([_always1, _always2])
    result = combined()
    assert result is False


def test_nonefn_when_one_is_false():
    combined = kopf.none_([_always1, _never1])
    result = combined()
    assert result is False


def test_nonefn_when_all_are_false():
    combined = kopf.none_([_never1, _never2])
    result = combined()
    assert result is True


def test_nonefn_when_no_functions():
    combined = kopf.none_([])
    result = combined()
    assert result is True
