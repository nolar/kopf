import pytest

import kopf
from kopf.reactor.causation import Cause
from kopf.reactor.invocation import invoke


@pytest.mark.parametrize('lifecycle', [
    kopf.lifecycles.all_at_once,
    kopf.lifecycles.one_by_one,
    kopf.lifecycles.randomized,
    kopf.lifecycles.shuffled,
    kopf.lifecycles.asap,
])
async def test_protocol_invocation(mocker, lifecycle):
    """
    To be sure that all kwargs are accepted properly.
    Especially when the new kwargs are added or an invocation protocol changed.
    """
    cause = mocker.Mock(spec=Cause)
    handlers = []
    selected = await invoke(lifecycle, handlers, cause=cause)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 0
