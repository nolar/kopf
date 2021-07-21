import logging

import pytest

import kopf
from kopf._cogs.structs.bodies import Body
from kopf._cogs.structs.ephemera import Memo
from kopf._cogs.structs.patches import Patch
from kopf._core.actions.progression import State
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.intents.causes import ChangingCause, Reason


@pytest.mark.parametrize('lifecycle', [
    kopf.lifecycles.all_at_once,
    kopf.lifecycles.one_by_one,
    kopf.lifecycles.randomized,
    kopf.lifecycles.shuffled,
    kopf.lifecycles.asap,
])
async def test_protocol_invocation(lifecycle, resource):
    """
    To be sure that all kwargs are accepted properly.
    Especially when the new kwargs are added or an invocation protocol changed.
    """
    # The values are irrelevant, they can be anything.
    state = State.from_scratch()
    cause = ChangingCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body({}),
        initial=False,
        reason=Reason.NOOP,
    )
    handlers = []
    selected = lifecycle(handlers, state=state, **cause.kwargs)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 0
