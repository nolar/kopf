import kopf
from kopf.reactor.handling import handler_var
from kopf.reactor.invocation import context


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def child_fn(**_):
    pass


def test_with_no_parent(
        resource_registry_cls, cause_factory):

    cause = cause_factory(resource_registry_cls)
    registry = resource_registry_cls()

    with context([(handler_var, None)]):
        kopf.subhandler(registry=registry)(child_fn)

    handlers = registry.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is child_fn
    assert handlers[0].id == 'child_fn'


def test_with_parent(
        parent_handler, resource_registry_cls, cause_factory):

    cause = cause_factory(resource_registry_cls)
    registry = resource_registry_cls()

    with context([(handler_var, parent_handler)]):
        kopf.subhandler(registry=registry)(child_fn)

    handlers = registry.get_handlers(cause)
    assert len(handlers) == 1
    assert handlers[0].fn is child_fn
    assert handlers[0].id == 'parent_fn/child_fn'
