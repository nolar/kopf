import kopf


def test_nothing(invoke, login, real_run):
    result = invoke(['run'])
    assert result.exit_code == 0

    registry = kopf.get_default_registry()
    assert len(registry.resources) == 0


def test_one_file(invoke, login, real_run):
    result = invoke(['run', 'handler1.py'])
    assert result.exit_code == 0

    registry = kopf.get_default_registry()
    assert len(registry.resources) == 1
    resource = list(registry.resources)[0]
    handlers = registry._cause_handlers[resource]._handlers
    assert len(handlers) == 1
    assert handlers[0].id == 'create_fn'


def test_two_files(invoke, login, real_run):
    result = invoke(['run', 'handler1.py', 'handler2.py'])
    assert result.exit_code == 0

    registry = kopf.get_default_registry()
    assert len(registry.resources) == 1
    resource = list(registry.resources)[0]
    handlers = registry._cause_handlers[resource]._handlers
    assert len(handlers) == 2
    assert handlers[0].id == 'create_fn'
    assert handlers[1].id == 'update_fn'


def test_one_module(invoke, login, real_run):
    result = invoke(['run', '-m', 'package.module_1'])
    assert result.exit_code == 0

    registry = kopf.get_default_registry()
    assert len(registry.resources) == 1
    resource = list(registry.resources)[0]
    handlers = registry._cause_handlers[resource]._handlers
    assert len(handlers) == 1
    assert handlers[0].id == 'create_fn'


def test_two_modules(invoke, login, real_run):
    result = invoke(['run', '-m', 'package.module_1', '-m', 'package.module_2'])
    assert result.exit_code == 0

    registry = kopf.get_default_registry()
    assert len(registry.resources) == 1
    resource = list(registry.resources)[0]
    handlers = registry._cause_handlers[resource]._handlers
    assert len(handlers) == 2
    assert handlers[0].id == 'create_fn'
    assert handlers[1].id == 'update_fn'


def test_mixed_sources(invoke, login, real_run):
    result = invoke(['run', 'handler1.py', '-m', 'package.module_2'])
    assert result.exit_code == 0

    registry = kopf.get_default_registry()
    assert len(registry.resources) == 1
    resource = list(registry.resources)[0]
    handlers = registry._cause_handlers[resource]._handlers
    assert len(handlers) == 2
    assert handlers[0].id == 'create_fn'
    assert handlers[1].id == 'update_fn'
