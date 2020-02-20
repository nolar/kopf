import functools

from kopf.reactor.registries import get_callable_id


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn():
    pass


def test_id_of_simple_function():
    fn_id = get_callable_id(some_fn)
    assert fn_id == 'some_fn'


def test_id_of_single_partial():
    partial_fn = functools.partial(some_fn)

    fn_id = get_callable_id(partial_fn)
    assert fn_id == 'some_fn'


def test_id_of_double_partial():
    partial1_fn = functools.partial(some_fn)
    partial2_fn = functools.partial(partial1_fn)

    fn_id = get_callable_id(partial2_fn)
    assert fn_id == 'some_fn'


def test_id_of_single_wrapper():

    @functools.wraps(some_fn)
    def wrapped_fn():
        pass

    fn_id = get_callable_id(wrapped_fn)
    assert fn_id == 'some_fn'


def test_id_of_double_wrapper():

    @functools.wraps(some_fn)
    def wrapped1_fn():
        pass

    @functools.wraps(wrapped1_fn)
    def wrapped2_fn():
        pass

    fn_id = get_callable_id(wrapped2_fn)
    assert fn_id == 'some_fn'


def test_id_of_lambda():
    some_lambda = lambda: None

    fn_id = get_callable_id(some_lambda)
    assert fn_id.startswith(f'lambda:{__file__}:')
