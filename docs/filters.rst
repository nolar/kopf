=========
Filtering
=========

.. highlight:: python

It is possible to only execute handlers when the object that triggers a handler
matches certain criteria.

The following filters are available for all resource-related handlers
(event-watching and change-detecting):


By labels
=========

* Match an object's label and value::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': 'somevalue'})
    def my_handler(spec, **_):
        pass

* Match on the existence of an object's label::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': kopf.PRESENT})
    def my_handler(spec, **_):
        pass

* Match on the absence of an object's label::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': kopf.ABSENT})
    def my_handler(spec, **_):
        pass


By annotations
==============

* Match on object's annotation and value::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    annotations={'some-annotation': 'somevalue'})
    def my_handler(spec, **_):
        pass

* Match on the existence of an object's annotation::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    annotations={'some-annotation': kopf.PRESENT})
    def my_handler(spec, **_):
        pass

* Match on the absence of an object's annotation::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    annotations={'some-annotation': kopf.ABSENT})
    def my_handler(spec, **_):
        pass


By arbitrary callbacks
======================

* Check on any field on the body with a when callback.
  The filter callback takes the same args as a handler (see :doc:`kwargs`)::

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    when=lambda spec, **_: spec.get('my-field') == 'somevalue')
    def my_handler(spec, **_):
        pass

* Check on labels/annotations with an arbitrary callback for individual values
  (the value comes as the first positional argument, plus usual :doc:`kwargs`)::

    def check_value(value, spec, **_):
        return value == 'some-value' and spec.get('field') is not None

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': check_value},
                    annotations={'some-annotation': check_value})
    def my_handler(spec, **_):
        pass

Kopf provides few helpers to combine multiple callbacks into one
(the semantics is the same as for Python's built-in functions)::

    import kopf

    def whole_fn1(name, **_): return name.startswith('kopf-')
    def whole_fn2(spec, **_): return spec.get('field') == 'value'
    def value_fn1(value, **_): return value.startswith('some')
    def value_fn2(value, **_): return value.endswith('label')

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    when=kopf.all_([whole_fn1, whole_fn2]),
                    labels={'somelabel': kopf.all_([value_fn1, value_fn2])})
    def create_fn1(**_):
        pass

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    when=kopf.any_([whole_fn1, whole_fn2]),
                    labels={'somelabel': kopf.any_([value_fn1, value_fn2])})
    def create_fn2(**_):
        pass

The following wrappers are available:

* `kopf.not_(fn)` -- the function must return ``False`` to pass the filters.
* `kopf.any_([...])` -- at least one of the functions must return ``True``.
* `kopf.all_([...])` -- all of the functions must return ``True``.
* `kopf.none_([...])` -- all of the functions must return ``False``.


Stealth mode
============

.. note::

    Please note that if an object does not match any filters of any handlers
    for its resource kind, there will be no messages logged and no annotations
    stored on the object. Such objects are processed in the stealth mode
    even if the operator technically sees them in the watch-stream.

    As the result, when the object is updated to match the filters some time
    later (e.g. by putting labels/annotations on it, or changing its spec),
    this will not be considered as an update, but as a creation.

    From the operator's point of view, the object has suddenly appeared
    in sight with no diff-base, which means that it is a newly created object;
    so, the on-creation handlers will be called instead of the on-update ones.

    This behaviour is correct and reasonable from the filtering logic side.
    If this is a problem, then create a dummy handler without filters
    (e.g. a field-handler for a non-existent field) --
    this will make all the objects always being in the scope of the operator,
    even if the operator did not react to their creation/update/deletion,
    and so the diff-base annotations ("last-handled-configuration", etc)
    will be always added on the actual object creation, not on scope changes.
