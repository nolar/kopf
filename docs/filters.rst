=========
Filtering
=========

Handlers can be restricted to only the resources that match certain criteria.

Multiple criteria are joined with AND, i.e. they all must be satisfied.

Unless stated otherwise, the described filters are available for all handlers:
resuming, creation, deletion, updating, event-watching, timers, daemons,
or even to sub-handlers (thus eliminating some checks in its parent's code).

There are only a few kinds of checks:

* Specific values -- expressed with Python literals such as ``"a string"``.
* Presence of values -- with special markers ``kopf.PRESENT/kopf.ABSENT``.
* Per-value callbacks -- with anything callable and evaluatable to true/false.
* Whole-body callbacks -- with anything callable and evaluatable to true/false.

But there are multiple places where these checks can be applied,
each has its own specifics.


Metadata filters
================

Metadata is the most commonly filtered aspect of the resources.

Match only when the resource's label or annotation has a specific value:

.. code-block:: python

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': 'somevalue'},
                    annotations={'some-annotation': 'somevalue'})
    def my_handler(spec, **_):
        pass

Match only when the resource has a label or an annotation with any value:

.. code-block:: python

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': kopf.PRESENT},
                    annotations={'some-annotation': kopf.PRESENT})
    def my_handler(spec, **_):
        pass

Match only when the resource has no label or annotation with that name:

.. code-block:: python

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': kopf.ABSENT},
                    annotations={'some-annotation': kopf.ABSENT})
    def my_handler(spec, **_):
        pass

Note that empty strings in labels and annotations are treated as regular values,
i.e. they are considered as present on the resource.


Value callbacks
===============

Instead of specific values or special markers, all the value-based filters can
use arbitrary per-value callbacks (as an advanced use-case for advanced logic).

The value callbacks must receive the same :doc:`keyword arguments <kwargs>`
as the respective handlers (with ``**kwargs/**_`` for forward compatibility),
plus one *positional* (not keyword!) argument with the value being checked.
The passed value will be ``None`` if the value is absent in the resource.

.. code-block:: python

    def check_value(value, spec, **_):
        return value == 'some-value' and spec.get('field') is not None

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    labels={'some-label': check_value},
                    annotations={'some-annotation': check_value})
    def my_handler(spec, **_):
        pass


Callback filters
================

The resource callbacks must receive the same :doc:`keyword arguments <kwargs>`
as the respective handlers (with ``**kwargs/**_`` for forward compatibility).

.. code-block:: python

    def is_good_enough(spec, **_):
        return spec.get('field') in spec.get('items', [])

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    when=is_good_enough)
    def my_handler(spec, **_):
        pass

    @kopf.on.create('zalando.org', 'v1', 'kopfexamples',
                    when=lambda spec, **_: spec.get('field') in spec.get('items', []))
    def my_handler(spec, **_):
        pass

There is no need for the callback filters to only check the resource's content.
They can filter by any kwarg data, e.g. by a :kwarg:`reason` of this invocation,
remembered :kwarg:`memo` values, etc. However, it is highly recommended that
the filters do not modify the state of the operator -- keep it for handlers.


Callback helpers
================

Kopf provides several helpers to combine multiple callbacks into one
(the semantics is the same as for Python's built-in functions):

.. code-block:: python

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
