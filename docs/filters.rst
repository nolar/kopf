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

    @kopf.on.create('kopfexamples',
                    labels={'some-label': 'somevalue'},
                    annotations={'some-annotation': 'somevalue'})
    def my_handler(spec, **_):
        pass

Match only when the resource has a label or an annotation with any value:

.. code-block:: python

    @kopf.on.create('kopfexamples',
                    labels={'some-label': kopf.PRESENT},
                    annotations={'some-annotation': kopf.PRESENT})
    def my_handler(spec, **_):
        pass

Match only when the resource has no label or annotation with that name:

.. code-block:: python

    @kopf.on.create('kopfexamples',
                    labels={'some-label': kopf.ABSENT},
                    annotations={'some-annotation': kopf.ABSENT})
    def my_handler(spec, **_):
        pass

Note that empty strings in labels and annotations are treated as regular values,
i.e. they are considered as present on the resource.


Field filters
=============

Specific fields can be checked for specific values or for presence/absence,
similar to the metadata filters:

.. code-block:: python

    @kopf.on.create('kopfexamples', field='spec.field', value='world')
    def created_with_world_in_field(**_):
        pass

    @kopf.on.create('kopfexamples', field='spec.field', value=kopf.PRESENT)
    def created_with_field(**_):
        pass

    @kopf.on.create('kopfexamples', field='spec.no-field', value=kopf.ABSENT)
    def created_without_field(**_):
        pass

When the ``value=`` filter is not specified, but the ``field=`` filter is,
it is equivalent to ``value=kopf.PRESENT``, i.e. the field must be present
with any value (for update handlers: present before or after the change).

.. code-block:: python

    @kopf.on.create('kopfexamples', field='spec.field')
    def created_with_field(**_):
        pass

    @kopf.on.update('kopfexamples', field='spec.field')
    def field_is_affected(old, new, **_):
        pass

Due to a special nature of the update handlers (``@on.update``, ``@on.field``),
described in a note below, this filtering semantics is extended for them:

The ``field=`` filter restricts the update-handlers to cases when the specified
field is in any way affected: changed, added or removed to/from the resource.
When the specified field is not affected, but something else is changed,
such update-handlers are not invoked even if they do match the field criteria.

The ``value=`` filter applies to either the old or the new value:
i.e. if any of them satisfies the value criterion. This covers both sides
of the state transition: when the value criterion has just been satisfied
(though was not satisfied before), or when the value criterion was satisfied
before (but stopped being satisfied). For the latter case, it means that
the transitioning resource still satisfies the filter in its "old" state.

.. note::

    **Technically,** the update handlers are called after the change has already
    happened on the low level -- i.e. when the field already has the new value.

    **Semantically,** the update handlers are only initiated by this change,
    but are executed before the current (new) state is processed and persisted,
    thus marking the end of the change processing cycle -- i.e. they are called
    in-between the old and new states, and therefore belong to both of them.

    **In general,** the resource-changing handlers are an abstraction on top
    of the low level K8s machinery for eventual processing of such state
    transitions, so their semantics can differ from K8s's low-level semantics.
    In most cases, this is not visible or important to the operator developers,
    except for such cases, where it might affect the semantics of e.g. filters.

For reacting to *unrelated* changes of other fields while this field
satisfies the criterion, use ``when=`` instead of ``field=/value=``.

For reacting to only the cases when the desired state is reached
but not when the desired state is lost, use ``new=`` with the same criterion;
similarly, for the cases when the desired state is only lost, use ``old=``.

For all other handlers with no concept of "updating" and being in-between of
two equally valid and applicable states, the ``field=/value=`` filters
check the resource in its current --and the only-- state.
The handlers are being invoked and the daemons are running
as long as the field and the value match the criterion.


Change filters
==============

The update handlers (specifically, ``@kopf.on.update`` and ``@kopf.on.field``)
check the ``value=`` filter against both old & new values,
which might be not what is intended.
For more precision on filtering, the old/new values
can be checked separately with the ``old=/new=`` filters
with the same filtering methods/markers as all other filters.

.. code-block:: python

    @kopf.on.update('kopfexamples', field='spec.field', old='x', new='y')
    def field_is_edited(**_):
        pass

    @kopf.on.update('kopfexamples', field='spec.field', old=kopf.ABSENT, new=kopf.PRESENT)
    def field_is_added(**_):
        pass

    @kopf.on.update('kopfexamples', field='spec.field', old=kopf.PRESENT, new=kopf.ABSENT)
    def field_is_removed(**_):
        pass

If one of ``old=`` or ``new=`` is not specified (or set to ``None``),
that part is not checked, but the other (specified) part is still checked:

*Match when the field reaches a specific value either by being edited/patched
to it or by adding it to the resource (i.e. regardless of the old value):*

.. code-block:: python

    @kopf.on.update('kopfexamples', field='spec.field', new='world')
    def hello_world(**_):
        pass

*Match when the field loses a specific value either by being edited/patched
to something else, or by removing the field from the resource:*

.. code-block:: python

    @kopf.on.update('kopfexamples', field='spec.field', old='world')
    def goodbye_world(**_):
        pass

Generally, the update handlers with ``old=/new=`` filters are invoked only when
the field's value is changed, and are not invoked when it remains the same.

For clarity, "a change" means not only an actual change of the value,
but also a change in the field's presence or absence in the resource.

If none of the ``old=/new=/value=`` filters is specified, the handler is invoked
if the field is affected in any way, i.e. if it was modified, added, or removed.
This is the same behaviour as with the unspecified ``value=`` filter.

.. note::

    ``value=`` is currently made to be mutually exclusive with ``old=/new=``:
    only one filtering method can be used; if both methods are used together,
    it would be ambiguous. This can be reconsidered in the future.


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

    @kopf.on.create('kopfexamples',
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

    @kopf.on.create('kopfexamples', when=is_good_enough)
    def my_handler(spec, **_):
        pass

    @kopf.on.create('kopfexamples', when=lambda spec, **_: spec.get('field') in spec.get('items', []))
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

    @kopf.on.create('kopfexamples',
                    when=kopf.all_([whole_fn1, whole_fn2]),
                    labels={'somelabel': kopf.all_([value_fn1, value_fn2])})
    def create_fn1(**_):
        pass

    @kopf.on.create('kopfexamples',
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
