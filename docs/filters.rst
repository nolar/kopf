=========
Filtering
=========

Handlers can be restricted to only the resources that match certain criteria.

Multiple criteria are joined with AND, i.e. they all must be satisfied.

Unless stated otherwise, the described filters are available for all handlers:
resuming, creation, deletion, updating, event-watching, timers, daemons,
or even to sub-handlers (thus eliminating some checks in their parent's code).

There are only a few kinds of checks:

* Specific values --- expressed with Python literals such as ``"a string"``.
* Presence of values --- with special markers ``kopf.PRESENT/kopf.ABSENT``.
* Per-value callbacks --- with anything callable which evaluates to true/false.
* Whole-body callbacks --- with anything callable which evaluates to true/false.

But there are multiple places where these checks can be applied,
and each has its own specifics.


Metadata filters
================

Metadata is the most commonly filtered aspect of the resources.

Match only when the resource's label or annotation has a specific value:

.. code-block:: python

    @kopf.on.create('kopfexamples',
                    labels={'some-label': 'somevalue'},
                    annotations={'some-annotation': 'somevalue'})
    def my_handler(spec: kopf.Spec, **_: Any) -> None:
        pass

Match only when the resource has a label or an annotation with any value:

.. code-block:: python

    @kopf.on.create('kopfexamples',
                    labels={'some-label': kopf.PRESENT},
                    annotations={'some-annotation': kopf.PRESENT})
    def my_handler(spec: kopf.Spec, **_: Any) -> None:
        pass

Match only when the resource has no label or annotation with that name:

.. code-block:: python

    @kopf.on.create('kopfexamples',
                    labels={'some-label': kopf.ABSENT},
                    annotations={'some-annotation': kopf.ABSENT})
    def my_handler(spec: kopf.Spec, **_: Any) -> None:
        pass

Note that empty strings in labels and annotations are treated as regular values,
i.e. they are considered as present on the resource.


Field filters
=============

Specific fields can be checked for specific values or presence/absence,
similar to the metadata filters:

.. code-block:: python

    @kopf.on.create('kopfexamples', field='spec.field', value='world')
    def created_with_world_in_field(**_: Any) -> None:
        pass

    @kopf.on.create('kopfexamples', field='spec.field', value=kopf.PRESENT)
    def created_with_field(**_: Any) -> None:
        pass

    @kopf.on.create('kopfexamples', field='spec.no-field', value=kopf.ABSENT)
    def created_without_field(**_: Any) -> None:
        pass

When the ``value=`` filter is not specified, but the ``field=`` filter is,
it is equivalent to ``value=kopf.PRESENT``, i.e. the field must be present
with any value (for update handlers: present before or after the change).

.. code-block:: python

    @kopf.on.create('kopfexamples', field='spec.field')
    def created_with_field(**_: Any) -> None:
        pass

    @kopf.on.update('kopfexamples', field='spec.field')
    def field_is_affected(old: Any, new: Any, **_: Any) -> None:
        pass


Since the field name is part of the handler id (e.g., ``"fn/spec.field"``),
multiple decorators can be defined to react to different fields with the same
function, and it will be invoked multiple times with different old and new values
relevant to the specified fields, as well as different values of :kwarg:`param`:

.. code-block:: python

    @kopf.on.update('kopfexamples', field='spec.field', param='fld')
    @kopf.on.update('kopfexamples', field='spec.items', param='itm')
    def one_of_the_fields_is_affected(old: Any, new: Any, **_: Any) -> None:
        pass

However, different causes ---mostly resuming combined with one of creation/update/deletion---
will not be distinguished, so e.g. a resume+create pair with the same field
will be called only once.

Due to the special nature of update handlers (``@on.update``, ``@on.field``),
described in a note below, these filtering semantics are extended for them:

The ``field=`` filter restricts the update handlers to cases where the specified
field is affected in any way: changed, added, or removed from the resource.
When the specified field is not affected but something else is changed,
such update handlers are not invoked even if they do match the field criteria.

The ``value=`` filter applies to either the old or the new value:
i.e. if either of them satisfies the value criterion. This covers both sides
of the state transition: when the value criterion has just been satisfied
(but was not satisfied before), or when the value criterion was satisfied
before (but is no longer satisfied). In the latter case, the transitioning
resource still satisfies the filter in its "old" state.

.. note::

    **Technically,** the update handlers are called after the change has already
    happened on the low level --- i.e. when the field already has the new value.

    **Semantically,** the update handlers are only initiated by this change,
    but are executed before the current (new) state is processed and persisted,
    thus marking the end of the change processing cycle --- i.e. they are called
    in-between the old and new states, and therefore belong to both of them.

    **In general,** the resource-changing handlers are an abstraction on top
    of the low-level K8s machinery for eventual processing of such state
    transitions, so their semantics can differ from K8s's low-level semantics.
    In most cases, this is not visible or important to operator developers,
    except in cases where it might affect the semantics of e.g. filters.

For reacting to *unrelated* changes of other fields while this field
satisfies the criterion, use ``when=`` instead of ``field=/value=``.

For reacting to only the cases when the desired state is reached
but not when the desired state is lost, use ``new=`` with the same criterion;
similarly, for the cases when the desired state is only lost, use ``old=``.

For all other handlers that have no concept of "updating" and being in between
two equally valid and applicable states, the ``field=/value=`` filters
check the resource in its current ---and only--- state.
The handlers are invoked and the daemons run
as long as the field and value match the criterion.


Change filters
==============

The update handlers (specifically, ``@kopf.on.update`` and ``@kopf.on.field``)
check the ``value=`` filter against both old and new values,
which might not be what is intended.
For more precise filtering, the old and new values
can be checked separately with the ``old=/new=`` filters
using the same filtering methods/markers as all other filters.

.. code-block:: python

    @kopf.on.update('kopfexamples', field='spec.field', old='x', new='y')
    def field_is_edited(**_: Any) -> None:
        pass

    @kopf.on.update('kopfexamples', field='spec.field', old=kopf.ABSENT, new=kopf.PRESENT)
    def field_is_added(**_: Any) -> None:
        pass

    @kopf.on.update('kopfexamples', field='spec.field', old=kopf.PRESENT, new=kopf.ABSENT)
    def field_is_removed(**_: Any) -> None:
        pass

If one of ``old=`` or ``new=`` is not specified (or set to ``None``),
that part is not checked, but the other (specified) part is still checked:

*Match when the field reaches a specific value either by being edited/patched
to it or by adding it to the resource (i.e. regardless of the old value):*

.. code-block:: python

    @kopf.on.update('kopfexamples', field='spec.field', new='world')
    def hello_world(**_: Any) -> None:
        pass

*Match when the field loses a specific value either by being edited/patched
to something else, or by removing the field from the resource:*

.. code-block:: python

    @kopf.on.update('kopfexamples', field='spec.field', old='world')
    def goodbye_world(**_: Any) -> None:
        pass

Generally, the update handlers with ``old=/new=`` filters are invoked only when
the field's value changes, and are not invoked when it remains the same.

For clarity, "a change" means not only an actual change of the value,
but also a change in whether the field is present or absent in the resource.

If none of the ``old=/new=/value=`` filters is specified, the handler is invoked
if the field is affected in any way, i.e. if it was modified, added, or removed.
This is the same behavior as with the unspecified ``value=`` filter.

.. note::

    ``value=`` is currently mutually exclusive with ``old=/new=``:
    only one filtering method can be used; using both together
    would be ambiguous. This may be reconsidered in the future.


Value callbacks
===============

Instead of specific values or special markers, all value-based filters can
use arbitrary per-value callbacks (as an advanced use case for complex logic).

The value callbacks must accept the same :doc:`keyword arguments <kwargs>`
as the respective handlers (with ``**kwargs/**_`` for forward compatibility),
plus one *positional* (not keyword!) argument with the value being checked.
The passed value will be ``None`` if the value is absent in the resource.

.. code-block:: python

    def check_value(value: str | None, /, spec: kopf.Spec, **_: Any) -> bool:
        return value == 'some-value' and spec.get('field') is not None

    @kopf.on.create('kopfexamples',
                    labels={'some-label': check_value},
                    annotations={'some-annotation': check_value})
    def my_handler(spec: kopf.Spec, **_: Any) -> None:
        pass


Callback filters
================

The resource callbacks must accept the same :doc:`keyword arguments <kwargs>`
as the respective handlers (with ``**kwargs/**_`` for forward compatibility).

.. code-block:: python

    def is_good_enough(spec: kopf.Spec, **_: Any) -> bool:
        return spec.get('field') in spec.get('items', [])

    @kopf.on.create('kopfexamples', when=is_good_enough)
    def my_handler(spec: kopf.Spec, **_: Any) -> None:
        pass

    @kopf.on.create('kopfexamples', when=lambda spec, **_: spec.get('field') in spec.get('items', []))
    def my_handler(spec: kopf.Spec, **_: Any) -> None:
        pass

Callback filters are not limited to checking the resource's content.
They can filter by any kwarg data, e.g. by the :kwarg:`reason` of the invocation,
remembered :kwarg:`memo` values, etc. However, it is highly recommended that
filters do not modify the state of the operator --- keep that for handlers.

There is a subtle difference between callable filters and resource selectors
(see :doc:`resources`): a callable filter applies to all events
coming from a live watch stream identified by a resource kind and a namespace
(or by a resource kind alone for watch streams of cluster-wide operators);
a callable resource selector decides whether to start the watch stream
for that resource kind at all, which can help reduce the load on the API.


Callback helpers
================

Kopf provides several helpers to combine multiple callbacks into one
(the semantics are the same as for Python's built-in functions):

.. code-block:: python

    import kopf
    from typing import Any

    def whole_fn1(name: str, **_: Any) -> bool: return name.startswith('kopf-')
    def whole_fn2(spec: kopf.Spec, **_: Any) -> bool: return spec.get('field') == 'value'
    def value_fn1(value: str | None, **_: Any) -> bool: return value and value.startswith('some')
    def value_fn2(value: str | None, **_: Any) -> bool: return value and value.endswith('label')

    @kopf.on.create('kopfexamples',
                    when=kopf.all_([whole_fn1, whole_fn2]),
                    labels={'somelabel': kopf.all_([value_fn1, value_fn2])})
    def create_fn1(**_: Any) -> None:
        pass

    @kopf.on.create('kopfexamples',
                    when=kopf.any_([whole_fn1, whole_fn2]),
                    labels={'somelabel': kopf.any_([value_fn1, value_fn2])})
    def create_fn2(**_: Any) -> None:
        pass

The following wrappers are available:

* ``kopf.not_(fn)`` --- the function must return ``False`` to pass the filters.
* ``kopf.any_([...])`` --- at least one of the functions must return ``True``.
* ``kopf.all_([...])`` --- all of the functions must return ``True``.
* ``kopf.none_([...])`` --- all of the functions must return ``False``.


Stealth mode
============

.. note::

    Please note that if an object does not match any filters of any handlers
    for its resource kind, no messages will be logged and no annotations
    will be stored on the object. Such objects are processed in stealth mode
    even if the operator technically sees them in the watch stream.

    As a result, when the object is updated to match the filters some time
    later (e.g. by adding labels/annotations to it, or changing its spec),
    this will not be considered an update but a creation.

    From the operator's point of view, the object has suddenly appeared
    with no diff-base, which means it is treated as a newly created object;
    so the on-creation handlers will be called instead of the on-update ones.

    This behavior is correct and reasonable from the filtering logic perspective.
    If this is a problem, create a dummy handler without filters
    (e.g. a field handler for a non-existent field) ---
    this will keep all objects always in scope of the operator,
    even if the operator did not react to their creation/update/deletion,
    so the diff-base annotations ("last-handled-configuration", etc.)
    will always be added on actual object creation, not on scope changes.
