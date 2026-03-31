========
Patching
========

Handlers can modify the Kubernetes resource they are handling
by using the :kwarg:`patch` keyword argument.
There are two patching strategies available:
the merge-patch dictionary for simple field changes,
and transformation functions for operations that depend
on the current state of the resource, such as list manipulations.

The changes from both strategies are mixed with the framework's own
modifications (such as progress storage, finalizer management,
and handler result delivery) and applied together in the minimal number
of API calls (depending on the resource definition).

There can be anywhere from zero to four patches per processing cycle,
depending on whether the status is a subresource, and whether the patch
is a mix of dictionary changes and transformation functions with actual changes.

Alternatively, operator developers can use any third-party
Kubernetes client library to patch their resources directly
inside the handlers instead of using the provided :kwarg:`patch` facility.


Dictionary merge-patches
========================

The :kwarg:`patch` object behaves as a mutable dictionary.
The changes accumulated in it are applied to the resource
as a JSON merge-patch (``application/merge-patch+json``)
after the handler finishes:

.. code-block:: python

    import kopf

    @kopf.on.create('kopfexamples')
    def ensure_defaults(spec, patch, **_):
        if 'greeting' not in spec:
            patch.spec['greeting'] = 'hello'
        patch.status['state'] = 'initialized'

Setting a field to ``None`` deletes it from the resource.
Nested dictionaries are merged recursively.
Other values overwrite the existing ones:

.. code-block:: python

    import kopf

    @kopf.on.update('kopfexamples')
    def cleanup_obsolete_fields(patch, **_):
        patch.spec['obsoleteField'] = None  # deletes the field
        patch.status['phase'] = 'updated'   # overwrites the value


Transformation functions
========================

Some changes cannot be expressed as a merge patch.
In particular, list operations (appending to or removing from a list)
require knowing the current state of the list to calculate the correct indices.
For example, adding a finalizer requires knowing how many finalizers
are already present; removing one requires knowing its position in the list.

For these cases, the :kwarg:`patch` object provides the ``patch.fns`` property.
You can append any function of type ``Callable[[dict], None]`` to ``patch.fns``
(or insert into the beginning or the middle of the list, if that matters).
Each function accepts the raw resource body as a positional argument
(a regular mutable dictionary) and mutates it in place.
The dictionary being mutated is already a deep copy of the original body,
so there is no need to worry:

.. code-block:: python

    import kopf

    def add_finalizer(body):
        finalizers = body.setdefault('metadata', {}).setdefault('finalizers', [])
        if 'my-operator/cleanup' not in finalizers:
            finalizers.append('my-operator/cleanup')

    @kopf.on.create('kopfexamples')
    def create_fn(patch, **_):
        patch.fns.append(add_finalizer)

The framework calls the transformation functions against the freshest seen
resource body and computes a JSON diff (``application/json-patch+json``)
relative to that body.
The resulting JSON Patch operations are sent to the Kubernetes API
with an optimistic concurrency check on the ``metadata.resourceVersion``.

.. note::

    The body passed to the transformation function is the latest version
    of the resource known to the framework at the time the function is applied.
    It may already reflect the results of earlier patch operations
    in the same or previous processing cycles, so it is not necessarily
    the body from the event that triggered the handler.

The transformation functions may be called more than once across
the same or several processing cycles ---
for instance, if the API server rejects the patch due to a conflict.
The functions should therefore be safe to call repeatedly:
they should check the current state before making changes
rather than assuming a particular initial state.

The transformation function takes a single positional argument
for the body. If additional positional or keyword arguments are needed,
use ``functools.partial``:

.. code-block:: python

    import functools
    import kopf

    def set_label(body, name, value):
        body.setdefault('metadata', {}).setdefault('labels', {})[name] = value

    @kopf.on.create('kopfexamples')
    def create_fn(patch, **_):
        patch.fns.append(functools.partial(set_label, name='my-label', value='my-value'))


Patch timing in daemons and timers
==================================

For :doc:`daemons <daemons>` and :doc:`timers <timers>`, the patch
is applied after the handler exits on each iteration of the run loop ---
including when the handler raises :class:`kopf.TemporaryError` for retrying.
After the patch is applied, it is cleared for the next iteration.

This means any changes accumulated in the patch dictionary
and any transformation functions appended to ``patch.fns``
during the handler's execution are sent to the Kubernetes API
before the next invocation of the handler starts.

If a transformation function's JSON Patch is rejected by the API server
due to an optimistic concurrency conflict (HTTP 422), the transformation
functions are carried forward to the next iteration, where they are
retried against the newer state of the resource. The retry does not happen
in the background --- it waits until the handler is invoked again on the next
timer interval or daemon retry. Handlers can detect carried-forward
transformation functions by checking ``bool(patch)`` at the start of the
handler: if it is true before the handler has made any changes, it means
there are pending transformation functions from a previous iteration.
