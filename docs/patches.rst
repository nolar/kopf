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

There can be from zero to four patches per one processing cycle,
depending on whether the status is a subresource, and whether the patch
is a mix of dictionary changes and transformation functions with actual changes.

Alternatively, the operator developers can use any third-party
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

Some changes cannot be expressed as a merge-patch.
In particular, list operations (appending to or removing from a list)
require knowing the current state of the list to calculate the correct indices.
For example, adding a finalizer requires knowing how many finalizers
are already present; removing one requires knowing its position in the list.

For these cases, the :kwarg:`patch` object provides the ``patch.fns`` property.
Any function of type :type:`kopf.PatchFn` can be appended to ``patch.fns``
(or inserted into the beginning or the middle of the list, if that matters).
Each function accepts the raw resource body as a positional argument
(a regular mutable dictionary) and mutates it in place; the dictionary being
mutated is already a deep copy of the original body, so no need to worry:

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

The transformation functions can be called more than once across
the same or several processing cycles --
for instance, if the API server rejects the patch due to a conflict.
The functions should therefore be safe to call repeatedly:
they should check the current state before making changes
rather than assuming a particular starting state.


Arguments to transformation functions
--------------------------------------

The transformation function signature is a single positional argument
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
