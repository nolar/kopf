==================
In-memory indexing
==================

Indexers automatically maintain in-memory overviews of resources (indices),
grouped by keys that are usually calculated based on these resources.

The indices can be used for cross-resource awareness:
e.g., when a resource of kind X is changed, it can get all the information
about all resources of kind Y without talking to the Kubernetes API.
Under the hood, the centralised watch-streams ---one per resource kind--- are
more efficient in gathering the information than individual listing requests.


Index declaration
=================

Indices are declared with a ``@kopf.index`` decorator on an indexing function
(all standard filters are supported --- see :doc:`filters`):

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def my_idx(**_):
        ...

The name of the function or its ``id=`` option is the index's name.

The indices are then available to all resource- and operator-level handlers
as the direct kwargs named the same as the index (type hints are optional):

.. code-block:: python

    import kopf

    # ... continued from previous examples:
    @kopf.timer('KopfExample', interval=5)
    def tick(my_idx: kopf.Index, **_):
        ...

    @kopf.on.probe()
    def metric(my_idx: kopf.Index, **_):
        ...

When a resource is created or starts matching the filters, it is processed
by all relevant indexing functions, and the result is put into the indices.

When a previously indexed resource is deleted or stops matching the filters,
all associated values are removed (so are all empty collections after this
--- to keep the indices clean).

.. seealso::
    :doc:`/probing` for probing handlers in the example above.


Index structure
===============

An index is always a read-only *mapping* (dictionary-like) of type `kopf.Index`
with arbitrary keys leading to *collections* of arbitrary values (`kopf.Store`).
The index is initially empty. The collections are never empty
(empty collections are removed when the last item in them is removed).

For example, if several individual resources return the following results
from the same indexing function, then the index gets the following structure
(shown in the comment below the code):

.. code-block:: python

    return {'key1': 'valueA'}  # 1st
    return {'key1': 'valueB'}  # 2nd
    return {'key2': 'valueC'}  # 3rd
    # {'key1': ['valueA', 'valueB'],
    #  'key2': ['valueC']}

The indices are not nested. The 2nd-level mapping in the result
is stored as a regular value:

.. code-block:: python

    return {'key1': 'valueA'}  # 1st
    return {'key1': 'valueB'}  # 2nd
    return {'key2': {'key3': 'valueC'}}  # 3rd
    # {'key1': ['valueA', 'valueB'],
    #  'key2': [{'key3': 'valueC'}]}


Index content
=============

When an indexing function returns a ``dict`` (strictly ``dict``!
not a generic mapping, not even a descendant of ``dict``, such as `kopf.Memo`),
it is merged into the index under the key taken from the result:

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def string_keys(namespace, name, **_):
        return {namespace: name}
        # {'namespace1': ['pod1a', 'pod1b', ...],
        #  'namespace2': ['pod2a', 'pod2b', ...],
        #   ...]

Multi-value keys are possible with e.g. tuples or other hashable types:

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def tuple_keys(namespace, name, **_):
        return {(namespace, name): 'hello'}
        # {('namespace1', 'pod1a'): ['hello'],
        #  ('namespace1', 'pod1b'): ['hello'],
        #  ('namespace2': 'pod2a'): ['hello'],
        #  ('namespace2', 'pod2b'): ['hello'],
        #   ...}

Multiple keys can be returned at once for a single resource.
They are all merged into their relevant places in the index:

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def by_label(labels, name, **_):
        return {(label, value): name for label, value in labels.items()}
        # {('label1', 'value1a'): ['pod1', 'pod2', ...],
        #  ('label1', 'value1b'): ['pod3', 'pod4', ...],
        #  ('label2', 'value2a'): ['pod5', 'pod6', ...],
        #  ('label2', 'value2b'): ['pod1', 'pod3', ...],
        #   ...}

    @kopf.timer('kex', interval=5)
    def tick(by_label: kopf.Index, **_):
        print(list(by_label.get(('label2', 'value2b'), [])))
        # ['pod1', 'pod3']
        for podname in by_label.get(('label2', 'value2b'), []):
            print(f"==> {podname}")
        # ==> pod1
        # ==> pod3

Note the multiple occurrences of some pods because they have two or more labels.
But they never repeat within the same label --- labels can have only one value.


Recipes
=======

Unindexed collections
---------------------

When an indexing function returns a non-``dict`` --- i.e. strings, numbers,
tuples, lists, sets, memos, arbitrary objects except ``dict`` --- then the key
is assumed to be ``None`` and a flat index with only one key is constructed.
The resources are not indexed, but rather collected under the same key
(which is still considered as indexing):

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def pod_names(name: str, **_):
        return name
        # {None: ['pod1', 'pod2', ...]}

Other types and complex objects returned from the indexing function are stored
"as is" (i.e. with no special treatment):

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def container_names(spec: kopf.Spec, **_):
        return {container['name'] for container in spec.get('containers', [])}
        # {None: [{'main1', 'sidecar2'}, {'main2'}, ...]}


Enumerating resources
---------------------

If the goal is not to store any payload but to only list the existing resources,
then index the resources' identities (usually, their namespaces and names).

One way is to only collect their identities in a flat collection -- in case
you need mostly to iterate over all of them without key lookups:

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def pods_list(namespace, name, **_):
        return namespace, name
        # {None: [('namespace1', 'pod1a'),
        #         ('namespace1', 'pod1b'),
        #         ('namespace2', 'pod2a'),
        #         ('namespace2', 'pod2b'),
        #           ...]}

    @kopf.timer('kopfexamples', interval=5)
    def tick_list(pods_list: kopf.Index, **_):
        for ns, name in pods_list.get(None, []):
            print(f"{ns}::{name}")

Another way is to index them by keys --- when index lookups are going to happen
more often than index iterations:

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def pods_dict(namespace, name, **_):
        return {(namespace, name): None}
        # {('namespace1', 'pod1a'): [None],
        #  ('namespace1', 'pod1b'): [None],
        #  ('namespace2', 'pod2a'): [None],
        #  ('namespace2', 'pod2b'): [None],
        #   ...}

    @kopf.timer('kopfexamples', interval=5)
    def tick_dict(pods_dict: kopf.Index, spec: kopf.Spec, namespace: str, **_):
        monitored_namespace = spec.get('monitoredNamespace', namespace)
        for ns, name in pods_dict:
            if ns == monitored_namespace:
                print(f"in {ns}: {name}")


Mirroring resources
-------------------

To store the whole resource or its essential parts, return them explicitly:

.. code-block:: python

    import kopf

    @kopf.index('deployments')
    def whole_deployments(name: str, namespace: str, body: kopf.Body, **_):
        return {(namespace, name): body}

    @kopf.timer('kopfexamples', interval=5)
    def tick(whole_deployments: kopf.Index, **_):
        deployment, *_ = whole_deployments[('kube-system', 'coredns')]
        actual = deployment.status.get('replicas')
        desired = deployment.spec.get('replicas')
        print(f"{deployment.meta.name}: {actual}/{desired}")

.. note::

    Mind the memory consumption on large clusters and/or overly verbose objects.
    Especially mind the memory consumption for "managed fields"
    (see `kubernetes/kubernetes#90066`__).

    __ https://github.com/kubernetes/kubernetes/issues/90066


Indices of indices
------------------

Iterating over all keys of the index can be slow (especially if there are many
keys: e.g. with thousands of pods). For that case, an index of an index
can be built: with one primary indexing containing the real values to be used,
while the other secondary index only contains the keys of the primary index
(full or partial).

By looking up a single key in the secondary index, the operator can directly
get or indirectly reconstruct all the necessary keys in the primary index
instead of iterating over the primary index with filtering.

For example, we want to get all container names of all pods in a namespace.
In that case, the primary index will index containers by pods' namespaces+names,
while the secondary index will index pods' names by namespaces only:

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def primary(namespace, name, spec, **_):
        container_names = {container['name'] for container in spec['containers']}
        return {(namespace, name): container_names}
        # {('namespace1', 'pod1a'): [{'main'}],
        #  ('namespace1', 'pod1b'): [{'main', 'sidecar'}],
        #  ('namespace2', 'pod2a'): [{'main'}],
        #  ('namespace2', 'pod2b'): [{'the-only-one'}],
        #   ...}

    @kopf.index('pods')
    def secondary(namespace, name, **_):
        return {namespace: name}
        # {'namespace1': ['pod1a', 'pod1b'],
        #  'namespace2': ['pod2a', 'pod2b'],
        #   ...}

    @kopf.timer('kopfexamples', interval=5)
    def tick(primary: kopf.Index, secondary: kopf.Index, spec: kopf.Spec, **_):
        namespace_containers = set()
        monitored_namespace = spec.get('monitoredNamespace', 'default')
        for pod_name in secondary.get(monitored_namespace, []):
            reconstructed_key = (monitored_namespace, pod_name)
            pod_containers, *_ = primary[reconstructed_key]
            namespace_containers |= pod_containers
        print(f"containers in {monitored_namespace}: {namespace_containers}")
        # containers in namespace1: {'main', 'sidecar'}
        # containers in namespace2: {'main', 'the-only-one'}

However, such complicated structures and such performance requirements are rare.
For simplicity and performance, nested indices are not directly provided by
the framework as a feature, only as this tip based on other official features.


Conditional indexing
====================

Besides the usual filters (see :doc:`/filters`), the resources can be skipped
from indexing by returning ``None`` (Python's default for no-result functions).

If the indexing function returns ``None`` or does not return anything,
its result is ignored and not indexed. The existing values in the index
are preserved as they are (this is also the case when unexpected errors
happen in the indexing function with the errors mode set to ``IGNORED``):

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def empty_index(**_):
        pass
        # {}

However, if the indexing function returns a dict with ``None`` as values,
such values are indexed as usually (they are not ignored). ``None`` values
can be used as placeholders when only the keys are sufficient; otherwise,
indices and collections with no values left in them are removed from the index:

.. code-block:: python

    import kopf

    @kopf.index('pods')
    def index_of_nones(**_):
        return {'key': None}
        # {'key': [None, None, ...]}


Errors in indexing
==================

The indexing functions are supposed to be fast and non-blocking,
as they are capable of delaying the operator startup and resource processing.
For this reason, in case of errors in handlers, the handlers are never retried.

Arbitrary exceptions with ``errors=IGNORED`` (the default) make the framework
ignore the error and keep the existing indexed values (which are now stale).
It means that the new values are expected to appear soon, but the old values
are good enough meanwhile (which is usually highly probable). This is the same
as returning ``None``, except that the exception's stack trace is logged too:

.. code-block:: python

    import kopf

    @kopf.index('pods', errors=kopf.ErrorsMode.IGNORED)  # the default
    def fn1(**_):
        raise Exception("Keep the stale values, if any.")

`kopf.PermanentError` and arbitrary exceptions with ``errors=PERMANENT``
remove any existing indexed values and the resource's keys from the index,
and exclude the failed resource from indexing by this index in the future
(so that even the indexing function is not invoked for them):

.. code-block:: python

    import kopf

    @kopf.index('pods', errors=kopf.ErrorsMode.PERMANENT)
    def fn1(**_):
        raise Exception("Excluded forever.")

    @kopf.index('pods')
    def fn2(**_):
        raise kopf.PermamentError("Excluded forever.")

`kopf.TemporaryError` and arbitrary exceptions with ``errors=TEMPORARY``
remove any existing indexed values and the resource's keys from the index,
and exclude the failed resource from indexing for the specified duration
(via the error's ``delay`` option; set to ``0`` or ``None`` for no delay).
It is expected that the resource could be reindexed in the future,
but right now, problems are preventing this from happening:

.. code-block:: python

    import kopf

    @kopf.index('pods', errors=kopf.ErrorsMode.TEMPORARY)
    def fn1(**_):
        raise Exception("Excluded for 60s.")

    @kopf.index('pods')
    def fn2(**_):
        raise kopf.TemporaryError("Excluded for 30s.", delay=30)

In the "temporary" mode, the decorator's options for error handling are used:
the ``backoff=`` is a default delay before the resource can be re-indexed
(the default is 60 seconds; for no delay, use ``0`` explicitly);
the ``retries=`` and ``timeout=`` are the limit of retries and the overall
duration since the first failure until the resource will be marked
as permanently excluded from indexing (unless it succeeds at some point).

The handler's kwargs :kwarg:`retry`, :kwarg:`started`, :kwarg:`runtime`
report the retrying attempts since the first indexing failure.
Successful indexing resets all the counters/timeouts and the retrying state
is not stored (to save memory).

The same as with regular handlers (:doc:`errors`),
Kopf's error classes (expected errors) only log a short message,
while arbitrary exceptions (unexpected errors) also dump their stack traces.

This matches the semantics of regular handlers but with in-memory specifics.

.. warning::

    **There is no good out-of-the-box default mode for error handling:**
    any kind of errors in the indexing functions means that the index becomes
    inconsistent with the actual state of the cluster and its resources:
    the entries for matching resources are either "lost" (permanent or temporary
    errors), or contain possibly outdated/stale values (ignored errors) ---
    all of these cases are misinformation about the actual state of the cluster.

    The default mode is chosen to reduce the index changes and reindexing
    in case of frequent errors --- by not making any changes to the index.
    Besides, the stale values can still be relevant and useful to some extent.

    For two other cases, the operator developers have to explicitly accept the
    risks by setting ``errors=`` if the operator can afford to lose the keys.


Kwargs safety
=============

Indices that are injected into kwargs, overwrite any kwargs of the framework,
existing and those to be added later. This guarantees that the new framework
versions will not break an operator if new kwargs are added with the same name
as the existing indices.

In this case, the trade-off is that the handlers cannot use the new features
until their indices are renamed to something else. Since the new features are
new, the old operator's code does not use them, so it is backwards compatible.

To reduce the probability of name collisions, keep these conventions in mind
when naming indices (they are fully optional and for convenience only):

* System kwargs are usually one-word; name your indices with 2+ words.
* System kwargs are usually singular (not always); name the indices as plurals.
* System kwargs are usually nouns; using abbreviations or prefixes/suffixes
  (e.g. ``cnames``, ``rpods``) would reduce the probability of collisions.


Performance
===========

Indexing can be a CPU- & RAM-consuming operation.
The data structures behind indices are chosen to be as efficient as possible:

* The index's lookups are O(1) --- as in Python's ``dict``.
* The store's updates/deletions are O(1) -- a ``dict`` is used internally.
* The overall updates/deletions are O(k), where "k" is the number of keys
  per object (not of all keys!), which is fixed in most cases, so it is O(1).

Neither the number of values stored in the index nor the overall amount of keys
affect its performance (in theory).

Some performance can be lost on additional method calls of the user-facing
mappings/collections made to hide the internal ``dict`` structures.
It is assumed to be negligible compared to the overall code overhead.


Guarantees
==========

If an index is declared, there is no need to additionally pre-check for its
existence --- the index exists immediately even if it contains no resources.

The indices are guaranteed to be fully pre-populated before any other
resource-related handlers are invoked in the operator.
As such, even the on-creation handlers or raw event handlers are guaranteed
to have the complete indexed overview of the cluster,
not just partially populated to the moment when they happened to be triggered.

There is no such guarantee for the operator handlers, such as startup/cleanup,
authentication, health probing, and for the indexing functions themselves:
the indices are available in kwargs but can be empty or partially populated
in the operator's startup and index pre-population stage. This can affect
the cleanup/login/probe handlers if they are invoked at that stage.

Though, the indices are safe to be passed to threads/tasks for later processing
if such threads/tasks are started from the before-mentioned startup handlers.


Limitations
===========

All in-memory values are lost on operator restarts; there is no persistence.
In particular, the indices are fully recalculated on operator restarts during
the initial listing of the resources (equivalent to ``@kopf.on.event``).

On large clusters with thousands of resources, the initial index population
can take time, so the operator's processing will be delayed regardless of
whether the handlers do use the indices or they do not (the framework cannot
know this for sure).

.. seealso::

    :doc:`/memos` --- other in-memory structures with similar limitations.

.. seealso::

    Indexers and indices are conceptually similar to `client-go's indexers`__
    -- with all the underlying components implemented inside of the framework
    ("batteries included").

    __ https://github.com/kubernetes/sample-controller/blob/master/docs/controller-client-go.md
