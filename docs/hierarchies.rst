===========
Hierarchies
===========

One of the most common patterns of the operators is to create
children resources in the same Kubernetes cluster.
Kopf provides some tools to simplify connecting these resources
by manipulating their content before it is sent to the Kubernetes API.

.. note::

    Kopf is not a Kubernetes client library.
    It does not provide any means to manipulate the Kubernetes resources
    in the cluster or to directly talk to the Kubernetes API in any other way.
    Use any of the existing libraries for that purpose,
    such as the official `kubernetes client`_, pykorm_, or pykube-ng_.

.. _kubernetes client: https://github.com/kubernetes-client/python
.. _pykorm: https://github.com/Frankkkkk/pykorm
.. _pykube-ng: https://github.com/hjacobs/pykube

In all examples below, ``obj`` and ``objs`` are either a supported object type
(native or 3rd-party, see below) or a list/tuple/iterable with several objects.


Labels
======

To label the resources to be created, use :func:`kopf.label`:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.label(objs, {'label1': 'value1', 'label2': 'value2'})
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'labels': {'label1': 'value1', 'label2': 'value2'}}},
        #  {'kind': 'Deployment',
        #   'metadata': {'labels': {'label1': 'value1', 'label2': 'value2'}}}]


To label the specified resource(s) with the same labels as the resource being
processed at the moment, omit the labels or set them to ``None`` (note, it is
not the same as an empty dict ``{}`` -- which is equivalent to doing nothing):

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.label(objs)
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'labels': {'somelabel': 'somevalue'}}},
        #  {'kind': 'Deployment',
        #   'metadata': {'labels': {'somelabel': 'somevalue'}}}]


By default, if some of the requested labels already exist, they will not
be overwritten. To overwrite labels, use ``forced=True``:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.label(objs, {'label1': 'value1', 'somelabel': 'not-this'}, forced=True)
        kopf.label(objs, forced=True)
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'labels': {'label1': 'value1', 'somelabel': 'somevalue'}}},
        #  {'kind': 'Deployment',
        #   'metadata': {'labels': {'label1': 'value1', 'somelabel': 'somevalue'}}}]


Nested labels
=============

For some resources, e.g. ``Job`` or ``Deployment``, additional fields have
to be modified to affect the double-nested children (``Pod`` in this case).

For this, their nested fields must be mentioned in a ``nested=[...]`` iterable.
If this is only one nested field, it can be passed directly as ``nested='...'``.

If the nested structures are absent in the target resources, they are ignored
and no labels are added. The labels are added only to pre-existing structures:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment', 'spec': {'template': {}}}]
        kopf.label(objs, {'label1': 'value1'}, nested='spec.template')
        kopf.label(objs, nested='spec.template')
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'labels': {'label1': 'value1', 'somelabel': 'somevalue'}}},
        #  {'kind': 'Deployment',
        #   'metadata': {'labels': {'label1': 'value1', 'somelabel': 'somevalue'}},
        #   'spec': {'template': {'metadata': {'labels': {'label1': 'value1', 'somelabel': 'somevalue'}}}}}]

The nested structures are treated as if they were the root-level resources, i.e.
they are expected to have or automatically get the ``metadata`` structure added.

The nested resources are labelled *in addition* to the target resources.
To label only the nested resources without the root resource, pass them
to the function directly (e.g., ``kopf.label(obj['spec']['template'], ...)``).


Owner references
================

Kubernetes natively supports the owner references: a child resource
can be marked as "owned" by one or more other resources (owners or parents).
If the owner is deleted, its children will be deleted too, automatically,
and no additional handlers are needed.

The ``owner`` is dict containing the fields ``apiVersion``, ``kind``,
``metadata.name``, ``metadata.uid`` (other fields are ignored).
Usually this can be the :kwarg:`body` from the handler keyword arguments,
but you can construct your own dict or get it from a 3rd-party client library.

To set the ownership, use :func:`kopf.append_owner_reference`.
To remove the ownership, use :func:`kopf.remove_owner_reference`:

.. code-block:: python

    owner = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'pod1', 'uid': '123…'}}
    kopf.append_owner_reference(objs, owner)
    kopf.remove_owner_reference(objs, owner)

To add/remove the ownership of the requested resource(s) by the resource being
processed at the moment, omit the explicit owner argument or set it to ``None``:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.append_owner_reference(objs)
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'ownerReferences': [{'controller': True,
        #      'blockOwnerDeletion': True,
        #      'apiVersion': 'kopf.dev/v1',
        #      'kind': 'KopfExample',
        #      'name': 'kopf-example-1',
        #      'uid': '6b931859-5d50-4b5c-956b-ea2fed0d1058'}]}},
        #  {'kind': 'Deployment',
        #   'metadata': {'ownerReferences': [{'controller': True,
        #      'blockOwnerDeletion': True,
        #      'apiVersion': 'kopf.dev/v1',
        #      'kind': 'KopfExample',
        #      'name': 'kopf-example-1',
        #      'uid': '6b931859-5d50-4b5c-956b-ea2fed0d1058'}]}}]

To set an owner to not be a controller or not block owner deletion:

.. code-block:: python

    kopf.append_owner_reference(objs, controller=False, block_owner_deletion=False)

Both of the above are True by default

.. seealso::
    :doc:`walkthrough/deletion`.


Names
=====

It is common to name the children resources after the parent resource:
either strictly as the parent, or with a random suffix.

To give the resource(s) a name, use :func:`kopf.harmonize_naming`.
If the resource has its ``metadata.name`` field set, that name will be used.
If it does not, the specified name will be used.
It can be enforced with ``forced=True``:

.. code-block:: python

    kopf.harmonize_naming(objs, 'some-name')
    kopf.harmonize_naming(objs, 'some-name', forced=True)

By default, the specified name is used as a prefix, and a random suffix
is requested from Kubernetes (via ``metadata.generateName``). This is the
most widely used mode with multiple children resource of the same kind.
To ensure the exact name for single-child cases, pass ``strict=True``:

.. code-block:: python

    kopf.harmonize_naming(objs, 'some-name', strict=True)
    kopf.harmonize_naming(objs, 'some-name', strict=True, forced=True)

To align the name of the target resource(s) with the name of the resource
being processed at the moment, omit the name or set it to ``None``
(both ``strict=True`` and ``forced=True`` are supported in this form too):

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.harmonize_naming(objs, forced=True, strict=True)
        print(objs)
        # [{'kind': 'Job', 'metadata': {'name': 'kopf-example-1'}},
        #  {'kind': 'Deployment', 'metadata': {'name': 'kopf-example-1'}}]

Alternatively, the operator can request Kubernetes to generate a name
with the specified prefix and a random suffix (via ``metadata.generateName``).
The actual name will be known only after the resource is created:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.harmonize_naming(objs)
        print(objs)
        # [{'kind': 'Job', 'metadata': {'generateName': 'kopf-example-1-'}},
        #  {'kind': 'Deployment', 'metadata': {'generateName': 'kopf-example-1-'}}]

Both ways are commonly used for parent resources that orchestrate multiple
children resources of the same kind (e.g., pods in the deployment).


Namespaces
==========

Usually, it is expected that the children resources are created in the same
namespace as their parent (unless there are strong reasons to do differently).

To set the desired namespace, use :func:`kopf.adjust_namespace`:

.. code-block:: python

    kopf.adjust_namespace(objs, 'namespace')

If the namespace is already set, it will not be overwritten.
To overwrite, pass ``forced=True``:

.. code-block:: python

    kopf.adjust_namespace(objs, 'namespace', forced=True)

To align the namespace of the specified resource(s) with the namespace
of the resource being processed, omit the namespace or set it to ``None``:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.adjust_namespace(objs, forced=True)
        print(objs)
        # [{'kind': 'Job', 'metadata': {'namespace': 'default'}},
        #  {'kind': 'Deployment', 'metadata': {'namespace': 'default'}}]


Adopting
========

All of the above can be done in one call with :func:`kopf.adopt`; ``forced``,
``strict``, ``nested`` flags are passed to all functions that support them:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.adopt(objs, strict=True, forced=True, nested='spec.template')
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'ownerReferences': [{'controller': True,
        #      'blockOwnerDeletion': True,
        #      'apiVersion': 'kopf.dev/v1',
        #      'kind': 'KopfExample',
        #      'name': 'kopf-example-1',
        #      'uid': '4a15f2c2-d558-4b6e-8cf0-00585d823511'}],
        #    'name': 'kopf-example-1',
        #    'namespace': 'default',
        #    'labels': {'somelabel': 'somevalue'}}},
        #  {'kind': 'Deployment',
        #   'metadata': {'ownerReferences': [{'controller': True,
        #      'blockOwnerDeletion': True,
        #      'apiVersion': 'kopf.dev/v1',
        #      'kind': 'KopfExample',
        #      'name': 'kopf-example-1',
        #      'uid': '4a15f2c2-d558-4b6e-8cf0-00585d823511'}],
        #    'name': 'kopf-example-1',
        #    'namespace': 'default',
        #    'labels': {'somelabel': 'somevalue'}}}]


3rd-party libraries
===================

All described methods support resource-related classes of selected libraries
the same way as the native Python dictionaries (or any mutable mappings).
Currently, that is `pykube-ng`_ (classes based on ``pykube.objects.APIObject``)
and `kubernetes client`_ (resource models from ``kubernetes.client.models``).

.. code-block:: python

    import kopf
    import pykube

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        pod = pykube.objects.Pod(api, {})
        kopf.adopt(pod)

.. code-block:: python

    import kopf
    import kubernetes.client

    @kopf.on.create('KopfExample')
    def create_fn(**_):
        pod = kubernetes.client.V1Pod()
        kopf.adopt(pod)
        print(pod)
        # {'api_version': None,
        #  'kind': None,
        #  'metadata': {'annotations': None,
        #               'cluster_name': None,
        #               'creation_timestamp': None,
        #               'deletion_grace_period_seconds': None,
        #               'deletion_timestamp': None,
        #               'finalizers': None,
        #               'generate_name': 'kopf-example-1-',
        #               'generation': None,
        #               'labels': {'somelabel': 'somevalue'},
        #               'managed_fields': None,
        #               'name': None,
        #               'namespace': 'default',
        #               'owner_references': [{'api_version': 'kopf.dev/v1',
        #                                     'block_owner_deletion': True,
        #                                     'controller': True,
        #                                     'kind': 'KopfExample',
        #                                     'name': 'kopf-example-1',
        #                                     'uid': 'a114fa89-e696-4e84-9b80-b29fbccc460c'}],
        #               'resource_version': None,
        #               'self_link': None,
        #               'uid': None},
        #  'spec': None,
        #  'status': None}
