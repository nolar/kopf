===========
Hierarchies
===========

One of the most common operator patterns is to create
child resources in the same Kubernetes cluster.
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
(native or 3rd-party, see below) or a list, tuple, or iterable containing several objects.


Labels
======

To label the resources to be created, use :func:`kopf.label`:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.label(objs, {'label1': 'value1', 'label2': 'value2'})
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'labels': {'label1': 'value1', 'label2': 'value2'}}},
        #  {'kind': 'Deployment',
        #   'metadata': {'labels': {'label1': 'value1', 'label2': 'value2'}}}]


To label the specified resource(s) with the same labels as the resource being
processed, omit the labels or set them to ``None`` (note that this is
not the same as an empty dict ``{}`` --- which is equivalent to doing nothing):

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.label(objs)
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'labels': {'somelabel': 'somevalue'}}},
        #  {'kind': 'Deployment',
        #   'metadata': {'labels': {'somelabel': 'somevalue'}}}]


By default, if any of the requested labels already exist, they will not
be overwritten. To overwrite labels, use ``forced=True``:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
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

For some resources, such as ``Job`` or ``Deployment``, additional fields must
be modified to affect the doubly-nested children (``Pod`` in this case).

To do this, their nested fields must be listed in a ``nested=[...]`` iterable.
If there is only one nested field, it can be passed directly as ``nested='...'``.

If the nested structures are absent in the target resources, they are skipped
and no labels are added. Labels are added only to pre-existing structures:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
        objs = [{'kind': 'Job'}, {'kind': 'Deployment', 'spec': {'template': {}}}]
        kopf.label(objs, {'label1': 'value1'}, nested='spec.template')
        kopf.label(objs, nested='spec.template')
        print(objs)
        # [{'kind': 'Job',
        #   'metadata': {'labels': {'label1': 'value1', 'somelabel': 'somevalue'}}},
        #  {'kind': 'Deployment',
        #   'metadata': {'labels': {'label1': 'value1', 'somelabel': 'somevalue'}},
        #   'spec': {'template': {'metadata': {'labels': {'label1': 'value1', 'somelabel': 'somevalue'}}}}}]

The nested structures are treated as if they were root-level resources, i.e.
they are expected to have the ``metadata`` structure already, or it will be added automatically.

Nested resources are labelled *in addition* to the target resources.
To label only the nested resources without the root resource, pass them
directly to the function (e.g., ``kopf.label(obj['spec']['template'], ...)``).


Owner references
================

Kubernetes natively supports owner references: a child resource
can be marked as "owned" by one or more other resources (owners or parents).
If the owner is deleted, its children will be deleted automatically,
and no additional handlers are needed.

The ``owner`` is a dict containing the fields ``apiVersion``, ``kind``,
``metadata.name``, and ``metadata.uid`` (other fields are ignored).
This is usually the :kwarg:`body` from the handler keyword arguments,
but you can construct your own dict or obtain one from a 3rd-party client library.

To set the ownership, use :func:`kopf.append_owner_reference`.
To remove the ownership, use :func:`kopf.remove_owner_reference`:

.. code-block:: python

    owner = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'pod1', 'uid': '123…'}}
    kopf.append_owner_reference(objs, owner)
    kopf.remove_owner_reference(objs, owner)

To add or remove ownership of the specified resource(s) by the resource currently
being processed, omit the explicit owner argument or set it to ``None``:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
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

To set an owner that is not a controller or does not block owner deletion:

.. code-block:: python

    kopf.append_owner_reference(objs, controller=False, block_owner_deletion=False)

Both of the above are ``True`` by default.

.. seealso::
    :doc:`walkthrough/deletion`.


Names
=====

It is common to name child resources after the parent resource:
either exactly as the parent, or with a random suffix.

To assign a name to resource(s), use :func:`kopf.harmonize_naming`.
If the resource already has its ``metadata.name`` field set, that name will be used.
If it does not, the specified name will be used.
This can be enforced with ``forced=True``:

.. code-block:: python

    kopf.harmonize_naming(objs, 'some-name')
    kopf.harmonize_naming(objs, 'some-name', forced=True)

By default, the specified name is used as a prefix, and a random suffix
is requested from Kubernetes (via ``metadata.generateName``). This is the
most common mode when there are multiple child resources of the same kind.
To ensure an exact name for single-child cases, pass ``strict=True``:

.. code-block:: python

    kopf.harmonize_naming(objs, 'some-name', strict=True)
    kopf.harmonize_naming(objs, 'some-name', strict=True, forced=True)

To align the name of the target resource(s) with the name of the resource
currently being processed, omit the name or set it to ``None``
(both ``strict=True`` and ``forced=True`` are supported in this form too):

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.harmonize_naming(objs, forced=True, strict=True)
        print(objs)
        # [{'kind': 'Job', 'metadata': {'name': 'kopf-example-1'}},
        #  {'kind': 'Deployment', 'metadata': {'name': 'kopf-example-1'}}]

Alternatively, the operator can request Kubernetes to generate a name
with the specified prefix and a random suffix (via ``metadata.generateName``).
The actual name will only be known after the resource is created:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.harmonize_naming(objs)
        print(objs)
        # [{'kind': 'Job', 'metadata': {'generateName': 'kopf-example-1-'}},
        #  {'kind': 'Deployment', 'metadata': {'generateName': 'kopf-example-1-'}}]

Both approaches are commonly used for parent resources that orchestrate multiple
child resources of the same kind (e.g., pods in a deployment).


Namespaces
==========

Typically, child resources are expected to be created in the same
namespace as their parent (unless there are strong reasons to do otherwise).

To set the desired namespace, use :func:`kopf.adjust_namespace`:

.. code-block:: python

    kopf.adjust_namespace(objs, 'namespace')

If the namespace is already set, it will not be overwritten.
To overwrite, pass ``forced=True``:

.. code-block:: python

    kopf.adjust_namespace(objs, 'namespace', forced=True)

To align the namespace of the specified resource(s) with the namespace
of the resource currently being processed, omit the namespace or set it to ``None``:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
        objs = [{'kind': 'Job'}, {'kind': 'Deployment'}]
        kopf.adjust_namespace(objs, forced=True)
        print(objs)
        # [{'kind': 'Job', 'metadata': {'namespace': 'default'}},
        #  {'kind': 'Deployment', 'metadata': {'namespace': 'default'}}]


Adopting
========

All of the above can be done in a single call with :func:`kopf.adopt`; the ``forced``,
``strict``, and ``nested`` flags are passed to all functions that support them:

.. code-block:: python

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
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

All described methods support resource-related classes from selected libraries
in the same way as native Python dictionaries (or any mutable mappings).
Currently, these are `pykube-ng`_ (classes based on ``pykube.objects.APIObject``)
and `kubernetes client`_ (resource models from ``kubernetes.client.models``).

.. code-block:: python

    import kopf
    import pykube
    from typing import Any

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        pod = pykube.objects.Pod(api, {})
        kopf.adopt(pod)

.. code-block:: python

    import kopf
    import kubernetes.client
    from typing import Any

    @kopf.on.create('KopfExample')
    def create_fn(**_: Any) -> None:
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
