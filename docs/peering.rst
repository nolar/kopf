=======
Peering
=======

All running operators communicate with each other via peering objects
(an additional kind of custom resources), so they know about each other.

This allows several multi-instance setups for redundancy or workload management.


Priorities
==========

Each operator has a priority (the default is 0). Whenever the operator
notices that other operators start with a higher priority, it pauses
its operation until those operators stop working.

This is done to prevent collisions of multiple operators handling
the same objects. If two operators run with the same priority, all operators
issue a warning and freeze, leaving the cluster unserved.

To set the operator's priority, use :option:`--priority`:

.. code-block:: bash

    kopf run --priority=100 ...

Or:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_: Any) -> None:
        settings.peering.priority = 100

As a shortcut, there is a :option:`--dev` option, which sets
the priority to ``666``, and is intended for the development mode.


Scopes
======

There are two types of custom resources used for peering:

* ``ClusterKopfPeering`` for the cluster-scoped operators.
* ``KopfPeering`` for the namespace-scoped operators.

Kopf automatically chooses which one to use, depending on whether
the operator is restricted to a namespace with :option:`--namespace`,
or it is running cluster-wide with :option:`--all-namespaces`.

Create a peering object as needed with one of:

.. code-block:: yaml

    apiVersion: kopf.dev/v1
    kind: ClusterKopfPeering
    metadata:
      name: example

.. code-block:: yaml

    apiVersion: kopf.dev/v1
    kind: KopfPeering
    metadata:
      namespace: default
      name: example

.. note::

    In ``kopf<0.11`` (until May 2019), ``KopfPeering`` was the only CRD,
    and it was cluster-scoped. In ``kopf>=0.11,<1.29`` (until Dec 2020),
    this mode was deprecated but supported if the old CRD existed.
    Since ``kopf>=1.29`` (Jan 2021), it is not supported anymore.
    To upgrade, delete and re-create the peering CRDs to the new ones.

.. note::

    In ``kopf<1.29``, all peering CRDs used the API group ``kopf.zalando.org``.
    Since ``kopf>=1.29`` (Jan'2021), they belong to the API group ``kopf.dev``.

    At runtime, both API groups are supported. However, these resources
    of different API groups are mutually exclusive and cannot co-exist
    in the same cluster since they use the same names. Whenever possible,
    re-create them with the new API group after the operator/framework upgrade.


Custom peering
==============

The operator can be instructed to use alternative peering objects:

.. code-block:: bash

    kopf run --peering=example ...
    kopf run --peering=example --namespace=some-ns ...

Or:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_: Any) -> None:
        settings.peering.name = "example"
        settings.peering.mandatory = True

Depending on :option:`--namespace` or :option:`--all-namespaces`,
either ``ClusterKopfPeering`` or ``KopfPeering`` will be used automatically.

If the peering object does not exist, the operator will pause at the start.
Using :option:`--peering` assumes that the peering is mandatory.

Note that in the startup handler, this is not the same:
the mandatory mode must be set explicitly. Otherwise, the operator will try
to auto-detect the presence of the custom peering object, but will not pause
if it is absent --- unlike with the ``--peering=`` CLI option.

The operators from different peering objects do not see each other.

This is especially useful for cluster-scoped operators handling different
resource kinds, which should not be concerned with operators for other kinds.


Standalone mode
===============

To prevent an operator from peering and talking to other operators,
the standalone mode can be enabled:

.. code-block:: bash

    kopf run --standalone ...

Or:

.. code-block:: python

    import kopf
    from typing import Any

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_: Any) -> None:
        settings.peering.standalone = True

In that case, the operator will not pause if other operators with
a higher priority start handling the objects, which may lead
to conflicting changes and reactions from multiple operators
for the same events.


Automatic peering
=================

If there is a peering object detected with the name ``default``
(either cluster-scoped or namespace-scoped),
then it is used by default as the peering object.

Otherwise, Kopf will run the operator in the standalone mode.


Multi-pod operators
===================

Fallback operators
------------------

Usually, one and only one operator instance should be deployed per resource type.
If that operator's pod dies, handling of resources of that type
will stop until the operator's pod is restarted (if it is restarted at all).

To start multiple operator pods, they must be distinctly prioritized.
In that case, only one operator will be active --- the one with the highest
priority. All other operators will pause and wait until this operator exits.
Once it exits, the second-highest priority operator will come into play.
And so on.

To achieve this, assign a monotonically increasing or random priority to each
operator in the deployment or replicaset:

.. code-block:: bash

    kopf run --priority=$RANDOM ...

Or:

.. code-block:: python

    import kopf
    import random
    from typing import Any

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_: Any) -> None:
        settings.peering.priority = random.randint(0, 32767)

``$RANDOM`` is a bash feature
(if you use another shell, see its man page for an equivalent).
It returns a random integer in the range 0..32767.
With high probability, 2–3 pods will get unique priorities.

You can also use the pod's IP address in its numeric form as the priority,
or any other source of integers.


Sharded processing
------------------

Kubernetes >= 1.36 (Apr'2026) provides an alpha-feature to split
the list/watch requests into shards. This requires the feature gate
``ShardedListAndWatch`` enabled (disabled by default).

Kopf directly supports shard selectors the same as it supports
field- and label-selectors: it passes the selector through directly
to the listing and watching queries (unmodified, uninterpreted).

Additionally, when there are *any* shards configured, several operator
instances with the same priority do not pause their execution as normally,
but continue working.

With this, multi-instance operators can split the workload between them.

For example, assume that we have the number of shards in the env var ``SHARDS``
and the zero-based shard index of the current operator instance in ``SHARD``.
We can configure it this way to shard the objects by uid:

.. code-block:: python

    from typing import Any
    import os
    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_: Any) -> None:
        settings.peering.priority = 123
        settings.peering.mandatory = True
        shard_cnt = int(os.environ.get('SHARDS', 1))
        shard_idx = int(os.environ.get('SHARD', 0))
        if shard_cnt > 1:
            start = 0x10000000000000000 // shard_cnt * shard_idx
            end = 0x10000000000000000 // shard_cnt * (shard_idx + 1)
            start_str = f"'0x{start:0{16}x}'"  # ensure zeroes!
            end_str = f"'0x{end:0{16}x}'"  # ensure zeroes!
            range_str = f"shardRange(object.metadata.uid,{start_str},{end_str})"
            settings.watching.shard_selectors['pods'] = range_str

    @kopf.on.event('pods')
    def event(namespace: str, name: str, **_: Any) -> None:
        print(f'Seeing pod {namespace}/{name}')

.. note::

    The magic value ``0x10000000000000000`` (2^64) is the size
    of the hash space of the ``shardRange()`` function in Kubernetes.
    Mind that the values must be zero-padded in the API request.

Then we launch a stateful set with 4 instances for this example:

.. code-block:: yaml

    apiVersion: apps/v1
    kind: StatefulSet
    metadata:
      name: my-operator
    spec:
      serviceName: worker
      replicas: 4
      selector:
        matchLabels:
          app: my-operator
      template:
        metadata:
          labels:
            app: my-operator
        spec:
          containers:
            - name: my-operator
              image: my-registry/my-operator:latest
              env:
                - name: SHARD
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.labels['apps.kubernetes.io/pod-index']
                - name: SHARDS
                  value: "4"

As a result, we have 4 pods serving the following ranges of pod uids:

* ``shardRange(object.metadata.uid, '0x0000000000000000', '0x4000000000000000')``
* ``shardRange(object.metadata.uid, '0x4000000000000000', '0x8000000000000000')``
* ``shardRange(object.metadata.uid, '0x8000000000000000', '0xc000000000000000')``
* ``shardRange(object.metadata.uid, '0xc000000000000000', '0x10000000000000000')``

.. warning::

    Kopf does not parse or interpret the shard selectors and does not track
    the shard coverage: neither gaps, nor overlaps.
    It is the responsibility of the operator developer to ensure full coverage
    and the absence of gaps or overlaps for all resources with sharding,
    so as idempotent operations (i.e., no duplicate side-effects)
    for all handled resources with no sharding.
    The latter can be useful for :doc:`indexing`, for example.

.. seealso::

    * https://kubernetes.io/docs/reference/using-api/api-concepts/#sharded-list-and-watch



Stealth keep-alive
==================

Every few seconds (60 by default), the operator sends a keep-alive update
to the chosen peering object, showing that it is still functioning. Other operators
will notice this and decide whether to pause or resume.

The operator also logs keep-alive activity. This can be distracting. To disable it:

.. code-block:: python

    import kopf
    import random
    from typing import Any

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_: Any) -> None:
        settings.peering.stealth = True

There is no equivalent CLI option for that.

Note that this only affects logging. The keep-alive is still sent regardless.
