# Kopf example with multiple processes and development mode

When multiple operators start for the same cluster (in the cluster or outside),
they become aware about each other, and exchange the basic information about
their liveliness and the priorities, and cooperate to avoid the undesired
side-effects (e.g., duplicated children creation, infinite cross-changes).

The main use-case for this is the development mode: when a developer starts
an operator on their workstation, all the deployed operators should freeze
and stop processing of the objects, until the developer's operator exits.

In shell A, start an operator:

```bash
kopf run example.py --verbose
```

In shell B, start another operator:

```bash
kopf run example.py --verbose
```

Notice how both A & B complain about the same-priority sibling operator:

```
[2019-02-05 20:42:39,052] kopf.peering         [WARNING ] Possibly conflicting operators with the same priority: [Peer(089e5a18a71d4660b07ae37acc776250, priority=0, lastseen=2019-02-05 19:42:38.932613, lifetime=0:01:00)].
```

```
[2019-02-05 20:42:39,223] kopf.peering         [WARNING ] Possibly conflicting operators with the same priority: [Peer(590581cbceff403e90a3e874379c4daf, priority=0, lastseen=2019-02-05 19:42:23.241150, lifetime=0:01:00)].
```

Now, stop the operator B wtih Ctrl+C (twice), and start it with `--dev` option
(equivalent to `--priority 666`):

```bash
kopf run example.py --verbose --dev
```

Observe how the operator A freezes and lets
operator B to take control over the objects.

```
[2019-02-05 20:43:40,360] kopf.peering         [INFO    ] Freezing operations in favour of [Peer(54e7054f28d948c4985db79410c9ef4a, priority=666, lastseen=2019-02-05 19:43:40.166561, lifetime=0:01:00)].
```

Stop the operator B again with Ctrl+C (twice).
The operator A resumes its operations:

```
[2019-02-05 20:44:54,311] kopf.peering         [INFO    ] Resuming operations after the freeze.
```

The same can be achieved with the explicit CLI commands:

```bash
kopf freeze --lifetime 60 --priority 100
kopf resume
```

```
[2019-02-05 20:45:34,354] kopf.peering         [INFO    ] Freezing operations in favour of [Peer(manual, priority=100, lastseen=2019-02-05 19:45:34.226070, lifetime=0:01:00)].
[2019-02-05 20:45:49,427] kopf.peering         [INFO    ] Resuming operations after the freeze.
```
