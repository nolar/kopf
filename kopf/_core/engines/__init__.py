"""
Engines are things that run around the reactor (see :mod:`kopf._core.reactor`)
to help it function at full strength, but are not part of it.
For example, all ever-running side tasks for peering and k8s-event-posting.

The reactor and engines exchange state with each other
via the provided synchronization objects, usually asyncio events & queues.
"""
