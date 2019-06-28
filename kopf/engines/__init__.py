"""
Engines are things that run around the reactor (see `kopf.reactor`)
to help it to function at full strength, but are not part of it.
For example, all never-ending side-tasks for peering and k8s-event-posting.

The reactor and engines exchange the state with each other (bi-directionally)
via the provided synchronization objects, usually asyncio events & queues.
"""
