"""
Every reactor needs a storage for its internal fuel.

This package contains classes and functions which are neither structures,
since they have behaviour, nor engines, since they do not run in parallel
with the reactor, nor parts of the reactor, since they are not processes.

The storage classes and functions are utilities with one purpose:
storing and fetching a state. The state can be a structured content,
a JSON-serialised content, or just markers (like finalizers).

All state classes are part of the storage package, even if they are structures:
in order to keep the whole storage mechanics together for cohesion.

For plain data structures reused in all packages, see `kopf.structs`.
For runnable processes, see `kopf.reactor` and `kopf.engines`.
"""
