"""
The reactor groups all modules to watch & process the low- & high-level events.

The low-level events are the kubernetes watch streams, received on every
object change, including the metadata, status, etc.

The high-level events are the actually identified changes in the objects,
such as their creation, deletion, update both in general and per-field.
"""
