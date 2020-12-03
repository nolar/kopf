"""
General-purpose helpers not related to the framework itself
(neither to the reactor nor to the engines nor to the structs),
which are used to prepare and control the runtime environment.

These are things that should better be in the standard library
or in the dependencies.

Utilities do not depend on anything in the framework. For most cases,
they do not even implement any entities or behaviours of the domain
of K8s Operators, but rather some unrelated low-level patterns.
"""
