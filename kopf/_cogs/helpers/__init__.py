"""
General-purpose helpers not related to the framework itself
(neither to the reactor nor to the engines nor to the structs),
which are used to prepare and control the runtime environment.

These are things that should better be in the standard library
or in the dependencies.

Utilities do not depend on anything in the framework. For most cases,
they do not even implement any entities or behaviours of the domain
of K8s Operators, but rather some unrelated low-level patterns.

As a rule of thumb, helpers MUST be abstracted from the framework
to such an extent that they could be extracted as reusable libraries.
If they implement concepts of the framework, they are not "helpers"
(consider making them _kits, structs, engines, or the reactor parts).
"""
