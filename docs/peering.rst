=======
Peering
=======

All running operators communicate with each other via the peering objects
(also the custom resources), so they know about each other.

The operator can be instructed to use the alternative peering objects::

    kopf run --peering=another ...

The operators from different peering objects do not see each other.

The default peering name (i.e. if no peering or standalone options are provided)
is ``default``.


Standalone mode
---------------

To prevent an operator from peering and talking to other operators,
the standalone mode can be enabled::

    kopf run --standalone ...

In that case, the operator will not freeze of other operators with
the higher priority will start handling the objects, which may lead
to the conflicting changes and reactions from multiple operators
for the same events.
