=======
Peering
=======

All running operators communicate with each other via peering objects
(also custom resources), so they know about each other.

The operator can be instructed to use alternative peering objects::

    kopf run --peering=another ...

The operators from different peering objects do not see each other.

Default behavior
----------------

If there is a peering object with name `default` then it's been used by default as the peering object. Otherwise kopf will run the operator in mode `Standalone`.

Standalone mode
---------------

To prevent an operator from peering and talking to other operators,
the standalone mode can be enabled::

    kopf run --standalone ...

In that case, the operator will not freeze of other operators with
the higher priority will start handling the objects, which may lead
to the conflicting changes and reactions from multiple operators
for the same events.
