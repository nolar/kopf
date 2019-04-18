======
Scopes
======

By default, Kopf watches for registered custom resources
in the whole cluster. This may be not desired, for example when:

* The cluster has restrictive permissions by namespaces or by teams.
* Different versions of the operator are deployed into different namespaces
  to serve the custom resources only in those namespaces.


Namespaces
----------

The running operator can be restricted to handle custom resources
in one namespace only::

    kopf run --namespace=some-namespace ...
