======
Scopes
======

Namespaces
==========

An operator can be restricted to handle custom resources in one namespace only::

    kopf run --namespace=some-namespace ...
    kopf run -n some-namespace ...

Multiple namespaces can be served::

    kopf run --namespace=some-namespace --namespace=another-namespace ...
    kopf run -n some-namespace -n another-namespace ...

Namespace globs with ``*`` and ``?`` characters can be used too::

    kopf run --namespace=*-pr-123-* ...
    kopf run -n *-pr-123-* ...

Namespaces can be negated: all namespaces are served except those excluded::

    kopf run --namespace=!*-pr-123-* ...
    kopf run -n !*-pr-123-* ...

Multiple globs can be used in one pattern. The rightmost matching one wins.
The first glob is decisive: if a namespace does not match it, it does not match
the whole pattern regardless of what is there (other globs are not checked).
If the first glob is a negation, it is implied that initially, all namespaces
do match (as if preceded by ``*``), and then the negated ones are excluded.

In this artificial example, ``myapp-live`` will match, ``myapp-pr-456`` will
not match, but ``myapp-pr-123`` will match; ``otherapp-live`` will not match;
even ``otherapp-pr-123`` will not match despite the ``-pr-123`` suffix in it
because it does not match the initial decisive glob::

    kopf run --namespace=myapp-*,!*-pr-*,*-pr-123 ...

In all cases, the operator monitors the namespaces that exist at startup
or are created/deleted at runtime, and starts/stops serving them accordingly.

If there are no permissions to list/watch the namespaces, the operator falls
back to the list of provided namespaces "as is", assuming they exist.
Namespace patterns do not work in this case; only the specific namespaces do
(which means, all namespaces with the ``,*?!`` characters are excluded).

If a namespace does not exist, `Kubernetes permits watching over it anyway`__.
The only difference is when the resource watching starts: if the permissions
are sufficient, the watching starts only after the namespace is created;
if not sufficient, the watching starts immediately (for an unexistent namespace)
and the resources will be actually served once that namespace is created.

__ https://github.com/kubernetes/kubernetes/issues/75537


Cluster-wide
============

To serve the resources in the whole cluster::

    kopf run --all-namespaces ...
    kopf run -A ...

In that case, the operator does not monitor the namespaces in the cluster,
and uses different K8s API URLs to list/watch the objects cluster-wide.
