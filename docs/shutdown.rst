========
Shutdown
========

The cleanup handlers are executed when the operator exits
either by a signal (e.g. SIGTERM), or by catching an exception,
or by raising the stop-flag, or by cancelling the operator's task
(for :doc:`embedded operators </embedding>`)::

    import kopf

    @kopf.on.cleanup()
    async def cleanup_fn(logger, **kwargs):
        pass

The cleanup handlers are not guaranteed to be fully executed if they take
too long -- due to a limited graceful period or non-graceful termination.

Similarly, the cleanup handlers are not executed if the operator
is force-killed with no possibility to react (e.g. by SIGKILL).

.. note::

    If the operator is running in a Kubernetes cluster, there can be
    timeouts set for graceful termination of a pod
    (``terminationGracePeriodSeconds``, the default is 30 seconds).

    If the cleanup takes longer than that in total (e.g. due to retries),
    the activity will not be finished in full,
    as the pod will be SIGKILL'ed by Kubernetes.

    Either design the cleanup activities to be as fast as possible,
    or configure ``terminationGracePeriodSeconds`` accordingly.

    Kopf itself does not set any implicit timeouts for the cleanup activity,
    and it can continue forever (unless explicitly limited).
