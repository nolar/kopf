====================
In-memory containers
====================

Kopf provides several ways of storing and exchanging the data in-memory
between handlers and operators.


Resource memos
==============

Every resource handler gets a :kwarg:`memo` kwarg of type :class:`kopf.Memo`.
It is an in-memory container for arbitrary runtime-only keys-values.
The values can be accessed as either object attributes or dictionary keys.

The memo is shared by all handlers of the same individual resource
(not of the resource kind, but a resource object).
If the resource is deleted and re-created with the same name,
the memo is also re-created (technically, it is a new resource).

.. code-block:: python

    import kopf

    @kopf.on.event('KopfExample')
    def pinged(memo: kopf.Memo, **_):
        memo.counter = memo.get('counter', 0) + 1

    @kopf.timer('KopfExample', interval=10)
    def tick(memo: kopf.Memo, logger, **_):
        logger.info(f"{memo.counter} events have been received in 10 seconds.")
        memo.counter = 0


Operator memos
==============

In the operator handlers, such as the operator startup/cleanup, liveness probes,
credentials retrieval, and everything else not specific to resources,
:kwarg:`memo` points to the operator's global container for arbitrary values.

The per-operator container can be either populated in the startup handlers,
or passed from outside of the operator when :doc:`embedding` is used, or both:

.. code-block:: python

    import kopf
    import queue
    import threading

    @kopf.on.startup()
    def start_background_worker(memo: kopf.Memo, **_):
        memo.my_queue = queue.Queue()
        memo.my_thread = threading.Thread(target=background, args=(memo.my_queue,))
        memo.my_thread.start()

    @kopf.on.cleanup()
    def stop_background_worker(memo: kopf.Memo, **_):
        memo['my_queue'].put(None)
        memo['my_thread'].join()

    def background(queue: queue.Queue):
        while True:
            item = queue.get()
            if item is None:
                break
            else:
                print(item)

.. note::

    For code quality and style consistency, it is recommended to use
    the same approach when accessing the stored values.
    The mixed style here is for demonstration purposes only.

The operator's memo is later used to populate the per-resource memos.
All keys & values are shallow-copied into each resource's memo,
where they can be mixed with the per-resource values:

.. code-block:: python

    # ... continued from the previous example.
    @kopf.on.event('KopfExample')
    def pinged(memo: kopf.Memo, namespace: str, name: str, **_):
        if not memo.get('is_seen'):
            memo.my_queue.put(f"{namespace}/{name}")
            memo.is_seen = True

Any changes to the operator's container since the first appearance
of the resource are **not** replicated to the existing resources' containers,
and are not guaranteed to be seen by the new resources (even if they are now).

However, due to shallow copying, the mutable objects (lists, dicts, and even
custom instances of :class:`kopf.Memo` itself) in the operator's container
can be modified from outside, and these changes will be seen in all individual
resource handlers & daemons which use their per-resource containers.


Custom memo classes
===================

For embedded operators (:doc:`/embedding`), it is possible to use any class
for memos. It is not even required to inherit from :class:`kopf.Memo`.

There are 2 strict requirements:

* The class must be supported by all involved handlers that use it.
* The class must support shallow copying via :func:`copy.copy` (``__copy__()``).

The latter is used to create per-resource memos from the operator's memo.
To have one global memo for all individual resources, redefine the class
to return ``self`` when requested to make a copy, as shown below:

.. code-block:: python

    import asyncio
    import dataclasses
    import kopf

    @dataclasses.dataclass()
    class CustomContext:
        create_tpl: str
        delete_tpl: str

        def __copy__(self) -> "CustomContext":
            return self

    @kopf.on.create('kopfexamples')
    def create_fn(memo: CustomContext, **kwargs):
        print(memo.create_tpl.format(**kwargs))

    @kopf.on.delete('kopfexamples')
    def delete_fn(memo: CustomContext, **kwargs):
        print(memo.delete_tpl.format(**kwargs))

    if __name__ == '__main__':
        kopf.configure(verbose=True)
        asyncio.run(kopf.operator(
            memo=CustomContext(
                create_tpl="Hello, {name}!",
                delete_tpl="Good bye, {name}!",
            ),
        ))

In all other regards, the framework does not use memos for its own needs
and passes them through the call stack to the handlers and daemons "as is".

This advanced feature is not available for operators executed via ``kopf run``.


Limitations
===========

All in-memory values are lost on operator restarts; there is no persistence.

The in-memory containers are recommended only for ephemeral objects scoped
to the process lifetime, such as concurrency primitives: locks, tasks, threads…
For persistent values, use the status stanza or annotations of the resources.

Essentially, the operator's memo is not much different from global variables
(unless 2+ embedded operator tasks are running there) or asyncio contextvars,
except that it provides the same interface as for the per-resource memos.

.. seealso::

    :doc:`/indexing` --- other in-memory structures with similar limitations.
