======
Timers
======

Timers are schedules of regular handler execution as long as the object exists,
no matter if there were any changes or not -- unlike the regular handlers,
which are event-driven and are triggered only when something changes.


Intervals
=========

The interval defines how often to trigger the handler (in seconds):

.. code-block:: python

    import asyncio
    import time
    import kopf

    @kopf.timer('kopfexamples', interval=1.0)
    def ping_kex(spec, **kwargs):
        pass


Sharpness
=========

Usually (by default), the timers are invoked with the specified interval
between each call. The time taken by the handler itself is not taken into
account. It is possible to define timers with sharp schedule: i.e. invoked
every number of seconds sharp, no matter how long it takes to execute it:

.. code-block:: python

    import asyncio
    import time
    import kopf

    @kopf.timer('kopfexamples', interval=1.0, sharp=True)
    def ping_kex(spec, **kwargs):
        time.sleep(0.3)

In this example, the timer takes 0.3 seconds to execute. The actual interval
between the timers will be 0.7 seconds in the sharp mode: whatever is left
of the declared interval of 1.0 seconds minus the execution time.


Idling
======

Timers can be defined to idle if the resource changes too often, and only
be invoked when it is stable for some time:

.. code-block:: python

    import asyncio
    import kopf

    @kopf.timer('kopfexamples', idle=10)
    def ping_kex(spec, **kwargs):
        print(f"FIELD={spec['field']}")

Creation of a resource is considered as a change, so idling also shifts
the very first invocation by that time.

The default is to have no idle time, just the intervals.

It is possible to have a timer with both idling and interval. In that case,
the timer will be invoked only if there were no changes for specified duration
(idle), and every N seconds after that (interval), as long as the object does
not change. Once changed, the timer will stop and wait for the new idling time:

.. code-block:: python

    import asyncio
    import kopf

    @kopf.timer('kopfexamples', idle=10, interval=1)
    def ping_kex(spec, **kwargs):
        print(f"FIELD={spec['field']}")


Postponing
==========

Normally, timers are invoked immediately once resource becomes visible
to the operator (unless idling is declared).

It is possible to postpone the invocations:

.. code-block:: python

    import asyncio
    import time
    import kopf

    @kopf.timer('kopfexamples', interval=1, initial_delay=5)
    def ping_kex(spec, **kwargs):
        print(f"FIELD={spec['field']}")

This is similar to idling, except that it is applied only once per
resource/operator lifecycle in the very beginning.


Combined timing
===============

It is possible to combine all schedule intervals to achieve the desired effect.
For example, to give an operator 1 minute for warming up, and then pinging
the resources every 10 seconds if they are unmodified for 10 minutes:

.. code-block:: python

    import kopf

    @kopf.timer('kopfexamples',
                initial_delay=60, interval=10, idle=600)
    def ping_kex(spec, **kwargs):
        pass


Errors in timers
================

The timers follow the standard :doc:`error handling <errors>` protocol:
:class:`TemporaryError` and arbitrary exceptions are treated according to
the ``errors``, ``timeout``, ``retries``, ``backoff`` options of the handler.
The kwargs :kwarg:`retry`, :kwarg:`started`, :kwarg:`runtime` are provided too.

The default behaviour is to retry arbitrary error
(similar to the regular resource handlers).

When an error happens, its delay overrides the timer's schedule or life cycle:

* For arbitrary exceptions, the timer's ``backoff=...`` option is used.
* For `kopf.TemporaryError`, the error's ``delay=...`` option is used.
* For `kopf.PermanentError`, the timer stops forever and is never retried.

The timer's own interval is only used if the function exits successfully.

For example, if the handler fails 3 times with a back-off time set to 5 seconds
and the interval set to 10 seconds, it will take 25 seconds (``3*5+10``)
from the first execution to the end of the retrying cycle:

.. code-block:: python

    import kopf

    @kopf.timer('kopfexamples',
                errors=kopf.ErrorsMode.TEMPORARY, interval=10, backoff=5)
    def monitor_kex_by_time(name, retry, **kwargs):
        if retry < 3:
            raise Exception()

It will be executed in that order:

* A new cycle begins:
  * 1st execution attempt fails (``retry == 0``).
  * Waits for 5 seconds (``backoff``).
  * 2nd execution attempt fails (``retry == 1``).
  * Waits for 5 seconds (``backoff``).
  * 3rd execution attempt fails (``retry == 2``).
  * Waits for 5 seconds (``backoff``).
  * 4th execution attempt succeeds (``retry == 3``).
  * Waits for 10 seconds (``interval``).
* A new cycle begins:
  * 5th execution attempt fails (``retry == 0``).

The timer never overlaps with itself. Though, multiple timers with
different interval settings and execution schedules can eventually overlap
with each other and with event-driven handlers.


Results delivery
================

The timers follow the standard :doc:`results delivery <results>` protocol:
the returned values are put on the object's status under the handler's id
as a key.

.. code-block:: python

    import random
    import kopf

    @kopf.timer('kopfexamples', interval=10)
    def ping_kex(spec, **kwargs):
        return random.randint(0, 100)

.. note::

    Whenever a resulting value is serialised and put on the resource's status,
    it modifies the resource, which, in turn, resets the idle timer.
    Use carefully with both idling & returned results.


Filtering
=========

It is also possible to use the existing :doc:`filters`:

.. code-block:: python

    import kopf

    @kopf.timer('kopfexamples', interval=10,
                annotations={'some-annotation': 'some-value'},
                labels={'some-label': 'some-value'},
                when=lambda name, **_: 'some' in name)
    def ping_kex(spec, **kwargs):
        pass


System resources
================

.. warning::

    Timers are implemented the same way as asynchronous daemons
    (see :doc:`daemons`) — via asyncio tasks for every resource & handler.

    Despite OS threads are not involved until the synchronous functions
    are invoked (through the asyncio executors), this can lead to significant
    OS resource usage on large clusters with thousands of resources.

    Make sure you only have daemons and timers with appropriate filters
    (e.g., by labels, annotations, or so).
