# Kopf example with spy-handlers for the raw events

Kopf stores its handler status on the objects' status field.
This can be not desired when the objects do not belong to this operator,
but a probably served by some other operator, and are just watched
by the current operator, e.g. for their status fields.

Event-handlers can be used as the silent spies on the raw events:
they do not store anything on the object, and do not create the k8s-events.

If the event handler fails, the error is logged to the operator's log,
and then ignored.

Please note that the event handlers are invoked for *every* event received
from the watching stream. This also includes the first-time listing when
the operator starts or restarts. It is the developer's responsibility to make
the handlers idempotent (re-executable with do duplicated side-effects).

Start the operator:

```bash
kopf run example.py --verbose
```

Trigger the object creation and monitor the stderr of the operator:

```bash
$ kubectl apply -f ../obj.yaml
```

Observe how the event-handlers are invoked.

```
[2019-05-28 11:03:29,537] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Invoking handler 'event_fn_with_error'.
[2019-05-28 11:03:29,537] kopf.reactor.handlin [ERROR   ] [default/kopf-example-1] Handler 'event_fn_with_error' failed with an exception. Will ignore.
Traceback (most recent call last):
  File ".../kopf/reactor/handling.py", line 159, in handle_event
  File ".../kopf/reactor/invocation.py", line 64, in invoke
  File "example.py", line 6, in event_fn_with_error
    raise Exception("Oops!")
Exception: Oops!

[2019-05-28 11:03:29,541] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Invoking handler 'normal_event_fn'.
Event received: {'type': 'ADDED', 'object': {'apiVersion': 'zalando.org/v1', 'kind': 'KopfExample', ...}
[2019-05-28 11:03:29,541] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler 'normal_event_fn' succeeded.
```

Cleanup in the end:

```bash
$ kubectl delete -f ../obj.yaml
```
