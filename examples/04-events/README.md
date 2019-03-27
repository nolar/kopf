# Kopf example with the event reporting

The framework reports some basic events on the handling progress.
But the developers can report their own events conveniently.

Start the operator:

```bash
kopf run example.py --verbose
```

The events are shown on the object's description
(and are usually garbage-collected after few minutes).

```bash
$ kubectl apply -f ../obj.yaml
$ kubectl describe kopfexample kopf-example-1
...
Events:
  Type      Reason      Age   From  Message
  ----      ------      ----  ----  -------
  Normal    SomeReason  5s    kopf  Some message
  Normal    Success     5s    kopf  Handler create_fn succeeded.
  SomeType  SomeReason  6s    kopf  Some message
  Normal    Finished    5s    kopf  All handlers succeeded.
  Error     SomeReason  5s    kopf  Some exception: Exception text.
  Warning   SomeReason  5s    kopf  Some message

```

Note that the events are shown out of any order -- this is a behaviour of the CLI tool or of the API.
It has nothing to do with the framework: the framework reports the timestamps properly.

Cleanup in the end:

```bash
$ kubectl delete -f ../obj.yaml
```
