# Kopf example with exceptions in the handler

This example raises the exceptions in the handler,
so that it is retried few time until succeeded.

Start the operator:

```bash
kopf run example.py --verbose
```

Observe how the exceptions are repored in the operator's log (stderr),
and also briefly reported as the events on the processed object:

```bash
$ kubectl apply -f ../obj.yaml
$ kubectl describe kopfexample kopf-example-1
Name:         kopf-example-1
Namespace:    default
Labels:       somelabel=somevalue
...
Status:
Events:
  Type    Reason       Age   From  Message
  ----    ------       ----  ----  -------
  Error   Exception    9s    kopf  Handler create_fn failed.: First failure.
  Error   MyException  6s    kopf  Handler create_fn failed.: Second failure.
  Normal  Success      4s    kopf  Handler create_fn succeeded.
  Normal  Finished     4s    kopf  All handlers succeeded.
```

Cleanup in the end:

```bash
$ kubectl delete -f ../obj.yaml
```
