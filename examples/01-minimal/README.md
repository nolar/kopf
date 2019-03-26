# Kopf minimal example

The minimum codebase needed for to make a runnable Kubernetes operator.

Start the operator:

```bash
kopf run example.py --verbose
```

It does nothing useful, just notices the object creation,
and prints the message to stdout -- can be seen in the operator's output.

In addition, the object's status is updated, as can be seen here:

```bash
$ kubectl apply -f ../obj.yaml
$ kubectl get kopfexamples
NAME             DURATION   CHILDREN   MESSAGE
kopf-example-1   1m                    hello world
```

```bash
$ kubectl describe KopfExample kopf-example-1
Name:         kopf-example-1
Namespace:    default
Labels:       somelabel=somevalue
...
Status:
  Message:  hello world
Events:
  Type    Reason    Age   From  Message
  ----    ------    ----  ----  -------
  Normal  Finished  42s   kopf  All handlers succeeded.
  Normal  Success   43s   kopf  Handler create_fn succeeded.
```

```bash
$ kubectl get KopfExample kopf-example-1 -o yaml
apiVersion: zalando.org/v1
kind: KopfExample
metadata:
  ...
spec:
  duration: 1m
  field: value
  items:
  - item1
  - item2
status:
  message: hello world
```

Cleanup in the end:

```bash
$ kubectl delete -f ../obj.yaml
```
