# Kopf example with children

This example creates a `Pod` for every created `KopfExample` object,
and attaches it as a child of that example object. The latter means that
when the parent object is deleted, the child pod is also terminated.

Start the operator:

```bash
kopf run example.py --verbose
```

The child pod's id is stored as the parent's status field,
so that it can be seen on the object listing (see also `crd.yaml`):

```bash
$ kubectl apply -f ../obj.yaml
$ kubectl get kopfexamples
NAME             FIELD   CHILDREN
kopf-example-1   value   [aed7f7ac-2971-11e9-b4d3-061441377794]

$ kubectl get pod -l somelabel=somevalue
NAME                   READY   STATUS    RESTARTS   AGE
kopf-example-1-jvlfs   1/1     Running   0          26s
```

```bash
$ kubectl delete -f ../obj.yaml
$ kubectl get pod -l somelabel=somevalue
NAME                   READY   STATUS        RESTARTS   AGE
kopf-example-1-jvlfs   1/1     Terminating   0          52s
```

Cleanup in the end:

```bash
$ kubectl delete -f ../obj.yaml
```
