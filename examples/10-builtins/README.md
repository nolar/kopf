# Kopf example for built-in resources

Kopf can also handle the built-in resources, such as Pods, Jobs, etc.

In this example, we take control all over the pods (namespaced/cluster-wide),
and allow the pods to exist for no longer than 30 seconds --
either after creation or after the operator restart.

For no specific reason, just for fun. Maybe, as a way of Chaos Engineering
to force making the resilient applications (tolerant to pod killing).

However, the system namespaces (kube-system, etc) are explicitly protected --
to prevent killing the cluster itself.

Start the operator:

```bash
kopf run example.py --verbose
```

Start a sample pod:

```bash
kubectl run -it --image=ubuntu expr1 -- bash -i
# wait for 30s
```

Since `kubectl run` creates a Deployment, not just a Pod,
a new pod will be created every 30 seconds. Observe with:

```bash
kubectl get pods --watch
```

*Please note that Kopf puts a finalizer on the managed resources,
so the pod deletion will be blocked unless the operator is running
(to remove the finalizer). This will be made optional in #24.* 

Cleanup in the end:

```bash
$ kubectl delete deployment expr1
```
