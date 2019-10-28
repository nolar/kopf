# Kopf example for startup/cleanup handlers

Kopf operators can have handlers invoked on startup and on cleanup.

The startup handlers are slightly different from the module-level code:
the actual tasks (e.g. API calls for watching) are not started until
all the startup handlers succeed.
If the handlers fail, the operator also fails.

The cleanup handlers are executed when the operator exits either by a signal
(e.g. SIGTERM), or by raising the stop-flag, or by cancelling
the operator's asyncio task.
They are not guaranteed to be fully executed if they take too long.

In this example, we start a background task for every pod we see,
and ask that task to finish when the operator exits. It takes some time
for the tasks to notice the request, so the exiting is not instant.

Start the operator:

```bash
kopf run example.py --verbose
```

Observe the startup logs:

```
[18:51:26,535] kopf.reactor.handlin [DEBUG   ] Invoking handler 'startup_fn_simple'.
[18:51:26,536] kopf.reactor.handlin [INFO    ] Initialising the task-lock...
[18:51:26,536] kopf.reactor.handlin [INFO    ] Handler 'startup_fn_simple' succeeded.
[18:51:26,536] kopf.reactor.handlin [DEBUG   ] Invoking handler 'startup_fn_retried'.
[18:51:26,536] kopf.reactor.handlin [ERROR   ] Handler 'startup_fn_retried' failed temporarily: Going to succeed in 3s
[18:51:27,543] kopf.reactor.handlin [DEBUG   ] Invoking handler 'startup_fn_retried'.
[18:51:27,544] kopf.reactor.handlin [ERROR   ] Handler 'startup_fn_retried' failed temporarily: Going to succeed in 2s
[18:51:28,550] kopf.reactor.handlin [DEBUG   ] Invoking handler 'startup_fn_retried'.
[18:51:28,550] kopf.reactor.handlin [ERROR   ] Handler 'startup_fn_retried' failed temporarily: Going to succeed in 1s
[18:51:29,553] kopf.reactor.handlin [DEBUG   ] Invoking handler 'startup_fn_retried'.
[18:51:29,553] kopf.reactor.handlin [INFO    ] Starting retried...
[18:51:29,554] kopf.reactor.handlin [INFO    ] Handler 'startup_fn_retried' succeeded.
[18:51:29,779] kopf.objects         [DEBUG   ] [kube-system/etcd-minikube] Invoking handler 'pod_task'.
[18:51:29,779] kopf.objects         [INFO    ] [kube-system/etcd-minikube] Handler 'pod_task' succeeded.
[18:51:36,784] kopf.objects         [INFO    ] [kube-system/etcd-minikube] Served by the background task.
```

Terminate the operator (depends on your IDE or CLI environment):

```bash
pkill -TERM kopf
```

Observe the cleanup logs (notice the timing — the final exit is not instant):

```
[18:51:44,122] kopf.reactor.running [INFO    ] Signal SIGINT is received. Operator is stopping.
[18:51:44,123] kopf.reactor.running [DEBUG   ] Root task 'poster of events' is cancelled.
[18:51:44,123] kopf.reactor.running [DEBUG   ] Root task 'watcher of pods.' is cancelled.
[18:51:44,128] kopf.reactor.handlin [DEBUG   ] Invoking handler 'cleanup_fn'.
[18:51:44,129] kopf.reactor.handlin [INFO    ] Cleaning up...
[18:51:44,130] kopf.reactor.handlin [INFO    ] All pod-tasks are requested to stop...
[18:51:44,130] kopf.reactor.handlin [INFO    ] Handler 'cleanup_fn' succeeded.
[18:51:44,130] kopf.reactor.running [DEBUG   ] Root tasks are stopped: finished normally; tasks left: set()
[18:51:46,789] kopf.objects         [INFO    ] [kube-system/etcd-minikube] Served by the background task.
[18:51:46,790] kopf.objects         [INFO    ] [kube-system/etcd-minikube] Serving is finished by request.
[18:51:49,136] kopf.reactor.running [DEBUG   ] Hung tasks are stopped: finished normally; tasks left: set()
```
