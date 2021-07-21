# Kopf example for embedded operator

Kopf operators can be embedded into arbitrary applications, such as UI;
or they can be orchestrated explicitly by the developers instead of `kopf run`.

In this example, we start the operator in a side thread, while simulating
an application activity in the main thread. In this case, the "application"
just creates and deletes the example objects, but it can be any activity.

Start the operator:

```bash
python example.py
```

Let it run for 6 seconds (mostly due to sleeps: 3 times by 1+1 second).
Here is what it will print (shortened; the actual output is more verbose):

```
Starting the main app.

[DEBUG   ] Pykube is configured via kubeconfig file.
[DEBUG   ] Client is configured via kubeconfig file.
[WARNING ] Default peering object is not found, falling back to the standalone mode.
[WARNING ] OS signals are ignored: running not in the main thread.

Do the main app activity here. Step 1/3.

[DEBUG   ] [default/kopf-example-0] Creation is in progress: ...
[DEBUG   ] [default/kopf-example-0] Deletion is in progress: ...

Do the main app activity here. Step 2/3.

[DEBUG   ] [default/kopf-example-1] Creation is in progress: ...
[DEBUG   ] [default/kopf-example-1] Deletion is in progress: ...

Do the main app activity here. Step 3/3.

[DEBUG   ] [default/kopf-example-2] Creation is in progress: ...
[DEBUG   ] [default/kopf-example-2] Deletion is in progress: ...

Exiting the main app.

[INFO    ] Stop-flag is set to True. Operator is stopping.
[DEBUG   ] Root task 'poster of events' is cancelled.
[DEBUG   ] Root task 'watcher of kopfexamples.kopf.dev' is cancelled.
[DEBUG   ] Root tasks are stopped: finished normally; tasks left: set()
[DEBUG   ] Hung tasks stopping is skipped: no tasks given.
```
