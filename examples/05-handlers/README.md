# Kopf example with multiple handlers

Multiple handlers can be registered for the same event.
They are executed in the order of registration.

Beside the stardard create-update-delete events, a per-field diff can be registered.
It is called only in case of the specified field changes,
with `old` & `new` set to that field's values.

Start the operator (we skip the verbose mode here, for clarity):

```bash
kopf run example.py
```

Trigger the object creation and monitor the stderr of the operator:

```bash
$ kubectl apply -f ../obj.yaml
```

```
CREATED 1st
[2019-02-05 20:33:50,336] kopf.handling        [INFO    ] [default/kopf-example-1] Handler create_fn_1 succeeded.
CREATED 2nd
[2019-02-05 20:33:50,557] kopf.handling        [INFO    ] [default/kopf-example-1] Handler create_fn_2 succeeded.
[2019-02-05 20:33:50,781] kopf.handling        [INFO    ] [default/kopf-example-1] All handlers succeeded.
```

Now, trigger the object change:

```bash
$ kubectl patch -f ../obj.yaml --type merge -p '{"spec": {"field": "newvalue", "newfield": 100}}'
```

```
UPDATED
[2019-02-05 20:34:06,358] kopf.handling        [INFO    ] [default/kopf-example-1] Handler update_fn succeeded.
FIELD CHANGED: value -> newvalue
[2019-02-05 20:34:06,682] kopf.handling        [INFO    ] [default/kopf-example-1] Handler field_fn/spec.field succeeded.
[2019-02-05 20:34:06,903] kopf.handling        [INFO    ] [default/kopf-example-1] All handlers succeeded.
```

Finally, delete the object:

```bash
$ kubectl delete -f ../obj.yaml 
```

```
DELETED 1st
[2019-02-05 20:34:42,496] kopf.handling        [INFO    ] [default/kopf-example-1] Handler delete_fn_1 succeeded.
DELETED 2nd
[2019-02-05 20:34:42,715] kopf.handling        [INFO    ] [default/kopf-example-1] Handler delete_fn_2 succeeded.
[2019-02-05 20:34:42,934] kopf.handling        [INFO    ] [default/kopf-example-1] All handlers succeeded.
```
