# Kopf example for testing the filtering of handlers

Kopf has the ability to execute handlers only if the watched objects
match the filters passed to the handler. This includes matching on:
* labels of a resource
* annotations of a resource

Start the operator:

```bash
kopf run example.py
```

Trigger the object creation and monitor the stderr of the operator:

```bash
$ kubectl apply -f ../obj.yaml
```

```
[2019-07-04 14:19:33,393] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Label satisfied.
[2019-07-04 14:19:33,395] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler 'create_with_labels_satisfied' succeeded.
[2019-07-04 14:19:33,648] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Label exists.
[2019-07-04 14:19:33,649] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler 'create_with_labels_exist' succeeded.
[2019-07-04 14:19:33,807] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Annotation satisfied.
[2019-07-04 14:19:33,809] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler 'create_with_annotations_satisfied' succeeded.
[2019-07-04 14:19:33,966] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Annotation exists.
[2019-07-04 14:19:33,967] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler 'create_with_annotations_exist' succeeded.
[2019-07-04 14:19:33,967] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] All handlers succeeded for creation.
```

Here, notice that only the handlers that have labels or annotations that match the applied
object are executed, and the ones that don't, aren't.
