# Kopf example with dynamic sub-handlers

It is convenient to re-use the framework's capabilities to track
the handler execution, to skip the finished or failed handlers,
and to retry to recoverable errors -- without the reimplemenation
of the same logic inside of the handlers.

In some cases, however, the required handlers can be identified
only at the handling time, mostly when they are based on the spec,
or on some external environment (databases, remote APIs, other objects).

For this case, the sub-handlers can be useful. The sub-handlers "extend"
the main handler, inside of which they are defined, but delegate
the progress tracking to the framework.

In all aspects, the sub-handler are the same as other handlers:
the same function signatures, the same execution environment,
the same error handling, etc.

Start the operator:

```bash
kopf run example.py --verbose
```

Trigger the object creation and monitor the stderr of the operator:

```bash
$ kubectl apply -f ../obj.yaml
```

Observe how the sub-handlers are nested within the parent handler,
and use the `spec.items` to dynamically decide how many and which
sub-handlers must be executed.

```
[2019-02-19 16:05:56,432] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] First appearance: ...
[2019-02-19 16:05:56,432] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Adding the finalizer, thus preventing the actual deletion.

[2019-02-19 16:05:56,645] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Creation event: ...
[2019-02-19 16:05:56,650] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Invoking handler create_fn.
[2019-02-19 16:05:56,654] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Invoking handler create_fn/item1.

=== Handling creation for item1. ===

[2019-02-19 16:05:56,656] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler create_fn/item1 succeeded.
[2019-02-19 16:05:56,982] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler create_fn has unfinished sub-handlers. Will retry soon.

[2019-02-19 16:05:57,200] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Creation event: ...
[2019-02-19 16:05:57,201] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Invoking handler create_fn.
[2019-02-19 16:05:57,203] kopf.reactor.handlin [DEBUG   ] [default/kopf-example-1] Invoking handler create_fn/item2.

=== Handling creation for item2. ===

[2019-02-19 16:05:57,208] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler create_fn/item2 succeeded.
[2019-02-19 16:05:57,419] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] Handler create_fn succeeded.

[2019-02-19 16:05:57,634] kopf.reactor.handlin [INFO    ] [default/kopf-example-1] All handlers succeeded for creation.
```

Try creating the object with more items in it to see more sub-handlers
executed (note: do not change it, but re-create it, as only the creation handler
is implemented in this example; or implement the update handler yourselves).

Cleanup in the end:

```bash
$ kubectl delete -f ../obj.yaml
```
