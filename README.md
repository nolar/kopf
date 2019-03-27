# Kubernetes Operator Pythonic Framework (Kopf)

**Kopf** -- "Kubernetes Operator Pythonic Framework" -- a framework and a library
to make Kubernetes operators development easier, just in few lines of Python code. 

The main goal is to bring the Domain-Driven Design to the infrastructure level,
with Kubernetes being an orchestrator/database of the domain objects (custom resources),
and the operators containing the domain logic (with no or minimal infrastructure logic).


## Features:

* A full-featured operator in just 2 files: `Dockerfile` + a Python module.
* Implicit object's status updates, as returned from the Python functions.
* Multiple creation/update/deletion handlers to track the object handling process.
* Update handlers for the selected fields with automatic value diffs.
* Dynamically generated sub-handlers using the same handling tracking feature.
* Retries of the handlers in case of failures or exceptions.
* Easy object hierarchy building with the labels/naming propagation.
* Built-in _events_ for the objects to reflect their state (as seen in `kubectl describe`).
* Automatic logging/reporting of the handling process (as logs + _events_).
* Handling of multiple CRDs in one process.
* The development instance temporarily suppresses the deployed ones.

Not yet, todos:

* ~~Multiple operators are automatically synced to avoid double-processing.~~
* ~~CLI commands to investigate the status of the handled objects in the cluster.~~


## Examples

See `./examples/` for the examples of the typical use-cases.

The minimalistic operator can look like this:

```python
import kopf

@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(spec, meta, status, **kwargs):
    print(f"And here we are! Creating: {spec}")
```

The keyword arguments available to the handlers:

* `body` for the whole body of the handled objects.
* `spec` as an alias for `body['spec']`.
* `meta` as an alias for `body['metadata']`.
* `status` as an alias for `body['status']`.
* `patch` is a dict with the object changes to applied after the handler.
* `retry` (`int`) is the sequential number of retry of this handler.
* `started` (`datetime.datetime`) is the start time of the handler, in case of retries & errors.
* `runtime` (`datetime.timedelat`) is the duration of the handler run, in case of retries & errors.
* `diff` is a list of changes of the object (only for the update events).
* `old` is the old state of the object or a field (only for the update events).
* `new` is the new state of the object or a field (only for the update events).
* `logger` is a per-object logger, with the messages prefixed with the object's namespace/name.
* `event` is the raw event as received from the Kubernetes API.
* `cause` is the processed cause of the handler as detected by the framework (create/update/delete).

`**kwargs` (or `**_` to stop lint warnings) is required for the forward
compatibility: the framework can add new keyword arguments in the future,
and the existing handlers should accept them.


## Usage

### In-cluster

We assume that when the operator is executed in the cluster, it must be packaged
into a docker image with CI/CD tool of your preference.

```bash
FROM python:3.7
ADD . /src
RUN pip install kopf
CMD kopf run handlers.py
```

Where `handlers.py` is your Python script with the handlers
(see `examples/*/example.py` for the examples).

See `kopf run --help` for others ways of attaching the handlers.


## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details
on our process for submitting pull requests to us, and please ensure
you follow the [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

To install the environment for the local development,
read [DEVELOPMENT.md](DEVELOPMENT.md).


## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available,
see the [tags on this repository](https://github.com/zalando-incubator/kopf/tags). 


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details


## Acknowledgments

* Thanks to [@side8](https://github.com/side8) and their [k8s-operator](https://github.com/side8/k8s-operator)
  for the inspiration.
