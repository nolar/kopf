# Kubernetes Operator Pythonic Framework (Kopf)

[![GitHub](https://img.shields.io/github/stars/nolar/kopf?style=flat&label=GitHub%E2%AD%90%EF%B8%8F)](https://github.com/nolar/kopf)
[![CI](https://github.com/nolar/kopf/actions/workflows/thorough.yaml/badge.svg)](https://github.com/nolar/kopf/actions/workflows/thorough.yaml)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/kopf.svg)](https://pypi.org/project/kopf/)
[![codecov](https://codecov.io/gh/nolar/kopf/branch/main/graph/badge.svg)](https://codecov.io/gh/nolar/kopf)
[![coverage](https://coveralls.io/repos/github/nolar/kopf/badge.svg?branch=main)](https://coveralls.io/github/nolar/kopf?branch=main)

**Kopf** —Kubernetes Operator Pythonic Framework— is a framework and a library
to make Kubernetes operators development easier, just in a few lines of Python code.

The main goal is to bring the Domain-Driven Design to the infrastructure level,
with Kubernetes being an orchestrator/database of the domain objects (custom resources),
and the operators containing the domain logic (with no or minimal infrastructure logic).

The project was originally started as `zalando-incubator/kopf` in March 2019,
and then forked as `nolar/kopf` in August 2020: but it is the same codebase,
the same packages, the same developer(s).

As of now, the project is in maintenance mode since approximately mid-2021:
Python, Kubernetes, CI tooling, dependencies are upgraded, new bugs are fixed,
new versions are released from time to time, but no new big features are added
— there is nothing to add to this project without exploding its scope
beyond the "operator framework" definition (ideas are welcome!).


## Documentation

* https://kopf.readthedocs.io/


## Features

* Simple, but powerful:
  * A full-featured operator in just 2 files: a `Dockerfile` + a Python file (*).
  * Handling functions registered via decorators with a declarative approach.
  * No infrastructure boilerplate code with K8s API communication.
  * Both sync and async handlers, with sync ones being threaded under the hood.
  * Detailed documentation with examples.
* Intuitive mapping of Python concepts to Kubernetes concepts and back:
  * Marshalling of resources' data to the handlers' kwargs.
  * Marshalling of handlers' results to the resources' statuses.
  * Publishing of logging messages as Kubernetes events linked to the resources.
* Support anything that exists in K8s:
  * Custom K8s resources.
  * Builtin K8s resources (pods, namespaces, etc).
  * Multiple resource types in one operator.
  * Both cluster and namespaced operators.
* All the ways of handling that a developer can wish for:
  * Low-level handlers for events received from K8s APIs "as is" (an equivalent of _informers_).
  * High-level handlers for detected causes of changes (creation, updates with diffs, deletion).
  * Handling of selected fields only instead of the whole objects (if needed).
  * Dynamically generated or conditional sub-handlers (an advanced feature).
  * Timers that tick as long as the resource exists, optionally with a delay since the last change.
  * Daemons that run as long as the resource exists (in threads or asyncio-tasks).
  * Validating and mutating admission webhook (with dev-mode tunneling).
  * Live in-memory indexing of resources or their excerpts.
  * Filtering with stealth mode (no logging): by arbitrary filtering functions,
    by labels/annotations with values, presence/absence, or dynamic callbacks.
  * In-memory all-purpose containers to store non-serializable objects for individual resources.
* Eventual consistency of handling:
  * Retrying the handlers in case of arbitrary errors until they succeed.
  * Special exceptions to request a special retry or to never retry again.
  * Custom limits for the number of attempts or the time.
  * Implicit persistence of the progress that survives the operator restarts.
  * Tolerance to restarts and lengthy downtimes: handles the changes afterwards.
* Awareness of other Kopf-based operators:
  * Configurable identities for different Kopf-based operators for the same resource kinds.
  * Avoiding double-processing due to cross-pod awareness of the same operator ("peering").
  * Pausing of a deployed operator when a dev-mode operator runs outside of the cluster.
* Extra toolkits and integrations:
  * Some limited support for object hierarchies with name/labels propagation.
  * Friendly to any K8s client libraries (and is client agnostic).
  * Startup/cleanup operator-level handlers.
  * Liveness probing endpoints and rudimentary metrics exports.
  * Basic testing toolkit for in-memory per-test operator running.
  * Embeddable into other Python applications.
* Highly configurable (to some reasonable extent).

(*) _Small font: two files of the operator itself, plus some amount of
deployment files like RBAC roles, bindings, service accounts, network policies
— everything needed to deploy an application in your specific infrastructure._


## Examples

See [examples](https://github.com/nolar/kopf/tree/main/examples)
for the examples of the typical use-cases.

A minimalistic operator can look like this:

```python
import kopf

@kopf.on.create('kopfexamples')
def create_fn(spec, name, meta, status, **kwargs):
    print(f"And here we are! Created {name} with spec: {spec}")
```

Numerous kwargs are available, such as `body`, `meta`, `spec`, `status`,
`name`, `namespace`, `retry`, `diff`, `old`, `new`, `logger`, etc:
see [Arguments](https://kopf.readthedocs.io/en/latest/kwargs/)

To run a never-exiting function for every resource as long as it exists:

```python
import time
import kopf

@kopf.daemon('kopfexamples')
def my_daemon(spec, stopped, **kwargs):
    while not stopped:
        print(f"Object's spec: {spec}")
        time.sleep(1)
```

Or the same with the timers:

```python
import kopf

@kopf.timer('kopfexamples', interval=1)
def my_timer(spec, **kwargs):
    print(f"Object's spec: {spec}")
```

That easy! For more features, see the [documentation](https://kopf.readthedocs.io/).


## Usage

Python 3.10+ is required:
[CPython](https://www.python.org/) and [PyPy](https://www.pypy.org/)
are officially supported and tested; other Python implementations can work too.

We assume that when the operator is executed in the cluster, it must be packaged
into a docker image with a CI/CD tool of your preference.

```dockerfile
FROM python:3.14
ADD . /src
RUN pip install kopf
CMD kopf run /src/handlers.py --verbose
```

Where `handlers.py` is your Python script with the handlers
(see `examples/*/example.py` for the examples).

See `kopf run --help` for other ways of attaching the handlers.


## Contributing

Please read [CONTRIBUTING.md](https://github.com/nolar/kopf/blob/main/CONTRIBUTING.md)
for details on our process for submitting pull requests to us, and please ensure
you follow the [CODE_OF_CONDUCT.md](https://github.com/nolar/kopf/blob/main/CODE_OF_CONDUCT.md).

To install the environment for the local development,
read [DEVELOPMENT.md](https://github.com/nolar/kopf/blob/main/DEVELOPMENT.md).


## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available,
see the [releases on this repository](https://github.com/nolar/kopf/releases).


## License

This project is licensed under the MIT License —
see the [LICENSE](https://github.com/nolar/kopf/blob/main/LICENSE) file for details.


## Acknowledgments

* Thanks to Zalando for starting this project in Zalando's Open-Source Incubator
  in the first place.
* Thanks to [@side8](https://github.com/side8) and their [k8s-operator](https://github.com/side8/k8s-operator)
  for inspiration.
