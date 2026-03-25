# Kopf example for testing the operator

Kopf provides some basic tools to test the Kopf-based operators.
With these tools, the testing frameworks (pytest in this case)
can run the operator-under-test in the background, while the test
performs the resource manipulation.

To run the tests in this directory:

```bash
pip install -r ../requirements.txt
```

```bash
pytest
```

KMock is a supplementary project for running a local mock server for any HTTP API, and for the Kubernetes API in particular --- with extended support for Kubernetes API endpoints, resource discovery, and implicit in-memory object persistence.

* https://kmock.readthedocs.io/
* https://github.com/nolar/kmock
* https://pypi.org/project/kmock/

To speed up tests written fully async (i.e., `async def` tests using `kopf.testing.KopfTask` runner), another library of the same author can be of use: `looptime`, which compacts the event loop's time into near-zero wall-clock time. With this, you can time your tests freely without fears that it will slow down the test suite execution --- it will not.

* https://looptime.readthedocs.io/
* https://github.com/nolar/looptime
* https://pypi.org/project/looptime/
