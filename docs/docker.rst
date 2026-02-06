============
Docker image
============

Kopf provides pre-built Docker images on the GitHub Container Registry (GHCR)
with all extras pre-installed. These images are intended for quick experimentation
and ad-hoc operator development. For production, it is recommended to build
your own image with ``pip install kopf`` (see :doc:`deployment`).

The images are available at:

.. code-block:: text

    ghcr.io/nolar/kopf

Each image includes the ``kopf`` CLI, the ``full-auth`` extra (``pykube-ng``
and ``kubernetes`` client libraries), ``uvloop`` for better async performance,
and the ``dev`` extra (``oscrypto``, ``certbuilder``, ``certvalidator``,
``pyngrok``) for development convenience.


Image variants
==============

Images are published for each release in several variants:

* **slim** (default) --- based on Debian, larger but with broader compatibility.
* **alpine** --- based on Alpine Linux, smaller but may have issues
  with some native dependencies.

Both variants are built for ``linux/amd64`` and ``linux/arm64`` platforms.


Image tags
==========

For a release such as ``1.42.5`` built with Python 3.14 (the default),
the following tags are available:

* ``ghcr.io/nolar/kopf:latest`` --- the latest release with the default
  Python version and the default variant (slim).
* ``ghcr.io/nolar/kopf:1.42.5`` --- a specific patch release.
* ``ghcr.io/nolar/kopf:1.42`` --- the latest patch within a minor release.
* ``ghcr.io/nolar/kopf:v1`` --- the latest release within a major version.

To pin a specific Python version or variant, use the extended tag format:

* ``ghcr.io/nolar/kopf:1.42.5-python3.13-alpine``
* ``ghcr.io/nolar/kopf:1.42-python3.14-slim``
* ``ghcr.io/nolar/kopf:v1-python3.13``

Tags without a variant suffix (e.g. ``1.42-python3.13``) point to the slim variant.
Tags without a Python version (e.g. ``1.42.5``) point to the default Python version.


Quick start
===========

The simplest way to run an operator is to mount a single Python file
at ``/app/main.py`` inside the container. The entrypoint will auto-detect it
and run ``kopf run /app/main.py``:

.. code-block:: bash

    docker run --rm -it \
        -v ./handler.py:/app/main.py:ro \
        -v ~/.kube/config:/root/.kube/config:ro \
        ghcr.io/nolar/kopf

The kubeconfig mount (``~/.kube/config``) gives the operator access
to the Kubernetes cluster. Adjust the path as needed for your setup.


Running a specific file
=======================

To run an operator from a custom path, pass the ``run`` command
with the path explicitly:

.. code-block:: bash

    docker run --rm -it \
        -v ./src:/src:ro \
        -v ~/.kube/config:/root/.kube/config:ro \
        ghcr.io/nolar/kopf run /src/handler.py


Extra dependencies
==================

If the operator needs additional Python packages, the entrypoint supports
two mechanisms for automatic installation at startup.

requirements.txt
----------------

Mount a ``requirements.txt`` file at ``/app/requirements.txt``.
The entrypoint will run ``pip install -r /app/requirements.txt`` before
starting the operator:

.. code-block:: bash

    docker run --rm -it \
        -v ./handler.py:/app/main.py:ro \
        -v ./requirements.txt:/app/requirements.txt:ro \
        -v ~/.kube/config:/root/.kube/config:ro \
        ghcr.io/nolar/kopf

pyproject.toml
--------------

Mount the entire project directory at ``/app/``. If a ``pyproject.toml`` is
found, the entrypoint will run ``pip install /app/`` to install the project
and its dependencies:

.. code-block:: bash

    docker run --rm -it \
        -v ./myproject:/app:ro \
        -v ~/.kube/config:/root/.kube/config:ro \
        ghcr.io/nolar/kopf


Passing CLI options
===================

Any arguments passed to the container are forwarded directly to the ``kopf``
CLI. For example, to run in verbose mode with a specific namespace:

.. code-block:: bash

    docker run --rm -it \
        -v ./handler.py:/app/main.py:ro \
        -v ~/.kube/config:/root/.kube/config:ro \
        ghcr.io/nolar/kopf run /app/main.py --verbose --namespace=default

To see all available CLI options:

.. code-block:: bash

    docker run --rm ghcr.io/nolar/kopf run --help

.. seealso::
    :doc:`cli` for the full list of command-line options.


Using with Docker Compose
=========================

The image can be used in a Docker Compose setup for local development:

.. code-block:: yaml

    services:
      operator:
        image: ghcr.io/nolar/kopf
        volumes:
          - ./handler.py:/app/main.py:ro
          - ~/.kube/config:/root/.kube/config:ro


Kubeconfig security
===================

Mounting ``~/.kube/config`` directly into the container is convenient but may
expose more credentials than necessary. The default kubeconfig often contains
access tokens or client certificates for multiple clusters and contexts.

For safer local development, create a minified copy that contains only
the current context and review it before mounting:

.. code-block:: bash

    kubectl config view --minify --flatten > dev.kubeconfig
    # Review dev.kubeconfig to ensure it contains only what you expect.

Then mount the minified config instead:

.. code-block:: bash

    docker run --rm -it \
        -v ./handler.py:/app/main.py:ro \
        -v ./dev.kubeconfig:/root/.kube/config:ro \
        ghcr.io/nolar/kopf

This limits the container to a single cluster and avoids accidentally leaking
credentials for other clusters or service accounts. Remember to regenerate
the file when switching contexts or after token rotation.


Building your own image
=======================

For production deployments, it is recommended to build a custom image
rather than relying on the pre-built one. This avoids startup-time dependency
installation, ensures reproducible builds, and keeps the image minimal:

.. code-block:: dockerfile

    FROM python:3.14
    RUN pip install kopf
    COPY handler.py /src/handler.py
    CMD ["kopf", "run", "/src/handler.py", "--verbose"]

.. seealso::
    :doc:`deployment` for the full deployment guide, including RBAC
    and Kubernetes Deployment manifests.
