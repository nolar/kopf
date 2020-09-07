#!/bin/bash
# Install the Kubernetes client libraries for Python.
#
# They are used only in a few (maybe one) CI job, so there is no need to put
# them as the framework's dependencies. However, it is needed to smoke-test
# the framework for compatibility with the clients.
#
set -eu
set -x

if [[ "${CLIENT:-}" ]] ; then
    pip install kubernetes
fi

if [[ "${PYKUBE:-}" ]] ; then
    pip install pykube-ng
fi
