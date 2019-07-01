"""
All the routines to talk to Kubernetes API and other APIs.

This library is supposed to be mocked when the mocked K8s client is needed,
and only the high-level logic has to be tested, not the API calls themselves.

Beware: this is NOT a Kubernetes client. It is set of dedicated adapters
specially tailored to do the framework-specific tasks, not the generic
Kubernetes object manipulation.

The operators MUST NOT rely on how the framework communicates with the cluster.
Specifically:

Currently, all the routines use the official Kubernetes client.
Eventually, it can be replaced with anything else (e.g. pykube-ng).

Currently, most of the routines are synchronous, i.e. blocking
from the asyncio's point of view. Later, they can be replaced
to async coroutines (if the client supports that),
or put into the asyncio executors (thread pools).
"""
