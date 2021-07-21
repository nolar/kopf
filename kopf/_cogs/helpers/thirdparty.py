"""
Type definitions from optional 3rd-party libraries, e.g. pykube-ng & kubernetes.

This utility does all the trickery needed to import the libraries if possible,
or to skip them and make typing/runtime dummies for the rest of the codebase.
"""
import abc
from typing import Any, Optional


# Since client libraries are optional, support their objects only if they are installed.
# If not installed, use a dummy class to miss all isinstance() checks for that library.
class _dummy: pass


# Do these imports look excessive? ==> https://github.com/python/mypy/issues/10063
# TL;DR: Strictly `from...import...as...`, AND strictly same-named (`X as X`).
try:
    from pykube.objects import APIObject as APIObject
    PykubeObject = APIObject
except ImportError:
    PykubeObject = _dummy

try:
    from kubernetes.client import V1ObjectMeta as V1ObjectMeta, V1OwnerReference as V1OwnerReference
except ImportError:
    V1ObjectMeta = V1OwnerReference = None


# Kubernetes client does not have any common base classes, its code is fully generated.
# Only recognise classes from a specific module. Ignore all API/HTTP/auth-related tools.
class KubernetesModel(abc.ABC):
    @classmethod
    def __subclasshook__(cls, subcls: Any) -> Any:  # suppress types in this hack
        if cls is KubernetesModel:
            if any(C.__module__.startswith('kubernetes.client.models.') for C in subcls.__mro__):
                return True
        return NotImplemented

    @property
    def metadata(self) -> Optional[V1ObjectMeta]:
        raise NotImplementedError

    @metadata.setter
    def metadata(self, _: Optional[V1ObjectMeta]) -> None:
        raise NotImplementedError
