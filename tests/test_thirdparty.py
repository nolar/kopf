import types

import pytest

from kopf._cogs.helpers.thirdparty import KubernetesModelSync


@pytest.mark.parametrize('name', ['V1Pod', 'V1ObjectMeta', 'V1PodSpec', 'V1PodTemplateSpec'])
@pytest.mark.timeout(10)  # kubernetes client v37 compiles slowly (2-3s) for the first time
def test_kubernetes_model_classes_detection(kubernetes, name):
    cls = getattr(kubernetes.client, name)
    assert issubclass(cls, KubernetesModelSync)


@pytest.mark.parametrize('name', ['CoreV1Api', 'ApiClient', 'Configuration'])
@pytest.mark.timeout(10)  # kubernetes client v37 compiles slowly (2-3s) for the first time
def test_kubernetes_other_classes_detection(kubernetes, name):
    cls = getattr(kubernetes.client, name)
    assert not issubclass(cls, KubernetesModelSync)


@pytest.mark.parametrize('cls', [object, types.SimpleNamespace])
@pytest.mark.timeout(10)  # kubernetes client v37 compiles slowly (2-3s) for the first time
def test_non_kubernetes_classes_detection(kubernetes, cls):
    assert not issubclass(cls, KubernetesModelSync)
