import types

import pytest

from kopf.utilities.thirdparty import KubernetesModel


@pytest.mark.parametrize('name', ['V1Pod', 'V1ObjectMeta', 'V1PodSpec', 'V1PodTemplateSpec'])
def test_kubernetes_model_classes_detection(kubernetes, name):
    cls = getattr(kubernetes.client, name)
    assert issubclass(cls, KubernetesModel)


@pytest.mark.parametrize('name', ['CoreV1Api', 'ApiClient', 'Configuration'])
def test_kubernetes_other_classes_detection(kubernetes, name):
    cls = getattr(kubernetes.client, name)
    assert not issubclass(cls, KubernetesModel)


@pytest.mark.parametrize('cls', [object, types.SimpleNamespace])
def test_non_kubernetes_classes_detection(kubernetes, cls):
    assert not issubclass(cls, KubernetesModel)
