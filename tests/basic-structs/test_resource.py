import pytest

from kopf.reactor.registries import Resource


def test_no_args():
    with pytest.raises(TypeError):
        Resource()


def test_all_args(mocker):
    group = mocker.Mock()
    version = mocker.Mock()
    plural = mocker.Mock()
    resource = Resource(
        group=group,
        version=version,
        plural=plural,
    )
    assert resource.group is group
    assert resource.version is version
    assert resource.plural is plural
