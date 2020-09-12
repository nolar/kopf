import pytest

from kopf.engines.loggers import LocalObjectLogger, ObjectLogger
from kopf.structs.bodies import Body


@pytest.mark.parametrize('cls', [ObjectLogger, LocalObjectLogger])
def test_mandatory_body(cls, settings, caplog):
    with pytest.raises(TypeError):
        cls(settings=settings)


@pytest.mark.parametrize('cls', [ObjectLogger, LocalObjectLogger])
def test_mandatory_settings(cls, settings, caplog):
    with pytest.raises(TypeError):
        cls(body=Body({}))


@pytest.mark.parametrize('cls', [ObjectLogger, LocalObjectLogger])
def test_extras_from_metadata(cls, settings, caplog):
    body = Body({
        'kind': 'kind1',
        'apiVersion': 'api1/v1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'namespace1'},
    })

    logger = cls(body=body, settings=settings)
    logger.info("hello")

    assert len(caplog.records) == 1
    assert hasattr(caplog.records[0], 'k8s_ref')
    assert caplog.records[0].k8s_ref == {
        'uid': 'uid1',
        'name': 'name1',
        'namespace': 'namespace1',
        'apiVersion': 'api1/v1',
        'kind': 'kind1',
    }


@pytest.mark.parametrize('cls', [ObjectLogger])
def test_k8s_posting_enabled_in_a_regular_logger(cls, settings, caplog):
    body = Body({})

    logger = cls(body=body, settings=settings)
    logger.info("hello")

    assert len(caplog.records) == 1
    assert hasattr(caplog.records[0], 'k8s_skip')
    assert caplog.records[0].k8s_skip is False


@pytest.mark.parametrize('cls', [LocalObjectLogger])
def test_k8s_posting_disabled_in_a_local_logger(cls, settings, caplog):
    body = Body({})

    logger = cls(body=body, settings=settings)
    logger.info("hello")

    assert len(caplog.records) == 1
    assert hasattr(caplog.records[0], 'k8s_skip')
    assert caplog.records[0].k8s_skip is True
