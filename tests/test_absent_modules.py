"""
Verify that the module-prohibiting fixtures do work as expected.
Otherwise, the tests are useless or can show false-positives.
"""
import pytest


@pytest.mark.usefixtures('no_kubernetes')
def test_client_uninstalled_has_effect():
    with pytest.raises(ImportError):
        import kubernetes


@pytest.mark.usefixtures('no_pykube')
def test_pykube_uninstalled_has_effect():
    with pytest.raises(ImportError):
        import pykube
