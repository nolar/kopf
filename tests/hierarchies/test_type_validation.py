import pytest

import kopf
from kopf.structs.bodies import Body


def test_in_owner_reference_appending():
    with pytest.raises(TypeError) as e:
        kopf.append_owner_reference(object(), Body({}))
    assert "K8s object class is not supported" in str(e.value)


def test_in_owner_reference_removal():
    with pytest.raises(TypeError) as e:
        kopf.remove_owner_reference(object(), Body({}))
    assert "K8s object class is not supported" in str(e.value)


def test_in_name_harmonization():
    with pytest.raises(TypeError) as e:
        kopf.harmonize_naming(object(), 'x')
    assert "K8s object class is not supported" in str(e.value)


def test_in_namepace_adjustment():
    with pytest.raises(TypeError) as e:
        kopf.adjust_namespace(object(), 'x')
    assert "K8s object class is not supported" in str(e.value)


def test_in_labelling():
    with pytest.raises(TypeError) as e:
        kopf.label(object(), {})
    assert "K8s object class is not supported" in str(e.value)


def test_in_adopting():
    with pytest.raises(TypeError) as e:
        kopf.adopt(object(), Body({}))
    assert "K8s object class is not supported" in str(e.value)
