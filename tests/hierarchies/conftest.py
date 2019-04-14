import pytest


class CustomIterable:
    def __init__(self, objs):
        self._objs = objs

    def __iter__(self):
        for obj in self._objs:
            yield obj


@pytest.fixture(params=[list, tuple, CustomIterable],
                ids=['list', 'tuple', 'custom'])
def multicls(request):
    return request.param
