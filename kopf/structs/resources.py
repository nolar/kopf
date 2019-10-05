from typing import NamedTuple


# An immutable reference to a custom resource definition.
class Resource(NamedTuple):
    group: str
    version: str
    plural: str

    @property
    def name(self):
        return f'{self.plural}.{self.group}'

    @property
    def api_version(self):
        # Strip heading/trailing slashes if group is absent (e.g. for pods).
        return f'{self.group}/{self.version}'.strip('/')
