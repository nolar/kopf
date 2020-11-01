import dataclasses
import urllib.parse
from typing import List, Mapping, NewType, Optional

# A specific really existing addressable namespace (at least, the one assumed to be so).
# Made as a NewType for stricter type-checking to avoid collisions with patterns and other strings.
NamespaceName = NewType('NamespaceName', str)

# A namespace reference usable in the API calls. `None` means cluster-wide API calls.
Namespace = Optional[NamespaceName]


@dataclasses.dataclass(frozen=True)
class Resource:
    group: str
    version: str
    plural: str

    @property
    def name(self) -> str:
        return f'{self.plural}.{self.group}'.strip('.')

    @property
    def api_version(self) -> str:
        # Strip heading/trailing slashes if group is absent (e.g. for pods).
        return f'{self.group}/{self.version}'.strip('/')

    def get_url(
            self,
            *,
            server: Optional[str] = None,
            namespace: Namespace = None,
            name: Optional[str] = None,
            subresource: Optional[str] = None,
            params: Optional[Mapping[str, str]] = None,
    ) -> str:
        if subresource is not None and name is None:
            raise ValueError("Subresources can be used only with specific resources by their name.")

        return self._build_url(server, params, [
            '/api' if self.group == '' and self.version == 'v1' else '/apis',
            self.group,
            self.version,
            'namespaces' if namespace is not None else None,
            namespace,
            self.plural,
            name,
            subresource,
        ])

    def get_version_url(
            self,
            *,
            server: Optional[str] = None,
            params: Optional[Mapping[str, str]] = None,
    ) -> str:
        return self._build_url(server, params, [
            '/api' if self.group == '' and self.version == 'v1' else '/apis',
            self.group,
            self.version,
        ])

    def _build_url(
            self,
            server: Optional[str],
            params: Optional[Mapping[str, str]],
            parts: List[Optional[str]],
    ) -> str:
        query = urllib.parse.urlencode(params, encoding='utf-8') if params else ''
        path = '/'.join([part for part in parts if part])
        url = path + ('?' if query else '') + query
        return url if server is None else server.rstrip('/') + '/' + url.lstrip('/')


@dataclasses.dataclass(frozen=True)
class Selector:
    group: str
    version: str
    plural: str

    def check(
            self,
            resource: Resource,
    ) -> bool:
        self_tuple = (self.group, self.version, self.plural)
        other_tuple = (resource.group, resource.version, resource.plural)
        return self_tuple == other_tuple
