import urllib.parse
from typing import NamedTuple, Optional, Mapping, List


# An immutable reference to a custom resource definition.
class Resource(NamedTuple):
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
            namespace: Optional[str] = None,
            name: Optional[str] = None,
            params: Optional[Mapping[str, str]] = None,
    ) -> str:
        return self._build_url(server, params, [
            '/api' if self.group == '' and self.version == 'v1' else '/apis',
            self.group,
            self.version,
            'namespaces' if namespace is not None else None,
            namespace,
            self.plural,
            name,
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
