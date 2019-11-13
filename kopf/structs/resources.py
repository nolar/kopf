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
        parts: List[Optional[str]] = [
            '/api' if self.group == '' and self.version == 'v1' else '/apis',
            self.group,
            self.version,
            'namespaces' if namespace is not None else None,
            namespace,
            self.plural,
            name,
        ]
        query = urllib.parse.urlencode(params, encoding='utf-8') if params else ''
        path = '/'.join([part for part in parts if part])
        url = path + ('?' if query else '') + query
        return url if server is None else server.rstrip('/') + '/' + url.lstrip('/')
