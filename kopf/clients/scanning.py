import asyncio
import ssl
import urllib.parse
from typing import Collection, Mapping, Optional, Set, Tuple

from kopf.clients import auth, errors
from kopf.structs import references


@auth.reauthenticated_request
async def read_sslcert(
        *,
        context: Optional[auth.APIContext] = None,
) -> Tuple[str, bytes]:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    parsed = urllib.parse.urlparse(context.server)
    host = parsed.hostname or ''  # NB: it cannot be None/empty in our case.
    port = parsed.port or 443
    loop = asyncio.get_running_loop()
    cert = await loop.run_in_executor(None, ssl.get_server_certificate, (host, port))
    return host, cert.encode('ascii')


@auth.reauthenticated_request
async def read_version(
        *,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Mapping[str, str]:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    server = context.server.rstrip('/')
    url = f'{server}/version'
    rsp: Mapping[str, str] = await errors.parse_response(await context.session.get(url))
    return rsp


@auth.reauthenticated_request
async def scan_resources(
        *,
        groups: Optional[Collection[str]] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Collection[references.Resource]:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")
    coros = {
        _read_old_api(groups=groups, context=context),
        _read_new_apis(groups=groups, context=context),
    }
    resources: Set[references.Resource] = set()
    for coro in asyncio.as_completed(coros):
        resources.update(await coro)
    return resources


async def _read_old_api(
        *,
        groups: Optional[Collection[str]],
        context: auth.APIContext,
) -> Collection[references.Resource]:
    resources: Set[references.Resource] = set()
    if groups is None or '' in groups:
        server = context.server.rstrip('/')
        url = f'{server}/api'
        rsp = await errors.parse_response(await context.session.get(url))
        coros = {
            _read_version(
                url=f'{server}/api/{version_name}',
                group='',
                version=version_name,
                preferred=True,
                context=context)
            for version_name in rsp['versions']
        }
        for coro in asyncio.as_completed(coros):
            resources.update(await coro)
    return resources


async def _read_new_apis(
        *,
        groups: Optional[Collection[str]],
        context: auth.APIContext,
) -> Collection[references.Resource]:
    resources: Set[references.Resource] = set()
    if groups is None or set(groups or {}) - {''}:
        server = context.server.rstrip('/')
        url = f'{server}/apis'
        rsp = await errors.parse_response(await context.session.get(url))
        items = [d for d in rsp['groups'] if groups is None or d['name'] in groups]
        coros = {
            _read_version(
                url=f'{server}/apis/{group_dat["name"]}/{version["version"]}',
                group=group_dat['name'],
                version=version['version'],
                preferred=version['version'] == group_dat['preferredVersion']['version'],
                context=context)
            for group_dat in items
            for version in group_dat['versions']
        }
        for coro in asyncio.as_completed(coros):
            resources.update(await coro)
    return resources


async def _read_version(
        *,
        url: str,
        group: str,
        version: str,
        preferred: bool,
        context: auth.APIContext,
) -> Collection[references.Resource]:
    try:
        rsp = await errors.parse_response(await context.session.get(url))
    except errors.APINotFoundError:
        # This happens when the last and the only resource of a group/version
        # has been deleted, the whole group/version is gone, and we rescan it.
        return set()
    else:
        # Note: builtins' singulars are empty strings in K3s (reasons unknown):
        # fall back to the lowercased kind so that the selectors could match.
        return {
            references.Resource(
                group=group,
                version=version,
                kind=resource['kind'],
                plural=resource['name'],
                singular=resource['singularName'] or resource['kind'].lower(),
                shortcuts=frozenset(resource.get('shortNames', [])),
                categories=frozenset(resource.get('categories', [])),
                subresources=frozenset(
                    subresource['name'].split('/', 1)[-1]
                    for subresource in rsp['resources']
                    if subresource['name'].startswith(f'{resource["name"]}/')
                ),
                namespaced=resource['namespaced'],
                preferred=preferred,
                verbs=frozenset(resource.get('verbs', [])),
            )
            for resource in rsp['resources']
            if '/' not in resource['name']
        }
