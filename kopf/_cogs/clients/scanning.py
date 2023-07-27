import asyncio
from typing import Collection, Mapping, Optional, Set

from kopf._cogs.clients import api, errors
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import references


async def read_version(
        *,
        settings: configuration.OperatorSettings,
        logger: typedefs.Logger,
) -> Mapping[str, str]:
    rsp: Mapping[str, str] = await api.get('/version', settings=settings, logger=logger)
    return rsp


async def scan_resources(
        *,
        settings: configuration.OperatorSettings,
        logger: typedefs.Logger,
        groups: Optional[Collection[str]] = None,
) -> Collection[references.Resource]:
    coros = {
        _read_old_api(groups=groups, settings=settings, logger=logger),
        _read_new_apis(groups=groups, settings=settings, logger=logger),
    }
    resources: Set[references.Resource] = set()
    for coro in asyncio.as_completed(coros):
        resources.update(await coro)
    return resources


async def _read_old_api(
        *,
        settings: configuration.OperatorSettings,
        logger: typedefs.Logger,
        groups: Optional[Collection[str]],
) -> Collection[references.Resource]:
    resources: Set[references.Resource] = set()
    if groups is None or '' in groups:
        rsp = await api.get('/api', settings=settings, logger=logger)
        coros = {
            _read_version(
                url=f'/api/{version_name}',
                group='',
                version=version_name,
                preferred=True,
                settings=settings,
                logger=logger,
            )
            for version_name in rsp['versions']
        }
        for coro in asyncio.as_completed(coros):
            resources.update(await coro)
    return resources


async def _read_new_apis(
        *,
        settings: configuration.OperatorSettings,
        logger: typedefs.Logger,
        groups: Optional[Collection[str]],
) -> Collection[references.Resource]:
    resources: Set[references.Resource] = set()
    if groups is None or set(groups or {}) - {''}:
        rsp = await api.get('/apis', settings=settings, logger=logger)
        items = [d for d in rsp['groups'] if groups is None or d['name'] in groups]
        coros = {
            _read_version(
                url=f'/apis/{group_dat["name"]}/{version["version"]}',
                group=group_dat['name'],
                version=version['version'],
                preferred=version['version'] == group_dat['preferredVersion']['version'],
                settings=settings,
                logger=logger,
            )
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
        settings: configuration.OperatorSettings,
        logger: typedefs.Logger,
) -> Collection[references.Resource]:
    try:
        rsp = await api.get(url, settings=settings, logger=logger)
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
                    for subresource in rsp.get('resources', [])
                    if subresource['name'].startswith(f'{resource["name"]}/')
                ),
                namespaced=resource['namespaced'],
                preferred=preferred,
                verbs=frozenset(resource.get('verbs') or []),
            )
            for resource in rsp.get('resources', [])
            if '/' not in resource['name']
        }
