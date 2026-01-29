import asyncio
import dataclasses
import functools
import os
from collections.abc import Callable, Collection
from typing import Any

import click

from kopf._cogs.aiokits import aioadapters
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import loaders
from kopf._cogs.structs import credentials, references
from kopf._core.actions import loggers
from kopf._core.engines import peering
from kopf._core.intents import registries
from kopf._core.reactor import running
from kopf._kits import loops


@dataclasses.dataclass()
class CLIControls:
    """ :class:`KopfRunner` controls, which are impossible to pass via CLI. """
    ready_flag: aioadapters.Flag | None = None
    stop_flag: aioadapters.Flag | None = None
    vault: credentials.Vault | None = None
    registry: registries.OperatorRegistry | None = None
    settings: configuration.OperatorSettings | None = None
    loop: asyncio.AbstractEventLoop | None = None


def logging_options(fn: Callable[..., Any]) -> Callable[..., Any]:
    """ A decorator to configure logging in all commands the same way."""
    @click.option('-v', '--verbose', is_flag=True)
    @click.option('-d', '--debug', is_flag=True)
    @click.option('-q', '--quiet', is_flag=True)
    @click.option('--log-format', type=click.Choice(loggers.LogFormat, case_sensitive=False), default='full')
    @click.option('--log-refkey', type=str)
    @click.option('--log-prefix/--no-log-prefix', default=None)
    @functools.wraps(fn)  # to preserve other opts/args
    def wrapper(verbose: bool, quiet: bool, debug: bool,
                log_format: loggers.LogFormat = loggers.LogFormat.FULL,
                log_prefix: bool | None = False,
                log_refkey: str | None = None,
                *args: Any, **kwargs: Any) -> Any:
        loggers.configure(debug=debug, verbose=verbose, quiet=quiet,
                          log_format=log_format, log_refkey=log_refkey, log_prefix=log_prefix)
        return fn(*args, **kwargs)

    return wrapper


@click.group(name='kopf', context_settings=dict(
    auto_envvar_prefix='KOPF',
))
@click.version_option(prog_name='kopf')
@click.make_pass_decorator(CLIControls, ensure=True)
def main(__controls: CLIControls) -> None:
    pass


@main.command()
@logging_options
@click.option('-A', '--all-namespaces', 'clusterwide', is_flag=True)
@click.option('-n', '--namespace', 'namespaces', multiple=True)
@click.option('--standalone', is_flag=True, default=None)
@click.option('--dev', is_flag=True)
@click.option('-L', '--liveness', 'liveness_endpoint', type=str)
@click.option('-P', '--peering', 'peering_name', type=str, envvar='KOPF_RUN_PEERING')
@click.option('-p', '--priority', type=int)
@click.option('-m', '--module', 'modules', multiple=True)
@click.argument('paths', nargs=-1)
@click.make_pass_decorator(CLIControls, ensure=True)
def run(
        __controls: CLIControls,
        paths: list[str],
        modules: list[str],
        peering_name: str | None,
        priority: int | None,
        dev: bool | None,
        standalone: bool | None,
        namespaces: Collection[references.NamespacePattern],
        clusterwide: bool,
        liveness_endpoint: str | None,
) -> None:
    """ Start an operator process and handle all the requests. """
    priority = 666 if dev else priority
    if os.environ.get('KOPF_RUN_NAMESPACE'):  # legacy for single-namespace mode
        namespaces = tuple(namespaces) + (os.environ.get('KOPF_RUN_NAMESPACE', ''),)
    if namespaces and clusterwide:
        raise click.UsageError("Either --namespace or --all-namespaces can be used, not both.")
    if __controls.registry is not None:
        registries.set_default_registry(__controls.registry)
    loaders.preload(
        paths=paths,
        modules=modules,
    )
    with loops.proper_loop(suggested_loop=__controls.loop) as actual_loop:
        return running.run(
            standalone=standalone,
            namespaces=namespaces,
            clusterwide=clusterwide,
            priority=priority,
            peering_name=peering_name,
            liveness_endpoint=liveness_endpoint,
            registry=__controls.registry,
            settings=__controls.settings,
            stop_flag=__controls.stop_flag,
            ready_flag=__controls.ready_flag,
            vault=__controls.vault,
            loop=actual_loop,
        )


@main.command()
@logging_options
@click.option('-n', '--namespace', 'namespaces', multiple=True)
@click.option('-A', '--all-namespaces', 'clusterwide', is_flag=True)
@click.option('-i', '--id', type=str, default=None)
@click.option('--dev', is_flag=True)
@click.option('-P', '--peering', 'peering_name', required=True, envvar='KOPF_FREEZE_PEERING')
@click.option('-p', '--priority', type=int, default=100, required=True)
@click.option('-t', '--lifetime', type=int, required=True)
@click.option('-m', '--message', type=str)
@click.make_pass_decorator(CLIControls, ensure=True)
def freeze(
        __controls: CLIControls,
        id: str | None,
        message: str | None,
        lifetime: int,
        namespaces: Collection[references.NamespacePattern],
        clusterwide: bool,
        peering_name: str,
        priority: int,
        dev: bool,
) -> None:
    """ Pause the resource handling in the operator(s). """
    priority = 666 if dev else priority
    identity = peering.Identity(id) if id else peering.detect_own_id(manual=True)
    insights = references.Insights()
    settings = configuration.OperatorSettings()
    settings.peering.name = peering_name
    settings.peering.priority = priority
    with loops.proper_loop(suggested_loop=__controls.loop) as actual_loop:
        return running.run(
            clusterwide=clusterwide,
            namespaces=namespaces,
            insights=insights,
            identity=identity,
            settings=settings,
            loop=actual_loop,
            _command=peering.touch_command(
                insights=insights,
                identity=identity,
                settings=settings,
                lifetime=lifetime))


@main.command()
@logging_options
@click.option('-n', '--namespace', 'namespaces', multiple=True)
@click.option('-A', '--all-namespaces', 'clusterwide', is_flag=True)
@click.option('-i', '--id', type=str, default=None)
@click.option('-P', '--peering', 'peering_name', required=True, envvar='KOPF_RESUME_PEERING')
@click.make_pass_decorator(CLIControls, ensure=True)
def resume(
        __controls: CLIControls,
        id: str | None,
        namespaces: Collection[references.NamespacePattern],
        clusterwide: bool,
        peering_name: str,
) -> None:
    """ Resume the resource handling in the operator(s). """
    identity = peering.Identity(id) if id else peering.detect_own_id(manual=True)
    insights = references.Insights()
    settings = configuration.OperatorSettings()
    settings.peering.name = peering_name
    with loops.proper_loop(suggested_loop=__controls.loop) as actual_loop:
        return running.run(
            clusterwide=clusterwide,
            namespaces=namespaces,
            insights=insights,
            identity=identity,
            settings=settings,
            loop=actual_loop,
            _command=peering.touch_command(
                insights=insights,
                identity=identity,
                settings=settings,
                lifetime=0))
