import asyncio
import dataclasses
import functools
from typing import Any, Optional, Callable, List

import click

from kopf import config
from kopf.engines import peering
from kopf.reactor import running
from kopf.structs import credentials
from kopf.structs import primitives
from kopf.utilities import loaders


@dataclasses.dataclass()
class CLIControls:
    """ `KopfRunner` controls, which are impossible to pass via CLI. """
    ready_flag: Optional[primitives.Flag] = None
    stop_flag: Optional[primitives.Flag] = None
    vault: Optional[credentials.Vault] = None


def logging_options(fn: Callable[..., Any]) -> Callable[..., Any]:
    """ A decorator to configure logging in all command in the same way."""
    @click.option('-v', '--verbose', is_flag=True)
    @click.option('-d', '--debug', is_flag=True)
    @click.option('-q', '--quiet', is_flag=True)
    @functools.wraps(fn)  # to preserve other opts/args
    def wrapper(verbose: bool, quiet: bool, debug: bool, *args: Any, **kwargs: Any) -> Any:
        config.configure(debug=debug, verbose=verbose, quiet=quiet)
        return fn(*args, **kwargs)

    return wrapper


@click.version_option(prog_name='kopf')
@click.group(name='kopf', context_settings=dict(
    auto_envvar_prefix='KOPF',
))
def main() -> None:
    pass


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('--standalone', is_flag=True, default=False)
@click.option('--dev', 'priority', type=int, is_flag=True, flag_value=666)
@click.option('-L', '--liveness', 'liveness_endpoint', type=str)
@click.option('-P', '--peering', 'peering_name', type=str, default=None, envvar='KOPF_RUN_PEERING')
@click.option('-p', '--priority', type=int, default=0)
@click.option('-m', '--module', 'modules', multiple=True)
@click.argument('paths', nargs=-1)
@click.make_pass_decorator(CLIControls, ensure=True)
def run(
        __controls: CLIControls,
        paths: List[str],
        modules: List[str],
        peering_name: Optional[str],
        priority: int,
        standalone: bool,
        namespace: Optional[str],
        liveness_endpoint: Optional[str],
) -> None:
    """ Start an operator process and handle all the requests. """
    loaders.preload(
        paths=paths,
        modules=modules,
    )
    return running.run(
        standalone=standalone,
        namespace=namespace,
        priority=priority,
        peering_name=peering_name,
        liveness_endpoint=liveness_endpoint,
        stop_flag=__controls.stop_flag,
        ready_flag=__controls.ready_flag,
        vault=__controls.vault,
    )


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('-i', '--id', type=str, default=None)
@click.option('--dev', 'priority', flag_value=666)
@click.option('-P', '--peering', 'peering_name', type=str, required=True, envvar='KOPF_FREEZE_PEERING')
@click.option('-p', '--priority', type=int, default=100, required=True)
@click.option('-t', '--lifetime', type=int, required=True)
@click.option('-m', '--message', type=str)
def freeze(
        id: Optional[str],
        message: Optional[str],
        lifetime: int,
        namespace: Optional[str],
        peering_name: str,
        priority: int,
) -> None:
    """ Freeze the resource handling in the cluster. """
    ourserlves = peering.Peer(
        id=id or peering.detect_own_id(),
        name=peering_name,
        namespace=namespace,
        priority=priority,
        lifetime=lifetime,
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ourserlves.keepalive())


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('-i', '--id', type=str, default=None)
@click.option('-P', '--peering', 'peering_name', type=str, required=True, envvar='KOPF_RESUME_PEERING')
def resume(
        id: Optional[str],
        namespace: Optional[str],
        peering_name: str,
) -> None:
    """ Resume the resource handling in the cluster. """
    ourselves = peering.Peer(
        id=id or peering.detect_own_id(),
        name=peering_name,
        namespace=namespace,
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ourselves.disappear())
