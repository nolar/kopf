import functools

import click

from kopf.config import login, configure
from kopf.reactor.loading import preload
from kopf.reactor.peering import Peer, PEERING_DEFAULT_NAME, detect_own_id
from kopf.reactor.queueing import run as real_run


def logging_options(fn):
    """ A decorator to configure logging in all command in the same way."""
    @click.option('-v', '--verbose', is_flag=True)
    @click.option('-d', '--debug', is_flag=True)
    @click.option('-q', '--quiet', is_flag=True)
    @functools.wraps(fn)  # to preserve other opts/args
    def wrapper(verbose, quiet, debug, *args, **kwargs):
        configure(debug=debug, verbose=verbose, quiet=quiet)
        return fn(*args, **kwargs)
    return wrapper


@click.group(context_settings=dict(
    auto_envvar_prefix='KOPF',
))
def main():
    pass


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('--standalone', is_flag=True, default=False)
@click.option('--dev', 'priority', flag_value=666)
@click.option('-P', '--peering', type=str, default=None)
@click.option('-p', '--priority', type=int, default=0)
@click.option('-m', '--module', 'modules', multiple=True)
@click.argument('paths', nargs=-1)
def run(paths, modules, peering, priority, standalone, namespace):
    """ Start an operator process and handle all the requests. """
    login()
    preload(
        paths=paths,
        modules=modules,
    )
    return real_run(
        standalone=standalone,
        namespace=namespace,
        priority=priority,
        peering=peering,
    )


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('-i', '--id', type=str, default=None)
@click.option('--dev', 'priority', flag_value=666)
@click.option('-P', '--peering', type=str, default=PEERING_DEFAULT_NAME)
@click.option('-p', '--priority', type=int, default=100)
@click.option('-t', '--lifetime', type=int, required=True)
@click.option('-m', '--message', type=str)
def freeze(id, message, lifetime, namespace, peering, priority):
    """ Freeze the resource handling in the cluster. """
    login()
    ourserlves = Peer(
        id=id or detect_own_id(),
        peering=peering,
        namespace=namespace,
        priority=priority,
        lifetime=lifetime,
    )
    ourserlves.keepalive()


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('-i', '--id', type=str, default=None)
@click.option('-P', '--peering', type=str, default=PEERING_DEFAULT_NAME)
def resume(id, namespace, peering):
    """ Resume the resource handling in the cluster. """
    login()
    ourselves = Peer(
        id=id or detect_own_id(),
        peering=peering,
        namespace=namespace,
    )
    ourselves.disappear()
