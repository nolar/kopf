import functools

import click

from kopf import config
from kopf.reactor import loading
from kopf.reactor import peering
from kopf.reactor import queueing


def logging_options(fn):
    """ A decorator to configure logging in all command in the same way."""
    @click.option('-v', '--verbose', is_flag=True)
    @click.option('-d', '--debug', is_flag=True)
    @click.option('-q', '--quiet', is_flag=True)
    @functools.wraps(fn)  # to preserve other opts/args
    def wrapper(verbose, quiet, debug, *args, **kwargs):
        config.configure(debug=debug, verbose=verbose, quiet=quiet)
        return fn(*args, **kwargs)
    return wrapper


@click.group(name='kopf', context_settings=dict(
    auto_envvar_prefix='KOPF',
))
def main():
    pass


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('--standalone', is_flag=True, default=False)
@click.option('--dev', 'priority', type=int, is_flag=True, flag_value=666)
@click.option('-P', '--peering', 'peering_name', type=str, default=None, envvar='KOPF_RUN_PEERING')
@click.option('-p', '--priority', type=int, default=0)
@click.option('-m', '--module', 'modules', multiple=True)
@click.argument('paths', nargs=-1)
def run(paths, modules, peering_name, priority, standalone, namespace):
    """ Start an operator process and handle all the requests. """
    config.login()
    loading.preload(
        paths=paths,
        modules=modules,
    )
    return queueing.run(
        standalone=standalone,
        namespace=namespace,
        priority=priority,
        peering_name=peering_name,
    )


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('-i', '--id', type=str, default=None)
@click.option('--dev', 'priority', flag_value=666)
@click.option('-P', '--peering', 'peering_name', type=str, default=None, envvar='KOPF_FREEZE_PEERING')
@click.option('-p', '--priority', type=int, default=100)
@click.option('-t', '--lifetime', type=int, required=True)
@click.option('-m', '--message', type=str)
def freeze(id, message, lifetime, namespace, peering_name, priority):
    """ Freeze the resource handling in the cluster. """
    config.login()
    ourserlves = peering.Peer(
        id=id or peering.detect_own_id(),
        name=peering_name,
        namespace=namespace,
        priority=priority,
        lifetime=lifetime,
    )
    ourserlves.keepalive()


@main.command()
@logging_options
@click.option('-n', '--namespace', default=None)
@click.option('-i', '--id', type=str, default=None)
@click.option('-P', '--peering', 'peering_name', type=str, default=None, envvar='KOPF_RESUME_PEERING')
def resume(id, namespace, peering_name):
    """ Resume the resource handling in the cluster. """
    config.login()
    ourselves = peering.Peer(
        id=id or peering.detect_own_id(),
        name=peering_name,
        namespace=namespace,
    )
    ourselves.disappear()
