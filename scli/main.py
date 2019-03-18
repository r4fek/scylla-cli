import os
import logging
from logging.handlers import SysLogHandler

import click
import click_log

import scli as meta
from .cluster import Cluster
from .repair import Repair
from .api_client import client


click_log.ColorFormatter.colors['info'] = dict(fg="green")
log = logging.getLogger('scli')
click_log.basic_config(log)


def _setup_logger(log_to):
    """
    :param log_to: `syslog` or path to log file
    :return:
    """
    if log_to is None:
        return
    if log_to == 'syslog':
        handler = SysLogHandler(address='/dev/log')

    else:
        if not os.path.exists(log_to):
            with open(log_to, 'w') as _:
                pass
        handler = logging.FileHandler(log_to)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('scli: [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)


@click.group()
@click.option('-h', '--host', envvar='SCYLLA_HOST')
@click.option('-u', '--ssh_username', envvar='SCYLLA_USERNAME', default='scli',
              help='SSH username on Scylla host')
@click.option('-k', '--ssh_pkey', envvar='SCYLLA_PKEY',
              default=os.path.expanduser('~/.ssh/id_rsa'),
              help='SSH public key path')
@click.option('-p', '--ssh_pass', is_flag=True,
              help='Use this flag if your SSH key is protected by password')
@click.option('-l', '--log_to', help='Where to store logs from the client')
@click_log.simple_verbosity_option(log)
@click.pass_context
def cli(ctx, host, ssh_username, ssh_pkey, ssh_pass, log_to):
    if host is None:
        click.echo('Either --host or SCYLLA_HOST env should be provided')
        raise click.Abort()

    _setup_logger(log_to)

    if ssh_pass:
        password = click.prompt('Please enter a valid SSH key password',
                                hide_input=True)
    else:
        password = None

    client.setup(
        ssh_username=ssh_username,
        ssh_pkey=ssh_pkey,
        ssh_pass=password,
        initial_endpoint=host,
    )

    def destroy_ssh_tunnels():
        client.stop()

    ctx.call_on_close(destroy_ssh_tunnels)


@cli.command(short_help='Repair Scylla Cluster')
@click.argument('keyspace', nargs=1)
@click.argument('table', nargs=1, required=False)
@click.option('--hosts', multiple=True, help='Hosts to repair')
@click.option('--exclude', multiple=True, help='Do not repair these hosts')
@click.option('--dc', help='Datacenter to repair')
@click.option('--local', is_flag=True, help='Repair using hosts in local DC '
                                            'only')
def repair(keyspace, table, hosts, exclude, dc, local):
    _repair = Repair(
        keyspace=keyspace,
        table=table,
        hosts=hosts,
        exclude=exclude,
        dc=dc,
        local=local,
    )
    _repair.start()


@cli.command(short_help='Show cluster status')
def status():
    c = Cluster()
    c.status()


@cli.command(short_help='Print version number')
def version():
    click.echo(
        'scli version {}'.format('.'.join(map(str, meta.__version__))))
