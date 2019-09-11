import logging

import click
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError

log = logging.getLogger('scli')


class SSHTunnelsContainer:
    def __init__(self, ssh_username=None, ssh_pkey=None, ssh_pass=None,
                 initial_endpoint=None):
        self._tunnels = {}
        self._ssh_username = ssh_username
        self._ssh_pkey = ssh_pkey
        self._ssh_pass = ssh_pass
        self._initial_endpoint = initial_endpoint

    def _init_tunnel(self, host):
        log.debug('Initializing SSH tunnel to {}'.format(host))
        server = SSHTunnelForwarder(
            host,
            ssh_username=self._ssh_username,
            ssh_pkey=self._ssh_pkey,
            ssh_private_key_password=self._ssh_pass or 'fake',
            remote_bind_address=('127.0.0.1', 10000),
            set_keepalive=10,
        )
        try:
            server.start()
        except BaseSSHTunnelForwarderError:
            log.error(
                'Connection to {username}@{host} using {pkey} failed'.format(
                    username=self._ssh_username,
                    host=host,
                    pkey=self._ssh_pkey)
            )
            raise click.Abort()

        self._tunnels[host] = server

    def init_tunnels(self, hosts):
        for host in hosts:
            self._init_tunnel(host)

    def get_port(self, host=None):
        if host is None:
            host = self._initial_endpoint

        if host not in self._tunnels:
            self._init_tunnel(host)

        self._tunnels[host].check_tunnels()
        return self._tunnels[host].local_bind_port

    def reset(self):
        """
        Reestablish SSH tunnels due to connection error
        """
        self.stop()
        discovered_endpoints = list(self._tunnels.keys())
        self._tunnels = {}
        self.init_tunnels(discovered_endpoints)

    def stop(self):
        for server in self._tunnels.values():
            server.stop()

    def __del__(self):
        self.stop()
