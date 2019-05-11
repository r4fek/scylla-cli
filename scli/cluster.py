from collections import defaultdict
import logging

import click
from prettytable import PrettyTable

from scli.parser import parse_application_state
from scli.utils import humansize
from scli.api_client import ApiClient

log = logging.getLogger('scli')


class Endpoint:
    def __init__(self, name, is_alive=True, application_state=None):
        self.name = name
        self.is_alive = is_alive
        self._application_state = parse_application_state(application_state)

    def __getattr__(self, item):
        if item in self._application_state:
            return self._application_state[item]
        else:
            raise AttributeError(item)


class Keyspace:
    name = None
    tables = set()

    def __init__(self, name, tables):
        self.name = name
        self.tables = tables


class Ring:
    def __init__(self, endpoint, keyspace):
        self.keyspace = keyspace
        self.ranges = defaultdict(list)
        self.client = ApiClient(endpoint)
        self._initialize_ring()

    def _initialize_ring(self):
        log.debug('Initializing ring for keyspace {}'.format(self.keyspace))

        for token_range in self.client.describe_ring(self.keyspace):
            for endpoint in token_range['endpoint_details']:
                self.ranges[endpoint['host']].append(
                    (token_range['start_token'], token_range['end_token'])
                )

    def ranges_for_endpoint(self, endpoint):
        return self.ranges.get(endpoint, [])


class Cluster(object):
    name = None
    endpoints = {}
    keyspaces = {}

    def __init__(self, endpoint):
        self.client = ApiClient(endpoint)
        self.name = self.client.cluster_name()
        self.initialize_endpoints()
        self.initialize_keyspaces()

    def initialize_keyspaces(self):
        keyspace_tables = defaultdict(set)

        for table_data in self.client.tables():
            keyspace_tables[table_data['ks']].add(table_data['cf'])

        for keyspace, tables in keyspace_tables.items():
            self.keyspaces[keyspace] = Keyspace(keyspace, tables)

    def initialize_endpoints(self):
        endpoints = self.client.endpoints_detailed()
        for data in endpoints:
            self.endpoints[data['addrs']] = Endpoint(
                data['addrs'],
                is_alive=data['is_alive'],
                application_state=data['application_state'],
            )

    @property
    def endpoints_by_dc(self):
        """
        :return: {'dc': [..endpoints]}
        """
        endpoints = defaultdict(list)
        for e in self.endpoints.values():
            endpoints[e.dc].append(e)

        return endpoints

    def status(self):
        field_names = ['State', 'Address', 'Load', 'Tokens', 'Version', 'Rack']
        nodes_down_by_dc = defaultdict(list)
        any_node_down = False
        click.echo(
            click.style('Cluster name: {}'.format(self.name), bold=True))

        for dc, endpoints in self.endpoints_by_dc.items():
            click.echo(click.style('\nDatacenter: {}'.format(dc), bold=True))
            table = PrettyTable()
            table.field_names = field_names
            table.sortby = 'Address'

            for c in field_names:
                table.align[c] = 'l'

            for e in endpoints:
                if not e.is_alive:
                    any_node_down = True
                    nodes_down_by_dc[dc].append(e.name)
                status = click.style(
                    e.status, fg='green' if e.is_alive else 'red')

                table.add_row((
                    status,
                    e.name,
                    humansize(e.load),
                    len(e.tokens),
                    e.release_version,
                    e.rack,
                ))

            click.echo(table)

        if any_node_down:
            click.echo(
                click.style('Cluster status: Unhealthy',
                            fg='red', bold=True))
            click.echo(click.style('Nodes down:', fg='red'))
            for dc, nodes_down in nodes_down_by_dc.items():
                click.echo(
                    click.style(
                        '{dc}: {nodes_down}'.format(
                            dc=dc,
                            nodes_down=', '.join(nodes_down)),
                        fg='red'))
        else:
            click.echo(
                click.style('Cluster status: All green!',
                            fg='green', bold=True))
