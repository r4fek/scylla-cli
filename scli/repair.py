from time import sleep
from datetime import timedelta, datetime
import logging

from progress.bar import Bar

from .cluster import Cluster, Ring
from .api_client import client


log = logging.getLogger('scli')


class ProgressBar(Bar):
    suffix = '%(index)d/%(max)d took %(elapsed_human)s / %(eta_td)s remaining'

    @property
    def elapsed_human(self):
        return str(timedelta(seconds=self.elapsed))


class Repair:
    MAX_FAILURES = 20

    def __init__(self, keyspace=None, table=None, dc=None,
                 hosts=None, exclude=None, local=None):
        self.cluster = Cluster()
        self.ring = None
        self.table = table
        self.dc = dc
        self.failures = 0
        self.failed_ranges = []
        self.local = local
        if keyspace is None:
            self.keyspaces = self.cluster.keyspaces.keys()
        else:
            self.keyspaces = [keyspace]

        if table is None:
            self.to_repair = self.cluster.keyspaces[keyspace].tables
        else:
            self.to_repair = [self.table]

        self.endpoints = self._prepare_endpoints(
            hosts=hosts, exclude=exclude, dc=dc)

    def _prepare_endpoints(self, hosts=None, exclude=None, dc=None):
        def _filter_endpoint(e):
            if hosts and e.name not in hosts:
                return False
            if dc and e.dc != dc:
                return False
            if exclude and e.name in exclude:
                return False
            return True

        return filter(_filter_endpoint, self.cluster.endpoints.values())

    def start(self):
        repair_start = datetime.now()

        for keyspace in self.keyspaces:
            self.ring = Ring(keyspace)
            for endpoint in self.endpoints:
                self._repair_endpoint(endpoint, keyspace, table=self.table)

        repair_end = datetime.now()
        log.info('Repair took {}'.format(repair_end-repair_start))

    def _check_repair_status(self, endpoint_name, keyspace, rid):
        while True:
            sleep(1)
            status = client.repair_status(
                endpoint_name, keyspace, rid)
            if status == '"FAILED"':
                return False
            elif status == '"RUNNING"':
                pass
            elif status == '"SUCCESSFUL"':
                return True
            else:
                log.warning('Unknown repair status {}'.format(status))
                return False

    def _run_repair(self, endpoint, keyspace, table=None):
        log.info('Repair {keyspace} {table} on {name}'.format(
            keyspace=keyspace, table=table or '', name=endpoint.name
        ))

        token_ranges = self.ring.ranges_for_endpoint(endpoint.name)
        bar = ProgressBar('Progress', max=len(token_ranges))
        bar.next(0)

        for start, end in token_ranges:
            if not self._repair_range(
                    endpoint, keyspace, start, end, table=table):
                # TODO: think about better value here or exp backoff
                sleep(60)
                if table is None:
                    # try to repair tables one by one if above fails
                    for _table in self.to_repair:
                        self._repair_range(
                            endpoint.name, keyspace, start, end, table=_table)

            bar.next()
            if not bar.is_tty():
                log.info('{index}/{max} complete'.format(
                    index=bar.index, max=bar.max))

            if self.failures >= self.MAX_FAILURES:
                raise Exception('Max number of failures exceeded')

        bar.finish()

    def _repair_range(self, endpoint, keyspace, start, end, table=None):
        repair_id = client.repair_async(
            endpoint.name,
            keyspace,
            table=table,
            start_token=start,
            end_token=end,
            dc=endpoint.dc if self.local else None,
        )

        ok = self._check_repair_status(endpoint.name, keyspace, repair_id)

        if not ok and table is not None:
            self.failures += 1
            log.error(
                '\nRepair range ({start}, {end}) cf: {table} on '
                '{endpoint_name} failed'.format(
                    start=start,
                    end=end,
                    table=table,
                    endpoint_name=endpoint.name)
            )
        return ok

    def _repair_endpoint(self, endpoint, keyspace, table=None):
        active_repair = client.active_repair(endpoint.name)
        if len(active_repair) > 0:
            log.warning('Node {name} is already involved in repair {repair}'
                        .format(name=endpoint.name, repair=active_repair))
            return

        self.failures = 0
        if table is not None:
            self._run_repair(endpoint, keyspace, table=table)
        else:
            self._run_repair(endpoint, keyspace)
