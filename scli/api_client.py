import logging

import requests
from requests import exceptions
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from furl import furl


from .tunnel import SSHTunnelsContainer


log = logging.getLogger('scli')
PATHS = {
    'cluster_name': '/storage_service/cluster_name',
    'endpoints_live': '/gossiper/endpoint/live/',
    'endpoints_down': '/gossiper/endpoint/down/',
    'endpoints': '/failure_detector/endpoints/',
    'endpoints_simple': '/failure_detector/simple_states',
    'tokens': '/storage_service/tokens/{endpoint}',
    'datacenter': '/snitch/datacenter',
    'describe_ring': '/storage_service/describe_ring/{keyspace}',
    'column_family': '/column_family/',
    'repair_async': '/storage_service/repair_async/{keyspace}',
    'active_repair': '/storage_service/active_repair/',
}


class ApiClient:
    def __init__(self, uses_ssh=True, initial_endpoint=None, port=10000,
                 timeout=(5, 5), total_retries=10, backoff_factor=5):

        self.initial_endpoint = initial_endpoint
        self.port = port
        self.timeout = timeout
        self.total_retries = total_retries
        self.backoff_factor = backoff_factor

        self._hosts = []
        self.base_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        self.base_url_tpl = 'http://{host}:{port}'

        self.uses_ssh = uses_ssh
        self._tunnels_container = None

    def setup_ssh(self, initial_endpoint=None, ssh_username=None,
                  ssh_pkey=None, ssh_pass=None):

        self.uses_ssh = True
        self._tunnels_container = SSHTunnelsContainer(
            ssh_username=ssh_username,
            ssh_pkey=ssh_pkey,
            ssh_pass=ssh_pass,
            initial_endpoint=initial_endpoint)

    def stop_ssh(self):
        if not self.uses_ssh:
            return

        self._tunnels_container.stop()

    def _get_base_url(self, host):
        if self.uses_ssh:
            _host = 'localhost'
            _port = self._tunnels_container.get_port(host=host)

        else:
            _host = host or self.initial_endpoint
            _port = self.port

        return self.base_url_tpl.format(host=_host, port=_port)

    def _send_request(self, req_type, url, data=None, headers=None, attempt=1):
        req = requests.Request(req_type, url, data=data or {},
                               headers=headers)
        prepped = req.prepare()

        s = requests.Session()
        retry = Retry(
            total=self.total_retries,
            read=self.total_retries,
            connect=self.total_retries,
            backoff_factor=self.backoff_factor,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount('http://', adapter)
        settings = s.merge_environment_settings(prepped.url, {}, None, None,
                                                None)
        try:
            resp = s.send(prepped, timeout=self.timeout, **settings)
            resp.raise_for_status()
            return resp
        except exceptions.RequestException as e:
            log.error(str(e))
            if attempt > self.total_retries:
                raise e

            if self.uses_ssh and issubclass(
                    e.__class__, exceptions.ConnectionError):
                self._tunnels_container.reset()
            return self._send_request(
                req_type, url, data=data, headers=headers, attempt=attempt+1)

    def _request(self, req_type, path, data=None, host=None, json=True):
        headers = self.base_headers
        if host is not None:
            assert host in self._hosts, '{} is not part of the cluster!'\
                .format(host)
            headers.update({'Host': host})

        base_url = self._get_base_url(host)
        url = furl(base_url + path)
        url.add(data or {})

        resp = self._send_request(
            req_type, url.url, data=data or {}, headers=headers)
        return resp.json() if json else resp.text

    def _get(self, path, data=None, host=None, json=True):
        return self._request('GET', path, data=data, host=host, json=json)

    def _post(self, path, data, host=None, json=True):
        return self._request('POST', path, data=data, host=host, json=json)

    def cluster_name(self):
        return self._get(PATHS['cluster_name'])

    def endpoints_detailed(self):
        """
        Get all endpoint states
        "return: [
          {
            "update_time": 1552310205453,
            "generation": 0,
            "version": 0,
            "addrs": "10.210.20.127",
            "is_alive": true,
            "application_state": [..]
          },
          ...
        ]
        """
        endpoints = self._get(PATHS['endpoints'])
        self._hosts = [e['addrs'] for e in endpoints]

        return endpoints

    def endpoints_simple(self):
        """
        return: [
          {
            "key": "hostname",
            "value": "UP/DOWN"
          }
        ]
        """
        endpoints = self._get(PATHS['endpoints_simple'])
        self._hosts = [e['key'] for e in endpoints]

        return endpoints

    def tokens(self, endpoint):
        return self._get(PATHS['tokens'].format(endpoint=endpoint))

    def datacenter(self, endpoint):
        return self._get(PATHS['datacenter'], data={'host': endpoint})

    def describe_ring(self, keyspace):
        return self._get(PATHS['describe_ring'].format(keyspace=keyspace))

    def tables(self):
        return self._get(PATHS['column_family'])

    def repair_async(self, host, keyspace, table, start_token=None,
                     end_token=None, dc=None):
        data = {
            'keyspace': keyspace,
            'primaryRange': 'true',
            'parallelism': 0,
            'jobThreads': 1,
            'startToken': start_token,
            'endToken': end_token,
            'columnFamilies': table,
            'dataCenters': dc,
            'trace': "'true'"
        }
        return self._post(PATHS['repair_async'].format(keyspace=keyspace),
                          data=data, host=host, json=False)

    def repair_status(self, host, keyspace, repair_id):
        return self._get(PATHS['repair_async'].format(keyspace=keyspace),
                         data={'id': repair_id}, host=host, json=False)

    def active_repair(self, host):
        return self._get(PATHS['active_repair'], host=host)
