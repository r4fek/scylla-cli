import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from furl import furl


from .tunnel import SSHTunnelsContainer


class ApiClient:
    BASE_URL = 'http://localhost:{port}'
    BASE_HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }
    TIMEOUT = (5, 5)
    TOTAL_RETRIES = 10
    BACKOFF_FACTOR = 5
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

    def __init__(self):
        self._hosts = []
        self._tunnels_container = None

    def setup(self, initial_endpoint=None, ssh_username=None, ssh_pkey=None,
              ssh_pass=None):
        self._tunnels_container = SSHTunnelsContainer(
            ssh_username=ssh_username,
            ssh_pkey=ssh_pkey,
            ssh_pass=ssh_pass,
            initial_endpoint=initial_endpoint)

    def stop(self):
        self._tunnels_container.stop()

    def _get_session(self):
        s = requests.Session()
        retry = Retry(
            total=self.TOTAL_RETRIES,
            read=self.TOTAL_RETRIES,
            connect=self.TOTAL_RETRIES,
            backoff_factor=self.BACKOFF_FACTOR,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount('http://', adapter)
        return s

    def _request(self, req_type, path, data=None, host=None, json=True):
        headers = self.BASE_HEADERS
        if host is not None:
            assert host in self._hosts, '{} is not part of the cluster!'\
                .format(host)
            headers.update({'Host': host})

        port = self._tunnels_container.get_port(host=host)
        base_url = self.BASE_URL.format(port=port)
        url = furl(base_url + path)
        url.add(data or {})

        req = requests.Request(req_type, url.url, data=data or {},
                               headers=headers)
        prepped = req.prepare()
        resp = self._get_session().send(prepped, timeout=self.TIMEOUT)
        resp.raise_for_status()

        return resp.json() if json else resp.text

    def _get(self, path, data=None, host=None, json=True):
        return self._request('GET', path, data=data, host=host, json=json)

    def _post(self, path, data, host=None, json=True):
        return self._request('POST', path, data=data, host=host, json=json)

    def cluster_name(self):
        return self._get(self.PATHS['cluster_name'])

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
        endpoints = self._get(self.PATHS['endpoints'])
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
        endpoints = self._get(self.PATHS['endpoints_simple'])
        self._hosts = [e['key'] for e in endpoints]

        return endpoints

    def tokens(self, endpoint):
        return self._get(self.PATHS['tokens'].format(endpoint=endpoint))

    def datacenter(self, endpoint):
        return self._get(self.PATHS['datacenter'], data={'host': endpoint})

    def describe_ring(self, keyspace):
        return self._get(self.PATHS['describe_ring'].format(keyspace=keyspace))

    def tables(self):
        return self._get(self.PATHS['column_family'])

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
        return self._post(self.PATHS['repair_async'].format(keyspace=keyspace),
                          data=data, host=host, json=False)

    def repair_status(self, host, keyspace, repair_id):
        return self._get(self.PATHS['repair_async'].format(keyspace=keyspace),
                         data={'id': repair_id}, host=host, json=False)

    def active_repair(self, host):
        return self._get(self.PATHS['active_repair'], host=host)


client = ApiClient()
__all__ = ['client']
