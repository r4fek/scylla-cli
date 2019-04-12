import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from furl import furl

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


class ApiClient(object):
    def __init__(self, endpoint, timeout=(5, 5), max_retries=10, backoff_factor=5):
        self.base_url = endpoint
        self.base_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._hosts = []

    def _request(self, req_type, path, data=None, host=None, json=True):
        headers = self.base_headers
        
        if host is not None:
            headers.update({'Host': host})

        url = furl(self.base_url + path)
        url.add(data or {})

        s = requests.Session()
        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            backoff_factor=self.backoff_factor,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount('http://', adapter)
        req = requests.Request(req_type, url)
        prepped = s.prepare_request(req)
        settings = s.merge_environment_settings(prepped.url, {}, None, None, None)
        resp = s.send(prepped, **settings)

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
