# https://github.com/scylladb/scylla/blob/b66f59aa3d19cb94c73a71c5d454c1cc53b85baf/gms/application_state.cc#L49
NUM_TO_STATE = {
    0: 'status',
    1: 'load',
    2: 'schema',
    3: 'dc',
    4: 'rack',
    5: 'release_version',
    6: 'removal_coordinator',
    7: 'internal_ip',
    8: 'rpc_address',
    9: 'severity',
    10: 'net_version',
    11: 'xxx',
    12: 'host_id',
    13: 'tokens',
    14: 'supported_features',
    15: 'cache_hitrates',
    16: 'schema_tables_version',
    17: 'rpc_ready',
    18: 'view_backlog',
}


def parse_application_state(state):
    parsed_state = {}
    for s in state:
        parsed_state[NUM_TO_STATE[s['application_state']]] = s['value']

    # do some additional cleanup ;)
    parsed_state['status'] = parsed_state['status'].split(',')[0]
    if 'load' in parsed_state:
        parsed_state['load'] = int(float(parsed_state['load']))
    else:
        parsed_state['load'] = 0
    parsed_state['tokens'] = parsed_state['tokens'].split(';')
    parsed_state['supported_features'] = \
        parsed_state['supported_features'].split(',')
    parsed_state['cache_hitrates'] = parsed_state['cache_hitrates'].split(';')

    return parsed_state
