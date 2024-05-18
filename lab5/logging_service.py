import json
import sys

import consul
import hazelcast

from base import BaseRequestHandler
from base import run_server

HZ_CLIENT = None
SERVER_PORT = None
CONSUL_SERVICE = consul.Consul()


class LoggingRequestHandler(BaseRequestHandler):
    def do_GET(self):
        uuid_msg_map = HZ_CLIENT.get_map('uuid_msg').blocking()
        self.write_response(200,'text/plain',';'.join(uuid_msg_map.values()))

    def do_POST(self):
        body = self.read_body()
        uuid_msg_map = HZ_CLIENT.get_map('uuid_msg').blocking()
        data = json.loads(body)
        for k, v in data.items():
            uuid_msg_map.put(k, v)
        print(body)
        self.write_response(200, 'text/plain', 'OK')


def init_config(inst_num):
    _, data = CONSUL_SERVICE.kv.get(f'logging/port{inst_num}')
    global SERVER_PORT
    SERVER_PORT = int(data['Value'])
    print(SERVER_PORT)
    _, data = CONSUL_SERVICE.kv.get('hz/host')
    hz_host = data['Value'].decode('utf-8')

    _, data = CONSUL_SERVICE.kv.get(f'hz/port{inst_num}')
    hz_port = int(data['Value'])

    _, data = CONSUL_SERVICE.kv.get('hz/cluster_name')
    cluster_name = data['Value'].decode('utf-8')

    print(f'Hazelcast config: {hz_host}:{hz_port}, {cluster_name}')
    global HZ_CLIENT
    HZ_CLIENT = hazelcast.HazelcastClient(cluster_members=[f'{hz_host}:{hz_port}'], cluster_name=cluster_name)


if __name__ == '__main__':
    inst_num = int(sys.argv[1])
    CONSUL_SERVICE.agent.service.register('logging', service_id=f'logging-{inst_num}')
    init_config(inst_num)
    print('Running on port', SERVER_PORT)
    run_server(SERVER_PORT, LoggingRequestHandler)
    HZ_CLIENT.shutdown()
    CONSUL_SERVICE.agent.service.deregister(f'logging-{inst_num}')

