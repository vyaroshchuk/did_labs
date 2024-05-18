import json
import sys

import hazelcast

from base import BaseRequestHandler
from base import run_server

HZ_CLIENT = None


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


if __name__ == '__main__':
    port = int(sys.argv[1])
    hz_port = int(sys.argv[2])
    HZ_CLIENT = hazelcast.HazelcastClient(cluster_members=[f'192.168.0.107:{hz_port}'], cluster_name='hello-world')
    print(f'Starting logging server on port {port} with hz node on port {hz_port}')
    run_server(port, LoggingRequestHandler)
    HZ_CLIENT.shutdown()

