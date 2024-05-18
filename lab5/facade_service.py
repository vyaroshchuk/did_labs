import json
import random
import uuid
import consul

import hazelcast
import requests

from base import BaseRequestHandler
from base import run_server


LOGGING_URLS = []
MESSAGES_URLS = []
CONSUL_SERVICE = consul.Consul()

HZ_CLIENT = None
QUEUE = None


class FacadeRequestHandler(BaseRequestHandler):
    def do_GET(self):
        logging_resp = requests.get(random.choice(LOGGING_URLS))
        messages_resp = requests.get(random.choice(MESSAGES_URLS))

        content = f'{logging_resp.content.decode('utf-8')}|{messages_resp.content.decode('utf-8')}'
        self.write_response(200, 'text/plain', content)

    def do_POST(self):
        body = self.read_body()
        msg_id = str(uuid.uuid4())

        QUEUE.offer(body)
        requests.post(
            random.choice(LOGGING_URLS),
            data=json.dumps({msg_id: body}),
            headers={'Content-Type': 'application/json'}
        )

        self.write_response(200, 'text/plain', 'OK')


def init_config():
    _, host_data = CONSUL_SERVICE.kv.get('host')
    host = host_data['Value'].decode('utf-8')

    _, lp_keys = CONSUL_SERVICE.kv.get('logging/port', keys=True)
    for k in lp_keys:
        _, data = CONSUL_SERVICE.kv.get(k)
        port = int(data['Value'])
        LOGGING_URLS.append(f'http://{host}:{port}')
    print(f'Logging services urls: {LOGGING_URLS}')

    _, mp_keys = CONSUL_SERVICE.kv.get('messages/port', keys=True)
    for k in mp_keys:
        _, data = CONSUL_SERVICE.kv.get(k)
        port = int(data['Value'])
        MESSAGES_URLS.append(f'http://{host}:{port}')
    print(f'Messages services urls {MESSAGES_URLS}')

    _, data = CONSUL_SERVICE.kv.get('hz/host')
    hz_host = data['Value'].decode('utf-8')
    _, data = CONSUL_SERVICE.kv.get('hz/cluster_name')
    cluster_name = data['Value'].decode('utf-8')
    print(f'Hazelcast config: {hz_host}, {cluster_name}')
    global HZ_CLIENT, QUEUE
    HZ_CLIENT = hazelcast.HazelcastClient(cluster_members=[hz_host], cluster_name=cluster_name)
    _, data = CONSUL_SERVICE.kv.get('messages/queue_name')
    queue_name = data['Value'].decode('utf-8')
    QUEUE = HZ_CLIENT.get_queue(queue_name)
    print(f'Queue name: {queue_name}')


if __name__ == '__main__':
    CONSUL_SERVICE.agent.service.register('facade')
    init_config()
    print('List of running services:')
    for s in CONSUL_SERVICE.agent.services():
        print(s)
    run_server(8000, FacadeRequestHandler)
    HZ_CLIENT.shutdown()
    CONSUL_SERVICE.agent.service.deregister('facade')
