import sys
import threading

import consul
import hazelcast

from base import BaseRequestHandler
from base import run_server

CONSUL_SERVICE = consul.Consul()
HZ_CLIENT = None
QUEUE = None
SERVER_PORT = None
MESSAGES = []


class MessagesRequestHandler(BaseRequestHandler):
    def do_GET(self):
        self.write_response(200, 'text/plain', ';'.join(MESSAGES))


def consume():
    while True:
        head = QUEUE.take().result()
        print("Consumed message {}".format(head))
        MESSAGES.append(head)


def init_config(inst_num):
    _, data = CONSUL_SERVICE.kv.get(f'messages/port{inst_num}')
    global SERVER_PORT
    SERVER_PORT = int(data['Value'])

    _, host_data = CONSUL_SERVICE.kv.get('hz/host')
    hz_host = host_data['Value'].decode('utf-8')

    _, data = CONSUL_SERVICE.kv.get('hz/cluster_name')
    cluster_name = data['Value'].decode('utf-8')

    print(f'Hazelcast config: {hz_host}, {cluster_name}')

    global HZ_CLIENT
    HZ_CLIENT = hazelcast.HazelcastClient(cluster_members=[hz_host], cluster_name=cluster_name)

    global QUEUE
    _, data = CONSUL_SERVICE.kv.get('messages/queue_name')
    queue_name = data['Value'].decode('utf-8')
    QUEUE = HZ_CLIENT.get_queue(queue_name)
    print(f'Queue name: {queue_name}')


if __name__ == '__main__':
    inst_num = int(sys.argv[1])
    CONSUL_SERVICE.agent.service.register('messages', service_id=f'messages-{inst_num}')
    init_config(inst_num)
    consumer_thread = threading.Thread(target=consume)
    consumer_thread.start()
    print(f'Starting messaging server on port {SERVER_PORT}')
    run_server(SERVER_PORT, MessagesRequestHandler)
    CONSUL_SERVICE.agent.service.deregister(f'messages-{inst_num}')
    HZ_CLIENT.shutdown()

