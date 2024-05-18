import sys
import threading

import hazelcast

from base import BaseRequestHandler
from base import run_server

HZ_CLIENT = hazelcast.HazelcastClient(cluster_members=['192.168.0.107'], cluster_name='hello-world')
QUEUE = HZ_CLIENT.get_queue("default")
MESSAGES = []


class MessagesRequestHandler(BaseRequestHandler):
    def do_GET(self):
        self.write_response(200, 'text/plain', ';'.join(MESSAGES))


def consume():
    while True:
        head = QUEUE.take().result()
        print("Consumed message {}".format(head))
        MESSAGES.append(head)


if __name__ == '__main__':
    port = int(sys.argv[1])
    consumer_thread = threading.Thread(target=consume)
    consumer_thread.start()
    print(f'Starting messaging server on port {port}')
    run_server(port, MessagesRequestHandler)
    HZ_CLIENT.shutdown()
