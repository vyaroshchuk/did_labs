import json
import random
import uuid

import hazelcast
import requests

from base import BaseRequestHandler
from base import run_server

LOGGING1_URL = 'http://0.0.0.0:8002'
LOGGING2_URL = 'http://0.0.0.0:8003'
LOGGING3_URL = 'http://0.0.0.0:8004'
LOGGING_URLS = [LOGGING1_URL, LOGGING2_URL, LOGGING3_URL]
MESSAGES_URL = 'http://0.0.0.0:8001'

MESSAGES1_URL = 'http://0.0.0.0:8005'
MESSAGES2_URL = 'http://0.0.0.0:8006'
MESSAGES_URLS = [MESSAGES1_URL, MESSAGES2_URL]

HZ_CLIENT = hazelcast.HazelcastClient(cluster_members=['192.168.0.107'], cluster_name='hello-world')
QUEUE = HZ_CLIENT.get_queue("default")


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


if __name__ == '__main__':
    run_server(8000, FacadeRequestHandler)
    HZ_CLIENT.shutdown()