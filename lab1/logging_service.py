import json

from base import BaseRequestHandler
from base import run_server

UUID_MSG_MAP = dict()


class LoggingRequestHandler(BaseRequestHandler):
    def do_GET(self):
        self.write_response(200,'text/plain',';'.join(UUID_MSG_MAP.values()))

    def do_POST(self):
        body = self.read_body()
        UUID_MSG_MAP.update(json.loads(body))
        print(body)
        self.write_response(200, 'text/plain', 'OK')


if __name__ == '__main__':
    run_server(8001, LoggingRequestHandler)
