from base import BaseRequestHandler
from base import run_server


class MessagesRequestHandler(BaseRequestHandler):
    def do_GET(self):
        self.write_response(501, 'text/plain', f'Not implemented yet')


if __name__ == '__main__':
    run_server(8001, MessagesRequestHandler)
