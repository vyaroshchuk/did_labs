from http.server import BaseHTTPRequestHandler, HTTPServer


class BaseRequestHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def write_response(self, status_code, content_type, content):
        response = content.encode('utf-8')

        self.send_response(status_code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def read_body(self):
        try:
            content_length = int(self.headers['Content-Length'])
            return self.rfile.read(content_length).decode('utf-8')
        except Exception: # noqa
            return None


def run_server(port, request_handler):
    httpd = HTTPServer(('', port), request_handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()