#!/usr/bin/env python3

import json
import typing
import http.server
import socketserver

if __name__ == "__main__":
    try:
        import handler  # type: ignore
    except ImportError:
        raise ImportError("Failed to import handler.py")

    def format_response(res: typing.Any) -> typing.Tuple[str, str]:
        """
        Format the response from handler.handle().
        Returns a tuple of (body, content_type).
        
        - If res is a dict, serialize to JSON
        - If res is a valid JSON string, return as-is with JSON content type
        - Otherwise, return as raw string
        """
        if res is None:
            return "", "text/plain"
        
        # If it's a dict, serialize to JSON
        if isinstance(res, dict):
            return json.dumps(res), "application/json"
        
        # Convert to string if not already
        res_str = str(res)
        
        # Check if it's a valid JSON string
        try:
            json.loads(res_str)
            return res_str, "application/json"
        except (json.JSONDecodeError, TypeError):
            pass
        
        # Return as raw string
        return res_str, "text/plain"

    # create a webserver at port 8080 and execute fn.fn for every request
    class tinyFaaSFNHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            print(f"GET {self.path}")
            if self.path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write("OK".encode("utf-8"))
                print("reporting health: OK")
                return

            self.send_response(404)
            self.end_headers()
            return

        def do_POST(self) -> None:
            d: typing.Optional[str] = self.rfile.read(
                int(self.headers["Content-Length"])
            ).decode("utf-8")
            if d == "":
                d = None

            # Read headers into a dictionary
            headers: typing.Dict[str, str] = {k: v for k, v in self.headers.items()}

            try:
                res = handler.handle(d, headers)
                body, content_type = format_response(res)
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.end_headers()
                if body:
                    self.wfile.write(body.encode("utf-8"))

                return
            except Exception as e:
                print(e)
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode("utf-8"))
                return

    with socketserver.ThreadingTCPServer(("", 8000), tinyFaaSFNHandler) as httpd:
        httpd.serve_forever()
