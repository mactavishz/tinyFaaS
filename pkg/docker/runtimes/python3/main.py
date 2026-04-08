#!/usr/bin/env python
from flask import Flask, Response, jsonify, request
from waitress import serve

try:
    import handler
except ImportError as exc:
    raise ImportError("Failed to import handler.py") from exc

app = Flask(__name__)

def format_status_code(res):
    if 'statusCode' in res:
        return res['statusCode']
    return 200

def format_body(res, content_type):
    if content_type == 'application/octet-stream':
        return res['body']

    if 'body' not in res:
        return ""
    elif type(res['body']) is dict:
        return jsonify(res['body'])
    else:
        return str(res['body'])

def format_headers(res):
    if 'headers' not in res:
        return []
    elif type(res['headers']) is dict:
        headers = []
        for key in res['headers'].keys():
            header_tuple = (key, res['headers'][key])
            headers.append(header_tuple)
        return headers

def format_response(res):
    statusCode = 200
    content_type = ""

    if res is None:
        return Response('', status=statusCode)

    if type(res) is dict:
        if 'headers' in res:
            content_type = res['headers'].get('Content-type', '')
        if 'statusCode' in res:
            statusCode = res['statusCode']
        body = format_body(res, content_type)
        headers = format_headers(res)

        return Response(body, status=statusCode, headers=headers)

    return res


@app.get("/health")
def health() -> Response:
    return Response("OK", status=200, mimetype="text/plain")


@app.post("/fn")
def invoke() -> Response:
    res = handler.handle(request)
    return format_response(res)

if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=8000)