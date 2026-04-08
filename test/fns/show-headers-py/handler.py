import json


def handle(request):
    print("Received request with headers:", request.headers)
    headers = {key.lower(): value for key, value in request.headers.items()}
    return {
        "body": json.dumps(headers),
        "headers": {
            "Content-Type": "application/json"
        }
    }
