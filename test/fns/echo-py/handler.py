def handle(request):
    print("Received request with headers:", request.headers)
    return {
        "body": request.get_data(as_text=True),
        "headers": {
            "Content-Type": "text/plain"
        }
    }
