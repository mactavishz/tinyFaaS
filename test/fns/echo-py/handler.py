from fastapi import Request
from fastapi.responses import Response


async def handle(request: Request) -> Response:
    print("Received request with headers:", request.headers)
    body = await request.body()
    return Response(content=body, media_type="text/plain")
