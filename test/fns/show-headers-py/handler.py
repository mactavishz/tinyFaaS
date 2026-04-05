from fastapi import Request
from fastapi.responses import JSONResponse


async def handle(request: Request) -> JSONResponse:
    print("Received request with headers:", request.headers)
    return JSONResponse(content=dict(request.headers))
