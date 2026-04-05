from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, Response

try:
    import handler  # type: ignore
except ImportError as exc:
    raise ImportError("Failed to import handler.py") from exc

app = FastAPI(
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.router.redirect_slashes = False


@app.get("/health")
async def health() -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.post("/fn")
async def invoke(request: Request) -> Response:
    try:
        return await handler.handle(request)
    except Exception as exc:
        return PlainTextResponse(str(exc), status_code=500)
