from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.errors import GatewayError


async def handle_gateway_error(
    request: Request,
    exc: GatewayError,
) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": {"code": exc.code, "message": exc.message}},
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(GatewayError, handle_gateway_error)
