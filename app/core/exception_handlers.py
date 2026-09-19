from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.core.errors import GatewayError
from app.core.logging import logger


_NATIVE_PATHS = ("/v1/chat/completions", "/v1/responses", "/v1/models")


def _is_native_request(request: Request) -> bool:
    return request.url.path in _NATIVE_PATHS


def _native_error(
    *, message: str, error_type: str, code: str, param: str | None = None
) -> dict[str, object]:
    return {
        "error": {
            "message": message,
            "type": error_type,
            "param": param,
            "code": code,
        }
    }


async def handle_gateway_error(
    request: Request,
    exc: GatewayError,
) -> JSONResponse:
    if _is_native_request(request):
        error_type = (
            "invalid_request_error" if 400 <= exc.status_code < 500 else "server_error"
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=_native_error(
                message=exc.message,
                error_type=error_type,
                code=exc.code,
            ),
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": {"code": exc.code, "message": exc.message}},
    )


async def handle_request_validation_error(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    if not _is_native_request(request):
        return await request_validation_exception_handler(request, exc)

    first_error = exc.errors()[0] if exc.errors() else {}
    location = [str(part) for part in first_error.get("loc", ()) if part != "body"]
    param = ".".join(location) or None
    description = str(first_error.get("msg", "Request validation failed"))
    message = f"Invalid value for {param}: {description}" if param else description
    return JSONResponse(
        status_code=422,
        content=_native_error(
            message=message,
            error_type="invalid_request_error",
            code="validation_error",
            param=param,
        ),
    )


async def handle_unexpected_error(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    logger.error(
        "unhandled gateway error",
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    if _is_native_request(request):
        return JSONResponse(
            status_code=500,
            content=_native_error(
                message="Internal server error",
                error_type="server_error",
                code="internal_server_error",
            ),
        )
    return JSONResponse(
        status_code=500,
        content={
            "detail": {
                "code": "internal_server_error",
                "message": "服务内部错误",
            }
        },
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(GatewayError, handle_gateway_error)
    app.add_exception_handler(RequestValidationError, handle_request_validation_error)
    app.add_exception_handler(Exception, handle_unexpected_error)
