import inspect

from openai import APIConnectionError, APITimeoutError, RateLimitError

from app.core.errors import RetryableProviderError
from app.core.logging import logger


OPENAI_RETRYABLE_ERRORS = (
    APIConnectionError,
    APITimeoutError,
    RateLimitError,
    TimeoutError,
    ConnectionError,
)


def raise_retryable_provider_error(error: Exception) -> None:
    if isinstance(error, OPENAI_RETRYABLE_ERRORS):
        raise RetryableProviderError(str(error)) from error
    raise error


async def close_provider_stream(stream: object) -> None:
    close = getattr(stream, "aclose", None) or getattr(stream, "close", None)
    if close is None:
        return
    try:
        result = close()
        if inspect.isawaitable(result):
            await result
    except Exception:
        logger.warning("failed to close upstream provider stream", exc_info=True)
