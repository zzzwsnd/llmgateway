class GatewayError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 502) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class RetryableProviderError(Exception):
    """A temporary upstream failure that may succeed when retried."""

    def __init__(self, message: str, *, resume_token: str | None = None) -> None:
        self.resume_token = resume_token
        super().__init__(message)


class JsonOutputValidationError(ValueError):
    def __init__(
        self,
        code: str,
        retry_prompt: str,
        *,
        missing_parameters: tuple[str, ...] = (),
        invalid_parameters: tuple[str, ...] = (),
    ) -> None:
        self.code = code
        self.retry_prompt = retry_prompt
        self.missing_parameters = missing_parameters
        self.invalid_parameters = invalid_parameters
        super().__init__(code)


class InvalidJsonSchemaError(ValueError):
    """The caller supplied a document that is not a valid JSON Schema."""
