class GatewayError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 502) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class RetryableProviderError(Exception):
    """A temporary upstream failure that may succeed when retried."""
