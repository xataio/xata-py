class UnauthorizedException(Exception):
    pass


class RateLimitException(Exception):
    pass


class RecordNotFoundException(Exception):
    id: str

    def __init__(self, id: str):
        self.id = id

    def __str__(self) -> str:
        return f"Record with id {self.id} not found"


class BadRequestException(Exception):
    status_code: int
    message: str

    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f"Bad request: {self.status_code} {self.message}"
