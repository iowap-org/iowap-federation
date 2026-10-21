"""Federation-specific errors (T-109)."""

from __future__ import annotations


class FederationError(Exception):
    """Base class for federation errors."""


class TaskTooLargeForRoute(FederationError):
    """The task exceeds the declared size limit of the addressed transport.

    This is a **terminal** condition: retrying the same oversized payload
    cannot succeed, so the message must not be re-queued. The transport
    declares its limit in ``registry/<name>.yaml`` (``max_task_bytes``).
    """

    def __init__(self, route: str, size: int, limit: int) -> None:
        self.route = route
        self.size = size
        self.limit = limit
        super().__init__(
            f"task of {size} bytes exceeds the limit of {limit} bytes "
            f"for route {route!r}"
        )