"""Route parsing for federation messages (T-109).

A route addresses one recipient over one transport: ``<transport>~<recipient>``
(e.g. ``imsg~ronny.pietschke@icloud.com``). The separator is ``~`` — the same
convention the Hermes bridge adapter uses, but implemented independently:
the federation node is its own platform and does not import that code.

The transport segment becomes a **directory name** in the file contract
(``outbox/<transport>/``), so it is validated as an identifier: no path
separators, no traversal, no whitespace.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: Separator between the transport segment and the recipient segment.
ROUTE_SEPARATOR = "~"

#: Transport segments are used as directory names -> identifier-like only.
_TRANSPORT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class RouteError(ValueError):
    """Raised when a route string is malformed."""


@dataclass(frozen=True)
class Route:
    """One addressable destination: a transport plus the recipient on it.

    ``transport`` is the transport segment (``imsg``, ``email``, ``nextcloud``);
    ``recipient`` is the address on that transport (an e-mail address, a chat id,
    a username). The two are independent: the same recipient may be reachable
    over several transports, each with its own route.
    """

    transport: str
    recipient: str

    def __post_init__(self) -> None:
        if not self.transport or not _TRANSPORT_RE.match(self.transport):
            raise RouteError(
                f"invalid transport segment: {self.transport!r} "
                "(expected an identifier: letters, digits, '.', '_', '-')"
            )
        if not self.recipient:
            raise RouteError("recipient must not be empty")
        if ROUTE_SEPARATOR in self.recipient:
            raise RouteError(
                f"recipient must not contain {ROUTE_SEPARATOR!r}: {self.recipient!r}"
            )

    def __str__(self) -> str:
        return f"{self.transport}{ROUTE_SEPARATOR}{self.recipient}"


def parse_route(raw: str) -> Route:
    """Parse ``"<transport>~<recipient>"`` into a :class:`Route`.

    Raises :class:`RouteError` when the separator is missing or the segments
    are invalid.
    """
    if not raw or not isinstance(raw, str):
        raise RouteError("route must be a non-empty string")
    if ROUTE_SEPARATOR not in raw:
        raise RouteError(
            f"route must contain the {ROUTE_SEPARATOR!r} separator: {raw!r}"
        )
    transport, _, recipient = raw.partition(ROUTE_SEPARATOR)
    return Route(transport=transport.strip(), recipient=recipient.strip())