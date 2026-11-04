"""Parse the explicit DATA-step ORDER BY extension."""

from __future__ import annotations

import re

OrderKey = tuple[str, bool]

_HEADER = re.compile(r"order\s+by\s+(.+)", re.IGNORECASE | re.DOTALL)
_KEY = re.compile(
    r"([A-Za-z_]\w*)(?:\s+(asc|ascending|desc|descending))?",
    re.IGNORECASE,
)


class OrderByError(ValueError):
    """Raised when a DATA-step ORDER BY extension is malformed."""


def parse_order_by(text: str) -> tuple[OrderKey, ...] | None:
    """Return ORDER BY columns and directions, or None for another statement."""
    statement = text.strip().rstrip(";").strip()
    if not re.match(r"order\s+by\b", statement, re.IGNORECASE):
        return None
    header = _HEADER.fullmatch(statement)
    if header is None:
        raise OrderByError("DATA-step ORDER BY requires simple column names.")

    keys: list[OrderKey] = []
    for token in header.group(1).split(","):
        match = _KEY.fullmatch(token.strip())
        if match is None:
            raise OrderByError(
                "DATA-step ORDER BY supports comma-separated simple column names "
                "with optional ASCENDING or DESCENDING direction."
            )
        direction = (match.group(2) or "asc").casefold()
        keys.append((match.group(1), direction not in {"desc", "descending"}))
    if not keys:
        raise OrderByError("DATA-step ORDER BY requires at least one column.")
    if len({name.casefold() for name, _ in keys}) != len(keys):
        raise OrderByError("DATA-step ORDER BY columns must be distinct.")
    return tuple(keys)


def order_by_sql(keys: tuple[OrderKey, ...]) -> str:
    """Render parsed keys as a Spark SQL ORDER BY expression."""
    rendered = ", ".join(
        f"`{name}` {'asc' if ascending else 'desc'}"
        for name, ascending in keys
    )
    return f"order by {rendered}"
