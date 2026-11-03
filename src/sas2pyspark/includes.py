"""Inline %INCLUDE 'file'; statements before parsing."""

from __future__ import annotations

import re
from pathlib import Path

_INCLUDE = re.compile(
    r"^[ \t]*%include[ \t]+(['\"])(.*?)\1[ \t]*;[ \t]*$", re.IGNORECASE | re.MULTILINE
)
_ANY_INCLUDE = re.compile(r"^[ \t]*%include\b", re.IGNORECASE | re.MULTILINE)
_MAX_DEPTH = 10


class IncludeError(ValueError):
    def __init__(self, message: str, line: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.line = line


def inline_includes(
    source: str, include_dir: Path | str | None, include_root: Path | str | None = None
) -> str:
    """Replace literal %INCLUDE statements with the referenced file contents.

    Included files must live under include_root (default include_dir), and
    relative paths resolve against the including file's directory.
    """
    if not _ANY_INCLUDE.search(source):
        return source
    if include_dir is None:
        raise IncludeError(
            "%INCLUDE requires a source directory; convert from a file or pass include_dir."
        )
    base = Path(include_dir).resolve()
    root = Path(include_root).resolve() if include_root is not None else base
    return _expand(source, base, root, (), 0)


def _expand(source: str, base: Path, root: Path, stack: tuple[Path, ...], depth: int) -> str:
    if depth > _MAX_DEPTH:
        raise IncludeError(f"%INCLUDE nesting is deeper than {_MAX_DEPTH} levels.")
    literal_spans = {match.start() for match in _INCLUDE.finditer(source)}
    for match in _ANY_INCLUDE.finditer(source):
        if match.start() not in literal_spans:
            line = source.count("\n", 0, match.start()) + 1
            raise IncludeError(
                "%INCLUDE supports only a quoted literal file path.", line=line
            )

    def replace(match: re.Match[str]) -> str:
        line = source.count("\n", 0, match.start()) + 1
        target = (base / match.group(2)).resolve()
        try:
            target.relative_to(root)
        except ValueError:
            raise IncludeError(
                f"%INCLUDE path escapes the allowed directory: {match.group(2)!r}.",
                line=line,
            ) from None
        if target in stack:
            raise IncludeError(
                f"Circular %INCLUDE of {match.group(2)!r}.", line=line
            )
        try:
            text = target.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise IncludeError(
                f"Cannot read %INCLUDE file {match.group(2)!r}: {error}", line=line
            ) from error
        return _expand(text, target.parent, root, stack + (target,), depth + 1)

    return _INCLUDE.sub(replace, source)
