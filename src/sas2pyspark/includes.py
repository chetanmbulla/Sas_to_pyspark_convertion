"""Inline %INCLUDE 'file'; statements before parsing."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Mapping

_INCLUDE = re.compile(
    r"^[ \t]*%include[ \t]+(['\"])(.*?)\1[ \t]*;[ \t]*\r?$", re.IGNORECASE | re.MULTILINE
)
_ANY_INCLUDE = re.compile(r"^[ \t]*%include\b", re.IGNORECASE | re.MULTILINE)
_DRIVE_PATH = re.compile(r"^([A-Za-z]):(?:[\\/]+(.*))?$")
_LET = re.compile(
    r"^[ \t]*%let[ \t]+([A-Za-z_]\w*)[ \t]*=[ \t]*([^;\r\n]*);",
    re.IGNORECASE | re.MULTILINE,
)
_FILENAME = re.compile(
    r"^[ \t]*filename[ \t]+([A-Za-z_]\w*)[ \t]+(['\"])(.*?)\2[ \t]*;", re.IGNORECASE | re.MULTILINE
)
_FILEREF_INCLUDE = re.compile(
    r"^[ \t]*%include[ \t]+([A-Za-z_]\w*)\(([^)\r\n]+)\)[ \t]*;[ \t]*\r?$",
    re.IGNORECASE | re.MULTILINE,
)
_VARIABLE = re.compile(r"&([A-Za-z_]\w*)\.?")
_MAX_DEPTH = 10


class IncludeError(ValueError):
    def __init__(self, message: str, line: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.line = line


def inline_includes(
    source: str,
    include_dir: Path | str | None,
    include_root: Path | str | None = None,
    drives: Mapping[str, Path | str] | None = None,
    macro_variables: Mapping[str, str] | None = None,
) -> str:
    """Replace literal %INCLUDE statements with the referenced file contents.

    Included files must live under include_root (default include_dir), and
    relative paths resolve against the including file's directory. Paths with a
    drive letter resolve through ``drives`` (letter -> directory) when given.
    """
    if not _ANY_INCLUDE.search(source):
        return source
    if include_dir is None:
        raise IncludeError(
            "%INCLUDE requires a source directory; convert from a file or pass include_dir."
        )
    base = Path(include_dir).resolve()
    root = Path(include_root).resolve() if include_root is not None else base
    mapped = {
        letter.rstrip(":").casefold(): Path(path).resolve()
        for letter, path in (drives or {}).items()
    }
    seed = {name.casefold(): value for name, value in (macro_variables or {}).items()}
    return _expand(source, base, root, (), 0, seed, mapped)


def _expand(
    source: str,
    base: Path,
    root: Path,
    stack: tuple[Path, ...],
    depth: int,
    variables: dict[str, str],
    drives: Mapping[str, Path],
) -> str:
    if depth > _MAX_DEPTH:
        raise IncludeError(f"%INCLUDE nesting is deeper than {_MAX_DEPTH} levels.")
    output: list[str] = []
    cursor = 0
    for match in _ANY_INCLUDE.finditer(source):
        for let in _LET.finditer(source, cursor, match.start()):
            variables[let.group(1).casefold()] = _resolve_variables(
                let.group(2).strip(), variables, let.start(), source
            )
        for filename in _FILENAME.finditer(source, cursor, match.start()):
            variables["filename:" + filename.group(1).casefold()] = _resolve_variables(
                filename.group(3).strip(), variables, filename.start(), source
            )
        include = _INCLUDE.match(source, match.start())
        line = source.count("\n", 0, match.start()) + 1
        member = _FILEREF_INCLUDE.match(source, match.start())
        if include is None and member is not None:
            directory = variables.get("filename:" + member.group(1).casefold())
            if directory is not None:
                include = member
                include_path = directory.rstrip("\\/") + "/" + member.group(2).strip()
        if include is None:
            raise IncludeError(
                "%INCLUDE requires a quoted path; dynamic paths must resolve from "
                "preceding %LET statements.",
                line=line,
            )
        try:
            if include is not member:
                include_path = _resolve_variables(
                    include.group(2), variables, match.start(), source
                )
        except IncludeError as error:
            raise IncludeError(error.message, line=line) from error
        line = source.count("\n", 0, match.start()) + 1
        drive = _DRIVE_PATH.match(include_path)
        if drive is not None:
            drive_root = drives.get(drive.group(1).casefold())
            if drive_root is None:
                raise IncludeError(
                    f"%INCLUDE drive {drive.group(1).upper()}: is not mapped: {include_path!r}.",
                    line=line,
                )
            target = (drive_root / (drive.group(2) or "").replace("\\", "/")).resolve()
        else:
            target = (base / include_path).resolve()
        try:
            target.relative_to(root)
        except ValueError:
            raise IncludeError(
                f"%INCLUDE path escapes the allowed directory: {include_path!r}.",
                line=line,
            ) from None
        if target in stack:
            raise IncludeError(
                f"Circular %INCLUDE of {include_path!r}.", line=line
            )
        try:
            text = target.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise IncludeError(
                f"Cannot read %INCLUDE file {include_path!r}: {error}", line=line
            ) from error
        output.append(source[cursor : match.start()])
        output.append(
            _expand(text, target.parent, root, stack + (target,), depth + 1, variables, drives)
        )
        cursor = include.end()
    output.append(source[cursor:])
    return "".join(output)


def _resolve_variables(
    text: str,
    variables: dict[str, str],
    position: int,
    source: str,
) -> str:
    for _ in range(10):
        matches = list(_VARIABLE.finditer(text))
        if not matches:
            return text
        for match in reversed(matches):
            name = match.group(1).casefold()
            if name not in variables:
                line = source.count("\n", 0, position) + 1
                raise IncludeError(
                    f"Undefined macro variable &{match.group(1)} in %INCLUDE path.",
                    line=line,
                )
            text = text[: match.start()] + variables[name] + text[match.end() :]
    line = source.count("\n", 0, position) + 1
    raise IncludeError("Indirect macro references in %INCLUDE exceeded the limit.", line)
