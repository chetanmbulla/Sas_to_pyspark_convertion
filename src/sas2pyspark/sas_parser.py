"""Source-aware lexical parsing for SAS programs.

This module provides the token and statement foundation for the SAS grammar.
It intentionally does not claim to parse SAS DATA-step or PROC semantics yet.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class SourceSpan:
    start_offset: int
    end_offset: int
    start_line: int
    start_column: int
    end_line: int
    end_column: int


@dataclass(frozen=True)
class SasToken:
    kind: str
    text: str
    span: SourceSpan


@dataclass(frozen=True)
class SasStatement:
    text: str
    tokens: tuple[SasToken, ...]
    span: SourceSpan


@dataclass(frozen=True)
class SasProgram:
    statements: tuple[SasStatement, ...]


class SasParseError(ValueError):
    def __init__(
        self, message: str, *, line: int, statement: str | None = None
    ) -> None:
        self.message = message
        self.line = line
        self.statement = statement
        location = f"SAS line {line}: "
        context = f"\n  > {statement.strip()}" if statement else ""
        super().__init__(f"{location}{message}{context}")


_NUMBER = re.compile(
    r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
)
_IDENTIFIER_START = re.compile(r"[A-Za-z_]")
_IDENTIFIER_PART = re.compile(r"\w")
_OPERATORS = (
    "**",
    "<=",
    ">=",
    "^=",
    "~=",
    "!=",
    "==",
    "<>",
    "||",
    "=>",
    "=<",
)
_PUNCTUATION = set(";,()[]{}.+-*/=<>:&%!|^~")


def _mask_comments(source: str) -> str:
    output = list(source)
    index = 0
    line = 1
    quote: str | None = None
    quote_line: int | None = None
    statement_start = True
    while index < len(source):
        char = source[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(source) and source[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            if char == "\n":
                line += 1
            index += 1
            continue

        if char in {"'", '"'}:
            quote = char
            quote_line = line
            statement_start = False
            index += 1
            continue

        if source.startswith("/*", index):
            end = source.find("*/", index + 2)
            if end < 0:
                raise SasParseError(
                    "Unterminated SAS block comment.", line=line
                )
            for position in range(index, end + 2):
                if output[position] != "\n":
                    output[position] = " "
                else:
                    line += 1
            index = end + 2
            continue

        if char == "*" and statement_start:
            end = source.find(";", index + 1)
            if end < 0:
                raise SasParseError(
                    "Unterminated SAS statement comment.", line=line
                )
            for position in range(index, end):
                if output[position] != "\n":
                    output[position] = " "
                else:
                    line += 1
            index = end
            continue

        if char == ";":
            statement_start = True
        elif not char.isspace():
            statement_start = False
        if char == "\n":
            line += 1
        index += 1

    if quote is not None:
        raise SasParseError(
            "Unterminated quoted string.", line=quote_line or line
        )
    return "".join(output)


def _position(source: str, offset: int) -> tuple[int, int]:
    line = source.count("\n", 0, offset) + 1
    line_start = source.rfind("\n", 0, offset) + 1
    return line, offset - line_start + 1


def _tokenize_statement(text: str, offset: int, source: str) -> tuple[SasToken, ...]:
    tokens: list[SasToken] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char.isspace():
            index += 1
            continue

        start = index
        if char in {"'", '"'}:
            quote = char
            index += 1
            while index < len(text):
                if text[index] == quote:
                    if index + 1 < len(text) and text[index + 1] == quote:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            kind = "STRING"
        elif char == "&":
            index += 1
            while index < len(text) and text[index] == "&":
                index += 1
            if index < len(text) and _IDENTIFIER_START.fullmatch(text[index]):
                index += 1
                while index < len(text) and _IDENTIFIER_PART.fullmatch(text[index]):
                    index += 1
                if index < len(text) and text[index] == ".":
                    index += 1
                kind = "MACRO_VARIABLE"
            else:
                kind = "PUNCTUATION"
        elif char == "%" and index + 1 < len(text) and _IDENTIFIER_START.fullmatch(
            text[index + 1]
        ):
            index += 2
            while index < len(text) and _IDENTIFIER_PART.fullmatch(text[index]):
                index += 1
            kind = "MACRO_DIRECTIVE"
        elif _IDENTIFIER_START.fullmatch(char):
            index += 1
            while index < len(text) and _IDENTIFIER_PART.fullmatch(text[index]):
                index += 1
            kind = "IDENTIFIER"
        else:
            number = _NUMBER.match(text, index)
            if number is not None:
                index = number.end()
                kind = "NUMBER"
            else:
                operator = next(
                    (
                        item
                        for item in _OPERATORS
                        if text.startswith(item, index)
                    ),
                    None,
                )
                if operator is not None:
                    index += len(operator)
                    kind = "OPERATOR"
                elif char in _PUNCTUATION:
                    index += 1
                    kind = "PUNCTUATION"
                else:
                    index += 1
                    kind = "OTHER"

        absolute_start = offset + start
        absolute_end = offset + index
        start_line, start_column = _position(source, absolute_start)
        end_line, end_column = _position(source, absolute_end)
        tokens.append(
            SasToken(
                kind,
                text[start:index],
                SourceSpan(
                    absolute_start,
                    absolute_end,
                    start_line,
                    start_column,
                    end_line,
                    end_column,
                ),
            )
        )
    return tuple(tokens)


def parse_program(source: str) -> SasProgram:
    """Lex SAS source into semicolon-terminated statements with source spans."""
    clean_source = _mask_comments(source)
    statements: list[SasStatement] = []
    start = 0
    index = 0
    quote: str | None = None
    while index < len(clean_source):
        char = clean_source[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(clean_source) and clean_source[index + 1] == quote:
                    index += 2
                    continue
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == ";":
            raw_text = clean_source[start:index]
            text = raw_text.strip()
            if text:
                leading = len(raw_text) - len(raw_text.lstrip())
                statement_offset = start + leading
                start_line, start_column = _position(clean_source, statement_offset)
                end_line, end_column = _position(clean_source, index)
                span = SourceSpan(
                    statement_offset,
                    index + 1,
                    start_line,
                    start_column,
                    end_line,
                    end_column + 1,
                )
                statements.append(
                    SasStatement(
                        text,
                        _tokenize_statement(text, statement_offset, clean_source),
                        span,
                    )
                )
            start = index + 1
        index += 1

    tail = clean_source[start:]
    if tail.strip():
        leading = len(tail) - len(tail.lstrip())
        statement_offset = start + leading
        line, _ = _position(clean_source, statement_offset)
        raise SasParseError(
            "Every SAS statement must end with a semicolon.",
            line=line,
            statement=tail.strip(),
        )
    return SasProgram(tuple(statements))
