"""SAS-to-PySpark conversion for a deliberately bounded SAS subset."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

from .macro_processor import (
    MacroExpansionError,
    MacroStatement as _Statement,
    expand_macros,
)


class ConversionError(ValueError):
    """Raised when source syntax is outside the converter's supported subset."""

    def __init__(
        self, message: str, *, line: int | None = None, statement: str | None = None
    ) -> None:
        self.message = message
        self.line = line
        self.statement = statement
        location = f"SAS line {line}: " if line is not None else ""
        context = f"\n  > {statement.strip()}" if statement else ""
        super().__init__(f"{location}{message}{context}")


@dataclass(frozen=True)
class ConversionResult:
    """Generated code and non-fatal conversion notes."""

    code: str
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class _DataAction:
    kind: str
    expression: str
    column: str | None = None
    condition: str | None = None
    otherwise: str | None = None
    loop_start: int | None = None
    loop_stop: int | None = None
    loop_step: int | None = None
    loop_assignments: tuple[tuple[str, str], ...] = ()
    merge_sources: tuple[str, ...] = ()
    merge_keys: tuple[str, ...] = ()
    branches: tuple[tuple[str, str], ...] = ()
    retain_initial: str | None = None
    retain_partition: tuple[str, ...] = ()
    retain_reset: str | None = None


_DATASET = re.compile(r"^(?:[A-Za-z_]\w*\.)?([A-Za-z_]\w*)$")
_ASSIGNMENT = re.compile(r"^([A-Za-z_]\w*)\s*=(?!=|>)\s*(.+)$", re.DOTALL)
_DATA_ASSIGNMENT = re.compile(
    r"^([A-Za-z_]\w*(?:\[\s*[^\]]+\s*\])?)\s*=(?!=|>)\s*(.+)$",
    re.DOTALL,
)
_ARRAY_REFERENCE = re.compile(r"\b([A-Za-z_]\w*)\s*\[\s*([^\]]+)\s*\]")
_IF_THEN = re.compile(r"^if\s+(.+?)\s+then(?:\s+(.+))?$", re.IGNORECASE | re.DOTALL)
_DO_LOOP = re.compile(
    r"^do\s+([A-Za-z_]\w*)\s*=\s*([+-]?\d+)\s+to\s*([+-]?\d+)"
    r"(?:\s+by\s+([+-]?\d+))?$",
    re.IGNORECASE,
)
_SQL_CREATE = re.compile(
    r"^\s*create\s+table\s+((?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*)\s+as\s+(.+)$",
    re.IGNORECASE | re.DOTALL,
)
_PROC_IMPORT_TOKEN = re.compile(
    r"([A-Za-z_]\w*)\s*=\s*(\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*'|[^\s]+)"
    r"|([A-Za-z_]\w*)"
)


def _strip_comments(source: str) -> str:
    """Remove SAS block and statement comments without changing quoted text."""
    output: list[str] = []
    index = 0
    quote: str | None = None
    quote_line: int | None = None
    statement_start = True
    line = 1
    while index < len(source):
        char = source[index]
        if quote:
            output.append(char)
            if char == quote:
                if index + 1 < len(source) and source[index + 1] == quote:
                    output.append(source[index + 1])
                    index += 1
                else:
                    quote = None
                    quote_line = None
        elif char in ("'", '"'):
            quote = char
            quote_line = line
            output.append(char)
            statement_start = False
        elif source.startswith("/*", index):
            end = source.find("*/", index + 2)
            if end < 0:
                raise ConversionError(
                    "Unterminated SAS block comment.",
                    line=source.count("\n", 0, index) + 1,
                )
            comment = source[index : end + 2]
            output.append(" " + "\n" * comment.count("\n"))
            line += comment.count("\n")
            index = end + 1
        elif char == "*" and statement_start:
            end = source.find(";", index + 1)
            if end < 0:
                raise ConversionError(
                    "Unterminated SAS statement comment.",
                    line=source.count("\n", 0, index) + 1,
                )
            comment = source[index : end + 1]
            output.append(" " + "\n" * comment.count("\n"))
            line += comment.count("\n")
            index = end
        else:
            output.append(char)
            if char == ";":
                statement_start = True
            elif not char.isspace():
                statement_start = False
        if char == "\n":
            line += 1
        index += 1
    if quote:
        raise ConversionError(
            "Unterminated quoted string.",
            line=quote_line,
        )
    return "".join(output)


def _statements(source: str) -> list[_Statement]:
    """Split SAS source at semicolons outside quoted strings."""
    source = _strip_comments(source)
    result: list[_Statement] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    line = 1
    statement_line = 1
    while index < len(source):
        char = source[index]
        if quote:
            current.append(char)
            if char == quote:
                if index + 1 < len(source) and source[index + 1] == quote:
                    current.append(source[index + 1])
                    index += 1
                else:
                    quote = None
        elif char in ("'", '"'):
            quote = char
            current.append(char)
        elif char == ";":
            statement = "".join(current).strip()
            if statement:
                result.append(_Statement(statement, statement_line))
            current = []
            statement_line = line
        else:
            current.append(char)
        if char == "\n":
            line += 1
            if not "".join(current).strip():
                statement_line = line
        index += 1
    if "".join(current).strip():
        raise ConversionError(
            "Every SAS statement must end with a semicolon.",
            line=statement_line,
            statement="".join(current).strip(),
        )
    return result


def _dataset_name(value: str) -> str:
    match = _DATASET.fullmatch(value.strip())
    if not match:
        raise ConversionError(f"Unsupported SAS dataset reference: {value!r}.")
    return match.group(1).lower()


def _python_name(dataset: str) -> str:
    return f"df_{_dataset_name(dataset)}"


def _sql_name(dataset: str) -> str:
    """Use the table component of a two-level SAS name as a Spark view name."""
    return _dataset_name(dataset)


def _map_unquoted(value: str, transform: Callable[[str], str]) -> str:
    """Apply a text transform outside SQL/SAS quoted strings."""
    pieces: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(value):
        char = value[index]
        if quote is None:
            if char in ("'", '"'):
                pieces.append(transform("".join(current)))
                current = [char]
                quote = char
            else:
                current.append(char)
        else:
            current.append(char)
            if char == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    current.append(value[index + 1])
                    index += 1
                else:
                    pieces.append("".join(current))
                    current = []
                    quote = None
        index += 1
    if current:
        final = "".join(current)
        pieces.append(final if quote else transform(final))
    return "".join(pieces)


def _expression(value: str) -> str:
    """Translate common SAS operators/functions into Spark SQL expressions."""
    expression = value.strip()
    substitutions = (
        (r"\bne\b|\^=", "!="),
        (r"\beq\b", "="),
        (r"\bge\b", ">="),
        (r"\bgt\b", ">"),
        (r"\ble\b", "<="),
        (r"\blt\b", "<"),
        (r"\band\b", "AND"),
        (r"\bor\b", "OR"),
        (r"\bnot\b", "NOT"),
        (r"\bupcase\s*\(", "upper("),
        (r"\blowcase\s*\(", "lower("),
        (r"\btoday\s*\(\s*\)", "current_date()"),
    )

    def translate_unquoted(part: str) -> str:
        for pattern, replacement in substitutions:
            part = re.sub(pattern, replacement, part, flags=re.IGNORECASE)
        return re.sub(
            r"\bmissing\s*\(\s*([A-Za-z_]\w*)\s*\)",
            r"\1 IS NULL",
            part,
            flags=re.IGNORECASE,
        )

    normalized = _map_unquoted(expression, translate_unquoted)
    return _translate_functions(normalized)


def _expand_array_references(
    value: str, arrays: dict[str, tuple[str, ...]]
) -> str:
    """Replace supported constant-index SAS array references with columns."""

    def replace(part: str) -> str:
        def substitute(match: re.Match[str]) -> str:
            array_name = match.group(1).lower()
            if array_name not in arrays:
                raise ConversionError(
                    f"SAS array reference {match.group(1)!r} requires a preceding "
                    "supported ARRAY declaration."
                )
            index_text = match.group(2).strip()
            if not index_text.isdigit():
                raise ConversionError(
                    "SAS array references currently require a positive integer "
                    "literal index."
                )
            index = int(index_text)
            elements = arrays[array_name]
            if index < 1 or index > len(elements):
                raise ConversionError(
                    f"SAS array index {index} is outside 1..{len(elements)} "
                    f"for array {match.group(1)!r}."
                )
            return elements[index - 1]

        return _ARRAY_REFERENCE.sub(substitute, part)

    return _map_unquoted(value, replace)


def _parse_array_declaration(statement: str) -> tuple[str, tuple[str, ...]]:
    match = re.fullmatch(
        r"array\s+([A-Za-z_]\w*)\s*\[\s*(\d+)\s*\]\s+(.+)",
        statement,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise ConversionError(
            "ARRAY supports one dimension, a positive literal size, and an "
            "explicit element list."
        )
    array_name = match.group(1)
    size = int(match.group(2))
    if size < 1:
        raise ConversionError("ARRAY size must be a positive integer.")
    elements: list[str] = []
    for token in match.group(3).replace(",", " ").split():
        range_match = re.fullmatch(
            r"([A-Za-z_]\w*?)(\d+)-([A-Za-z_]\w*?)(\d+)", token
        )
        if range_match:
            start_prefix, start_number, end_prefix, end_number = range_match.groups()
            if start_prefix.lower() != end_prefix.lower():
                raise ConversionError(
                    "ARRAY name ranges must share the same prefix."
                )
            first = int(start_number)
            last = int(end_number)
            if first > last:
                raise ConversionError("ARRAY name ranges must be ascending.")
            elements.extend(
                f"{start_prefix}{number}" for number in range(first, last + 1)
            )
        elif re.fullmatch(r"[A-Za-z_]\w*", token):
            elements.append(token)
        else:
            raise ConversionError(
                f"Unsupported ARRAY element or range: {token!r}."
            )
    if len(elements) != size:
        raise ConversionError(
            f"ARRAY {array_name!r} declares size {size} but lists "
            f"{len(elements)} elements."
        )
    if len({element.lower() for element in elements}) != len(elements):
        raise ConversionError("ARRAY element names must be distinct.")
    return array_name.lower(), tuple(elements)


def _function_arguments(expression: str, opening: int, name: str) -> tuple[list[str], int]:
    """Parse function arguments and return them with the closing-parenthesis index."""
    arguments: list[str] = []
    current: list[str] = []
    depth = 1
    quote: str | None = None
    index = opening + 1
    while index < len(expression):
        char = expression[index]
        if quote:
            current.append(char)
            if char == quote:
                if index + 1 < len(expression) and expression[index + 1] == quote:
                    current.append(expression[index + 1])
                    index += 1
                else:
                    quote = None
        elif char in ("'", '"'):
            quote = char
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            if depth == 0:
                final_argument = "".join(current).strip()
                if final_argument or arguments:
                    arguments.append(final_argument)
                return arguments, index
            current.append(char)
        elif char == "," and depth == 1:
            arguments.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        index += 1
    raise ConversionError(f"Unterminated {name} function call.")


def _translate_functions(expression: str) -> str:
    """Translate supported SAS functions, parsing nested calls and quoted arguments."""
    output: list[str] = []
    index = 0
    while index < len(expression):
        char = expression[index]
        if char in ("'", '"'):
            quote = char
            literal = [char]
            index += 1
            while index < len(expression):
                literal_char = expression[index]
                literal.append(literal_char)
                if literal_char == quote:
                    if index + 1 < len(expression) and expression[index + 1] == quote:
                        literal.append(expression[index + 1])
                        index += 1
                    else:
                        index += 1
                        break
                index += 1
            output.extend(literal)
            continue

        identifier = re.match(r"[A-Za-z_]\w*", expression[index:])
        if identifier is None:
            output.append(char)
            index += 1
            continue

        name = identifier.group()
        end_name = index + len(name)
        opening = end_name
        while opening < len(expression) and expression[opening].isspace():
            opening += 1
        if name.upper() not in {
            "CAT",
            "CATS",
            "CATX",
            "STRIP",
            "COALESCE",
            "COALESCEC",
            "SUBSTR",
            "COMPRESS",
            "ROUND",
        } or (
            opening >= len(expression) or expression[opening] != "("
        ):
            output.append(name)
            index = end_name
            continue

        arguments, closing = _function_arguments(expression, opening, name)
        translated_arguments = [_expression(argument) for argument in arguments]
        output.append(_render_function(name.upper(), translated_arguments))
        index = closing + 1
    return "".join(output)


def _render_function(name: str, arguments: list[str]) -> str:
    if name in {"CAT", "CATS"}:
        if not arguments or any(not argument for argument in arguments):
            raise ConversionError(f"{name} requires one or more non-empty arguments.")
        if name == "CAT":
            values = ", ".join(
                f"coalesce(CAST({argument} AS STRING), '')"
                for argument in arguments
            )
        else:
            values = ", ".join(
                f"coalesce(trim(CAST({argument} AS STRING)), '')"
                for argument in arguments
            )
        return f"concat({values})"

    if name == "CATX":
        if len(arguments) < 2:
            raise ConversionError("CATX requires a delimiter and at least one value.")
        if any(not argument for argument in arguments):
            raise ConversionError("CATX arguments cannot be empty.")
        values = ", ".join(
            f"nullif(trim(CAST({argument} AS STRING)), '')"
            for argument in arguments[1:]
        )
        return (
            f"concat_ws(coalesce(CAST({arguments[0]} AS STRING), ''), {values})"
        )

    if name == "STRIP":
        if len(arguments) != 1 or not arguments[0]:
            raise ConversionError("STRIP requires exactly one non-empty argument.")
        return f"coalesce(trim(CAST({arguments[0]} AS STRING)), '')"

    if name == "COALESCE":
        if not arguments or any(not argument for argument in arguments):
            raise ConversionError("COALESCE requires one or more non-empty arguments.")
        return f"coalesce({', '.join(arguments)})"

    if name == "COALESCEC":
        if not arguments or any(not argument for argument in arguments):
            raise ConversionError("COALESCEC requires one or more non-empty arguments.")
        branches = " ".join(
            f"WHEN nullif(trim(CAST({argument} AS STRING)), '') IS NOT NULL "
            f"THEN CAST({argument} AS STRING)"
            for argument in arguments
        )
        return f"CASE {branches} ELSE '' END"

    if name == "SUBSTR":
        if len(arguments) not in (2, 3):
            raise ConversionError("SUBSTR requires two or three arguments.")
        if re.fullmatch(r"[+-]?\d+", arguments[1]) and int(arguments[1]) <= 0:
            raise ConversionError("SUBSTR start position must be a positive integer.")
        if len(arguments) == 3 and re.fullmatch(r"[+-]?\d+", arguments[2]):
            if int(arguments[2]) <= 0:
                raise ConversionError("SUBSTR length must be a positive integer.")
        length = arguments[2] if len(arguments) == 3 else "2147483647"
        return f"substring({arguments[0]}, {arguments[1]}, {length})"

    if name == "COMPRESS":
        if len(arguments) not in (1, 2):
            raise ConversionError(
                "COMPRESS supports a source and optional character set; modifiers are unsupported."
            )
        characters = arguments[1] if len(arguments) == 2 else "' '"
        return (
            f"translate(CAST({arguments[0]} AS STRING), "
            f"CAST({characters} AS STRING), '')"
        )

    if name == "ROUND":
        if len(arguments) not in (1, 2):
            raise ConversionError("ROUND requires one value and an optional positive unit.")
        if len(arguments) == 1:
            return f"round({arguments[0]}, 0)"
        unit = arguments[1]
        if not re.fullmatch(r"\+?(?:\d+(?:\.\d*)?|\.\d+)", unit):
            raise ConversionError(
                "ROUND supports only a positive numeric literal as its rounding unit."
            )
        if float(unit) <= 0:
            raise ConversionError("ROUND rounding unit must be positive.")
        return f"(round(({arguments[0]}) / ({unit}), 0) * ({unit}))"

    raise AssertionError(f"Unknown SAS function: {name}")


class SasToPySparkConverter:
    """Convert supported SAS DATA steps and procedures into PySpark."""

    def convert(self, source: str) -> ConversionResult:
        try:
            statements = expand_macros(_statements(source))
        except MacroExpansionError as error:
            raise ConversionError(
                error.message,
                line=error.line,
                statement=error.statement,
            ) from error
        output = [
            "# Generated by sas2pyspark. Review semantics before running.",
            "from pyspark.sql import functions as F",
            "",
        ]
        warnings: list[str] = []
        index = 0
        sql_count = 0
        freq_count = 0
        means_count = 0
        transpose_count = 0

        while index < len(statements):
            statement = statements[index]
            lower = statement.text.lower()
            try:
                if lower.startswith("data "):
                    block, index, block_warnings = self._convert_data_step(
                        statements, index
                    )
                    output.extend(block)
                    warnings.extend(block_warnings)
                elif lower == "proc sort" or lower.startswith("proc sort "):
                    block, index, block_warnings = self._convert_sort(statements, index)
                    output.extend(block)
                    warnings.extend(block_warnings)
                elif lower == "proc sql":
                    block, index, sql_count = self._convert_sql(statements, index, sql_count)
                    output.extend(block)
                elif lower == "proc freq" or lower.startswith("proc freq "):
                    block, index, freq_count = self._convert_freq(
                        statements, index, freq_count
                    )
                    output.extend(block)
                elif lower == "proc means" or lower.startswith("proc means "):
                    block, index, means_count = self._convert_means(
                        statements, index, means_count
                    )
                    output.extend(block)
                elif lower == "proc summary" or lower.startswith("proc summary "):
                    block, index, means_count = self._convert_means(
                        statements, index, means_count
                    )
                    output.extend(block)
                elif lower == "proc transpose" or lower.startswith("proc transpose "):
                    block, index, transpose_count = self._convert_transpose(
                        statements, index, transpose_count
                    )
                    output.extend(block)
                elif lower == "proc import" or lower.startswith("proc import "):
                    block, index = self._convert_import(statements, index)
                    output.extend(block)
                elif lower == "proc export" or lower.startswith("proc export "):
                    block, index = self._convert_export(statements, index)
                    output.extend(block)
                else:
                    raise ConversionError(
                        f"Unsupported top-level statement: {statement.text[:80]!r}."
                    )
            except ConversionError as error:
                if error.line is not None:
                    raise
                raise ConversionError(
                    error.message, line=statement.line, statement=statement.text
                ) from error

        return ConversionResult("\n".join(output).rstrip() + "\n", tuple(warnings))

    def _convert_data_step(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int, list[str]]:
        target_statement = statements[start].text
        match = re.fullmatch(r"data\s+((?:[\w]+\.)?[\w]+)", target_statement, re.I)
        if not match:
            raise ConversionError(f"Unsupported DATA statement: {target_statement!r}.")
        target = _dataset_name(match.group(1))
        source: str | None = None
        merge_sources: tuple[str, ...] | None = None
        merge_keys: tuple[str, ...] | None = None
        where_filters: list[str] = []
        actions: list[_DataAction] = []
        keep: list[str] | None = None
        drop: list[str] = []
        retain_column: str | None = None
        retain_initial: str | None = None
        retain_assignment_seen = False
        retain_reset_key: str | None = None
        retain_reset_value: str | None = None
        arrays: dict[str, tuple[str, ...]] = {}
        by_columns: tuple[str, ...] | None = None
        by_flags_used: set[str] = set()

        def data_expression(expression: str) -> str:
            expanded = _expand_array_references(expression, arrays)

            def replace_by_flags(part: str) -> str:
                def replace(match: re.Match[str]) -> str:
                    flag, column = match.groups()
                    if by_columns is None:
                        raise ConversionError(
                            f"{flag.upper()}.{column} requires a preceding BY statement."
                        )
                    matching_key = next(
                        (
                            key
                            for key in by_columns
                            if key.lower() == column.lower()
                        ),
                        None,
                    )
                    if matching_key is None:
                        raise ConversionError(
                            f"{flag.upper()}.{column} is not a variable in the BY statement."
                        )
                    by_flags_used.add(flag.lower())
                    return f"__sas2pyspark_{flag.lower()}_{matching_key.lower()}"

                return re.sub(
                    r"\b(first|last)\s*\.\s*([A-Za-z_]\w*)\b",
                    replace,
                    part,
                    flags=re.IGNORECASE,
                )

            return _expression(_map_unquoted(expanded, replace_by_flags))

        def data_column(column: str) -> str:
            expanded = _expand_array_references(column, arrays).strip()
            if not re.fullmatch(r"[A-Za-z_]\w*", expanded):
                raise ConversionError(
                    "DATA-step array assignment targets require a declared array "
                    "and a constant valid index."
                )
            return expanded

        index = start + 1
        output_loop_seen = False

        while index < len(statements) and statements[index].text.lower() != "run":
            source_statement = statements[index]
            statement = source_statement.text.strip()
            lower = statement.lower()
            try:
                if output_loop_seen and not (
                    lower.startswith("keep ") or lower.startswith("drop ")
                ):
                    raise ConversionError(
                        "A DO loop containing OUTPUT must be the final data-processing "
                        "statement in this DATA step."
                    )
                if lower.startswith("array "):
                    array_name, elements = _parse_array_declaration(statement)
                    if array_name in arrays:
                        raise ConversionError(
                            f"ARRAY {array_name!r} is declared more than once."
                        )
                    existing_elements = {
                        element.lower()
                        for declared in arrays.values()
                        for element in declared
                    }
                    if any(element.lower() in existing_elements for element in elements):
                        raise ConversionError(
                            "ARRAY element variables cannot belong to multiple arrays."
                        )
                    arrays[array_name] = elements
                elif lower.startswith("retain "):
                    if retain_column is not None:
                        raise ConversionError(
                            "Only one initialized numeric RETAIN variable is "
                            "supported per DATA step."
                        )
                    retain_match = re.fullmatch(
                        r"retain\s+([A-Za-z_]\w*)\s+"
                        r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)",
                        statement,
                        re.IGNORECASE,
                    )
                    if not retain_match:
                        raise ConversionError(
                            "RETAIN supports one variable with a numeric literal "
                            "initial value."
                        )
                    retain_column = retain_match.group(1)
                    retain_initial = retain_match.group(2)
                elif lower.startswith("set "):
                    if source is not None or merge_sources is not None:
                        raise ConversionError(
                            "A DATA step must use either one SET statement or one "
                            "supported MERGE statement, not both."
                        )
                    source = _dataset_name(statement[4:].strip())
                elif lower.startswith("merge "):
                    if source is not None or merge_sources is not None:
                        raise ConversionError(
                            "A DATA step must use either one SET statement or one "
                            "supported MERGE statement, not both."
                        )
                    if actions or where_filters:
                        raise ConversionError(
                            "MERGE and BY must appear before DATA-step processing "
                            "statements in the supported subset."
                        )
                    datasets = statement[6:].split()
                    if len(datasets) < 2:
                        raise ConversionError(
                            "MERGE requires at least two input datasets."
                        )
                    merge_sources = tuple(_dataset_name(name) for name in datasets)
                    if len(set(merge_sources)) != len(merge_sources):
                        raise ConversionError(
                            "MERGE input datasets must have distinct names."
                        )
                elif lower.startswith("by "):
                    keys = statement[3:].split()
                    if not keys or any(
                        not re.fullmatch(r"[A-Za-z_]\w*", key) for key in keys
                    ):
                        raise ConversionError("BY requires simple column names.")
                    if any(key.lower() in {"ascending", "descending"} for key in keys):
                        raise ConversionError(
                            "BY ascending/descending order specifications are "
                            "unsupported."
                        )
                    if len({key.lower() for key in keys}) != len(keys):
                        raise ConversionError("BY column names must be distinct.")
                    if merge_sources is not None:
                        if merge_keys is not None:
                            raise ConversionError(
                                "MERGE supports only one BY statement."
                            )
                        merge_keys = tuple(keys)
                        actions.append(
                            _DataAction(
                                kind="merge",
                                expression="",
                                merge_sources=merge_sources,
                                merge_keys=merge_keys,
                            )
                        )
                    elif source is not None and by_columns is None:
                        by_columns = tuple(keys)
                    else:
                        raise ConversionError(
                            "BY is supported once after SET or MERGE."
                        )
                elif lower.startswith("where "):
                    if merge_sources is not None or any(
                        action.kind == "merge" for action in actions
                    ):
                        raise ConversionError(
                            "WHERE in a MERGE DATA step is unsupported; filter the "
                            "inputs explicitly before conversion."
                        )
                    where_filters.append(data_expression(statement[6:]))
                elif lower == "select":
                    branches: list[tuple[str, str]] = []
                    selected_column: str | None = None
                    otherwise_expression: str | None = None
                    body_index = index + 1
                    while (
                        body_index < len(statements)
                        and statements[body_index].text.lower() != "end"
                    ):
                        branch_statement = statements[body_index]
                        branch_text = branch_statement.text.strip()
                        branch_lower = branch_text.lower()
                        if branch_lower.startswith("when "):
                            if otherwise_expression is not None:
                                raise ConversionError(
                                    "WHEN cannot follow OTHERWISE in SELECT."
                                )
                            branch_match = re.fullmatch(
                                r"when\s*\((.*)\)\s+(.+)",
                                branch_text,
                                re.IGNORECASE | re.DOTALL,
                            )
                            if not branch_match:
                                raise ConversionError(
                                    "SELECT WHEN requires one condition and a "
                                    "simple assignment."
                                )
                            assignment = _DATA_ASSIGNMENT.fullmatch(
                                branch_match.group(2).strip()
                            )
                            if not assignment:
                                raise ConversionError(
                                    "SELECT WHEN branches must be simple assignments."
                                )
                            column = data_column(assignment.group(1))
                            if (
                                selected_column is not None
                                and selected_column.lower() != column.lower()
                            ):
                                raise ConversionError(
                                    "SELECT branches must assign the same column."
                                )
                            selected_column = column
                            branches.append(
                                (
                                    data_expression(branch_match.group(1)),
                                    data_expression(assignment.group(2)),
                                )
                            )
                        elif branch_lower.startswith("otherwise"):
                            if otherwise_expression is not None:
                                raise ConversionError(
                                    "SELECT supports only one OTHERWISE assignment."
                                )
                            otherwise_match = re.fullmatch(
                                r"otherwise\s+(.+)",
                                branch_text,
                                re.IGNORECASE | re.DOTALL,
                            )
                            if not otherwise_match:
                                raise ConversionError(
                                    "SELECT OTHERWISE requires a simple assignment."
                                )
                            assignment = _DATA_ASSIGNMENT.fullmatch(
                                otherwise_match.group(1).strip()
                            )
                            if not assignment:
                                raise ConversionError(
                                    "SELECT OTHERWISE must be a simple assignment."
                                )
                            column = data_column(assignment.group(1))
                            if (
                                selected_column is not None
                                and selected_column.lower() != column.lower()
                            ):
                                raise ConversionError(
                                    "SELECT branches must assign the same column."
                                )
                            selected_column = column
                            otherwise_expression = data_expression(
                                assignment.group(2)
                            )
                        else:
                            raise ConversionError(
                                "SELECT supports only WHEN and OTHERWISE assignments."
                            )
                        body_index += 1

                    if body_index >= len(statements):
                        raise ConversionError("SELECT is missing END.")
                    if not branches:
                        raise ConversionError("SELECT requires at least one WHEN.")
                    if otherwise_expression is None:
                        raise ConversionError(
                            "SELECT requires OTHERWISE to define unmatched conditions."
                        )
                    if selected_column is None:
                        raise AssertionError("SELECT branches did not define a column.")
                    actions.append(
                        _DataAction(
                            kind="select_assign",
                            expression=otherwise_expression,
                            column=selected_column,
                            branches=tuple(branches),
                        )
                    )
                    index = body_index
                elif lower.startswith("do "):
                    loop_match = _DO_LOOP.fullmatch(statement)
                    if not loop_match:
                        raise ConversionError(
                            "Only integer-literal DO index = start TO stop [BY step] "
                            "loops are supported."
                        )
                    loop_variable = loop_match.group(1)
                    loop_start = int(loop_match.group(2))
                    loop_stop = int(loop_match.group(3))
                    loop_step = int(loop_match.group(4) or "1")
                    if loop_step == 0:
                        raise ConversionError("DO loop BY step cannot be zero.")
                    body_assignments: list[tuple[str, str]] = []
                    output_count = 0
                    body_index = index + 1
                    while (
                        body_index < len(statements)
                        and statements[body_index].text.lower() != "end"
                    ):
                        body_statement = statements[body_index]
                        body_text = body_statement.text.strip()
                        if body_text.lower() == "output":
                            output_count += 1
                            if body_index + 1 >= len(statements) or statements[
                                body_index + 1
                            ].text.lower() == "run":
                                raise ConversionError("DO loop is missing END.")
                            if statements[body_index + 1].text.lower() != "end":
                                raise ConversionError(
                                    "OUTPUT must be the final statement in a supported DO loop."
                                )
                        else:
                            body_assignment = _DATA_ASSIGNMENT.fullmatch(body_text)
                            if not body_assignment:
                                raise ConversionError(
                                    "DO loop bodies support only simple assignments "
                                    "followed by OUTPUT."
                                )
                            column = data_column(body_assignment.group(1))
                            if column.lower() == loop_variable.lower():
                                raise ConversionError(
                                    "DO loop body cannot reassign its index variable."
                                )
                            expression = data_expression(body_assignment.group(2))
                            if re.search(
                                rf"\b{re.escape(column)}\b",
                                expression,
                                re.IGNORECASE,
                            ):
                                raise ConversionError(
                                    "DO loop assignments cannot depend on their own "
                                    "previous-iteration value."
                                )
                            body_assignments.append((column, expression))
                        body_index += 1
                    if body_index >= len(statements):
                        raise ConversionError("DO loop is missing END.")
                    if output_count != 1:
                        raise ConversionError(
                            "Supported DO loops require exactly one OUTPUT at the end."
                        )
                    if actions and actions[-1].kind == "do_output":
                        raise ConversionError(
                            "Only one output-generating DO loop is supported per DATA step."
                        )
                    actions.append(
                        _DataAction(
                            kind="do_output",
                            expression="",
                            column=loop_variable,
                            loop_start=loop_start,
                            loop_stop=loop_stop,
                            loop_step=loop_step,
                            loop_assignments=tuple(body_assignments),
                        )
                    )
                    output_loop_seen = True
                    index = body_index
                elif lower == "output":
                    raise ConversionError(
                        "OUTPUT is supported only as the final statement inside a "
                        "supported indexed DO loop."
                    )
                elif lower.startswith("if "):
                    conditional = _IF_THEN.fullmatch(statement)
                    if conditional:
                        else_match = (
                            re.fullmatch(
                                r"else\s+(.+)",
                                statements[index + 1].text,
                                re.IGNORECASE | re.DOTALL,
                            )
                            if index + 1 < len(statements)
                            else None
                        )
                        if else_match is None:
                            reset_assignment = (
                                _DATA_ASSIGNMENT.fullmatch(conditional.group(2).strip())
                                if conditional.group(2)
                                else None
                            )
                            reset_condition = re.fullmatch(
                                r"first\s*\.\s*([A-Za-z_]\w*)",
                                conditional.group(1).strip(),
                                re.IGNORECASE,
                            )
                            if reset_assignment is None or reset_condition is None:
                                raise ConversionError(
                                    "IF THEN requires a following ELSE assignment; "
                                    "the only supported form without ELSE is a "
                                    "numeric RETAIN reset on FIRST.<BY key>."
                                )
                            if retain_column is None:
                                raise ConversionError(
                                    "FIRST.<BY key> RETAIN resets require a preceding "
                                    "RETAIN declaration."
                                )
                            if retain_assignment_seen:
                                raise ConversionError(
                                    "A RETAIN reset must precede the accumulator assignment."
                                )
                            reset_column = data_column(reset_assignment.group(1))
                            if reset_column.lower() != retain_column.lower():
                                raise ConversionError(
                                    "FIRST.<BY key> reset must assign the retained variable."
                                )
                            reset_value = reset_assignment.group(2).strip()
                            if not re.fullmatch(
                                r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?",
                                reset_value,
                            ):
                                raise ConversionError(
                                    "RETAIN reset requires a numeric literal reset value."
                                )
                            if by_columns is None:
                                raise ConversionError(
                                    "FIRST.<BY key> RETAIN resets require a preceding BY statement."
                                )
                            reset_key = next(
                                (
                                    key
                                    for key in by_columns
                                    if key.lower() == reset_condition.group(1).lower()
                                ),
                                None,
                            )
                            if reset_key is None:
                                raise ConversionError(
                                    f"FIRST.{reset_condition.group(1)} is not a "
                                    "variable in the BY statement."
                                )
                            if retain_reset_key is not None:
                                raise ConversionError(
                                    "Only one FIRST.<BY key> RETAIN reset is supported."
                                )
                            data_expression(conditional.group(1))
                            retain_reset_key = reset_key
                            retain_reset_value = reset_value
                        else:
                            if not conditional.group(2):
                                raise ConversionError(
                                    "IF THEN branch must be a simple assignment."
                                )
                            then_assignment = _DATA_ASSIGNMENT.fullmatch(
                                conditional.group(2).strip()
                            )
                            else_assignment = _DATA_ASSIGNMENT.fullmatch(
                                else_match.group(1).strip()
                            )
                            if not then_assignment or not else_assignment:
                                raise ConversionError(
                                    "IF THEN and ELSE branches must each be a simple assignment."
                                )
                            then_column = data_column(then_assignment.group(1))
                            else_column = data_column(else_assignment.group(1))
                            if then_column.lower() != else_column.lower():
                                raise ConversionError(
                                    "IF THEN and ELSE branches must assign the same column."
                                )
                            actions.append(
                                _DataAction(
                                    kind="conditional_assign",
                                    expression=data_expression(then_assignment.group(2)),
                                    column=then_column,
                                    condition=data_expression(conditional.group(1)),
                                    otherwise=data_expression(else_assignment.group(2)),
                                )
                            )
                            index += 1
                    elif re.match(r"^if\b", statement, re.IGNORECASE):
                        actions.append(_DataAction("filter", data_expression(statement[2:].strip())))
                    else:
                        raise ConversionError(
                            f"Unsupported DATA step IF statement: {statement!r}."
                        )
                elif lower.startswith("else"):
                    raise ConversionError("ELSE must immediately follow a supported IF THEN assignment.")
                elif lower.startswith("keep "):
                    keep = statement[5:].split()
                elif lower.startswith("drop "):
                    drop.extend(statement[5:].split())
                else:
                    assignment = _DATA_ASSIGNMENT.fullmatch(statement)
                    if not assignment:
                        raise ConversionError(
                            f"Unsupported DATA step statement: {statement[:80]!r}."
                        )
                    column = data_column(assignment.group(1))
                    if retain_column and column.lower() == retain_column.lower():
                        if retain_assignment_seen:
                            raise ConversionError(
                                "A retained accumulator can be updated only once "
                                "per DATA-step iteration."
                            )
                        accumulator_match = re.fullmatch(
                            rf"{re.escape(retain_column)}\s*\+\s*(.+)",
                            assignment.group(2),
                            re.IGNORECASE | re.DOTALL,
                        )
                        if not accumulator_match:
                            raise ConversionError(
                                "RETAIN supports only accumulator assignments of "
                                "the form variable = variable + expression."
                            )
                        increment = accumulator_match.group(1).strip()
                        if re.search(
                            rf"\b{re.escape(retain_column)}\b",
                            increment,
                            re.IGNORECASE,
                        ):
                            raise ConversionError(
                                "RETAIN accumulator increments cannot reference "
                                "the retained variable."
                            )
                        actions.append(
                            _DataAction(
                                kind="retain_sum",
                                expression=data_expression(increment),
                                column=column,
                                retain_initial=retain_initial,
                                retain_partition=(
                                    by_columns[
                                        : next(
                                            key_index
                                            for key_index, key in enumerate(by_columns)
                                            if key.lower() == retain_reset_key.lower()
                                        )
                                        + 1
                                    ]
                                    if by_columns is not None
                                    and retain_reset_key is not None
                                    else ()
                                ),
                                retain_reset=retain_reset_value,
                            )
                        )
                        retain_assignment_seen = True
                    else:
                        actions.append(
                            _DataAction(
                                "assign",
                                data_expression(assignment.group(2)),
                                column,
                            )
                        )
            except ConversionError as error:
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1

        if index >= len(statements):
            raise ConversionError(f"DATA step for {target!r} is missing RUN.")
        if source is None and (merge_sources is None or merge_keys is None):
            raise ConversionError(
                "DATA steps require one SET statement or a MERGE followed by BY."
            )
        if retain_column is not None:
            if not retain_assignment_seen:
                raise ConversionError(
                    "RETAIN variable must have one supported accumulator assignment."
                )
            if (
                source is None
                or merge_sources is not None
                or where_filters
                or len(actions) != 1
                or actions[0].kind != "retain_sum"
            ):
                raise ConversionError(
                    "RETAIN currently supports one SET input and one accumulator "
                    "assignment, without other DATA-step processing."
                )
            if by_columns is not None and retain_reset_key is None:
                raise ConversionError(
                    "BY-group RETAIN requires an IF FIRST.<BY key> reset before "
                    "the accumulator assignment."
                )

        lines: list[str] = []
        if source is not None:
            lines.append(f"{_python_name(target)} = {_python_name(source)}")
        for condition in where_filters:
            lines.append(f"{_python_name(target)} = {_python_name(target)}.filter(F.expr({condition!r}))")
        by_flag_columns: list[str] = []
        if by_columns is not None and by_flags_used:
            order_column = "__sas2pyspark_by_order"
            by_flag_columns = [
                f"__sas2pyspark_{flag}_{column.lower()}"
                for flag in ("first", "last")
                if flag in by_flags_used
                for column in by_columns
            ]
            helper_columns = [order_column, *by_flag_columns]
            helper_names = {column.lower() for column in helper_columns}
            action_columns = {
                action.column.lower()
                for action in actions
                if action.column is not None
            }
            action_columns.update(
                column.lower()
                for action in actions
                for column, _ in action.loop_assignments
            )
            if helper_names.intersection(action_columns):
                raise ConversionError(
                    "DATA-step assignments cannot use reserved BY-processing "
                    "temporary column names."
                )
            lines.extend(
                [
                    "from pyspark.sql import Window as __sas2pyspark_window",
                    f"__sas2pyspark_by_missing = [column for column in {list(by_columns)!r} "
                    f"if column not in {_python_name(target)}.columns]",
                    "if __sas2pyspark_by_missing:",
                    "    raise ValueError('BY statement columns missing from input DataFrame: ' + ', '.join(__sas2pyspark_by_missing))",
                    f"if any(column.lower() in {{column.lower() for column in {helper_columns!r}}} "
                    f"for column in {_python_name(target)}.columns):",
                    "    raise ValueError('BY processing temporary column name conflicts with an input column.')",
                    f"{_python_name(target)} = {_python_name(target)}.withColumn("
                    f"{order_column!r}, F.monotonically_increasing_id())",
                ]
            )
            for key_index, key in enumerate(by_columns):
                if "first" in by_flags_used:
                    first_name = f"__sas2pyspark_first_{key.lower()}"
                    prefix = ", ".join(
                        f"F.col({column!r})" for column in by_columns[: key_index + 1]
                    )
                    lines.append(
                        f"{_python_name(target)} = {_python_name(target)}.withColumn("
                        f"{first_name!r}, F.row_number().over("
                        f"__sas2pyspark_window.partitionBy({prefix}).orderBy("
                        f"F.col({order_column!r}).asc())) == 1)"
                    )
                if "last" in by_flags_used:
                    last_name = f"__sas2pyspark_last_{key.lower()}"
                    prefix = ", ".join(
                        f"F.col({column!r})" for column in by_columns[: key_index + 1]
                    )
                    lines.append(
                        f"{_python_name(target)} = {_python_name(target)}.withColumn("
                        f"{last_name!r}, F.row_number().over("
                        f"__sas2pyspark_window.partitionBy({prefix}).orderBy("
                        f"F.col({order_column!r}).desc())) == 1)"
                    )
        for action in actions:
            if action.kind == "merge":
                lines.extend(self._render_merge(target, action.merge_sources, action.merge_keys))
            elif action.kind == "filter":
                lines.append(
                    f"{_python_name(target)} = {_python_name(target)}.filter(F.expr({action.expression!r}))"
                )
            elif action.kind == "assign" and action.column is not None:
                lines.append(
                    f"{_python_name(target)} = {_python_name(target)}.withColumn({action.column!r}, F.expr({action.expression!r}))"
                )
            elif (
                action.kind == "retain_sum"
                and action.column is not None
                and action.retain_initial is not None
            ):
                retain_input = "__sas2pyspark_retain_input"
                retain_order = (
                    "__sas2pyspark_by_order"
                    if action.retain_partition
                    else "__sas2pyspark_retain_order"
                )
                retain_delta = "__sas2pyspark_retain_delta"
                initial_value = float(action.retain_reset or action.retain_initial)
                retain_temporary_columns = (
                    [retain_delta]
                    if action.retain_partition
                    else [retain_order, retain_delta]
                )
                window_spec = "__sas2pyspark_window"
                if action.retain_partition:
                    partition_columns = ", ".join(
                        f"F.col({column!r})" for column in action.retain_partition
                    )
                    window_spec += f".partitionBy({partition_columns})"
                window_spec += f".orderBy(F.col({retain_order!r}))"
                lines.extend(
                    [
                        "from pyspark.sql import Window as __sas2pyspark_window",
                        "from pyspark.sql.types import NumericType as __sas2pyspark_numeric_type",
                        f"{retain_input} = {_python_name(target)}",
                        f"if any(column.lower() in "
                        f"{{column.lower() for column in {retain_temporary_columns!r}}} "
                        f"for column in {retain_input}.columns):",
                        "    raise ValueError('PROC/DATA-step RETAIN temporary column name conflicts with an input column.')",
                        *(
                            []
                            if action.retain_partition
                            else [
                                f"{retain_input} = {retain_input}.withColumn("
                                f"{retain_order!r}, F.monotonically_increasing_id())"
                            ]
                        ),
                        f"{retain_input} = {retain_input}.withColumn("
                        f"{retain_delta!r}, F.expr({action.expression!r}))",
                        f"if not isinstance({retain_input}.schema[{retain_delta!r}].dataType, "
                        "__sas2pyspark_numeric_type):",
                        "    raise TypeError('RETAIN accumulator increments must be numeric.')",
                        f"if {retain_input}.filter(F.col({retain_delta!r}).isNull()).limit(1).count():",
                        "    raise ValueError('RETAIN accumulator increments cannot contain missing values.')",
                        f"{_python_name(target)} = {retain_input}.withColumn("
                        f"{action.column!r}, F.lit({initial_value!r}) + "
                        f"F.sum(F.col({retain_delta!r})).over("
                        f"{window_spec}.rowsBetween("
                        "__sas2pyspark_window.unboundedPreceding, "
                        f"__sas2pyspark_window.currentRow))).drop(*{retain_temporary_columns!r})",
                    ]
                )
            elif (
                action.kind == "select_assign"
                and action.column is not None
                and action.branches
            ):
                expression = f"F.expr({action.expression!r})"
                for condition, branch_expression in reversed(action.branches):
                    expression = (
                        f"F.when(F.expr({condition!r}), "
                        f"F.expr({branch_expression!r})).otherwise({expression})"
                    )
                lines.append(
                    f"{_python_name(target)} = {_python_name(target)}.withColumn("
                    f"{action.column!r}, {expression})"
                )
            elif (
                action.kind == "conditional_assign"
                and action.column is not None
                and action.condition is not None
                and action.otherwise is not None
            ):
                lines.append(
                    f"{_python_name(target)} = {_python_name(target)}.withColumn("
                    f"{action.column!r}, "
                    f"F.when(F.expr({action.condition!r}), F.expr({action.expression!r}))"
                    f".otherwise(F.expr({action.otherwise!r})))"
                )
            elif (
                action.kind == "do_output"
                and action.column is not None
                and action.loop_start is not None
                and action.loop_stop is not None
                and action.loop_step is not None
            ):
                if (
                    action.loop_step > 0
                    and action.loop_start > action.loop_stop
                ) or (
                    action.loop_step < 0
                    and action.loop_start < action.loop_stop
                ):
                    lines.append(
                        f"{_python_name(target)} = {_python_name(target)}.withColumn("
                    f"{action.column!r}, F.explode(F.array().cast('array<int>')))"
                    )
                else:
                    lines.append(
                        f"{_python_name(target)} = {_python_name(target)}.withColumn("
                        f"{action.column!r}, F.explode(F.sequence("
                        f"F.lit({action.loop_start}), F.lit({action.loop_stop}), "
                        f"F.lit({action.loop_step}))))"
                    )
                    for column, expression in action.loop_assignments:
                        lines.append(
                            f"{_python_name(target)} = {_python_name(target)}.withColumn("
                            f"{column!r}, F.expr({expression!r}))"
                        )
            else:
                raise AssertionError(f"Unknown DATA step action: {action.kind!r}")
        if keep is not None:
            lines.append(f"{_python_name(target)} = {_python_name(target)}.select(*{keep!r})")
        elif drop:
            lines.append(f"{_python_name(target)} = {_python_name(target)}.drop(*{drop!r})")
        if by_flag_columns:
            lines.append(
                f"{_python_name(target)} = {_python_name(target)}.drop("
                f"*{[ '__sas2pyspark_by_order', *by_flag_columns]!r})"
            )
        lines.extend(
            [
                f"{_python_name(target)}.createOrReplaceTempView({target!r})",
                "",
            ]
        )
        data_step_warnings = []
        if retain_column is not None:
            data_step_warnings.append(
                "RETAIN uses Spark monotonically_increasing_id() as a best-effort "
                "row order; Spark does not guarantee SAS observation order, so "
                "stateful results may differ or change across executions."
            )
        if by_flag_columns:
            data_step_warnings.append(
                "BY-group FIRST./LAST. flags use Spark "
                "monotonically_increasing_id() as a best-effort observation "
                "order; row order is unspecified and results may differ from "
                "SAS or change across executions."
            )
        return lines, index + 1, data_step_warnings

    @staticmethod
    def _render_merge(
        target: str, sources: tuple[str, ...], keys: tuple[str, ...]
    ) -> list[str]:
        """Generate a guarded full-outer merge for unique SAS BY keys."""
        target_frame = _python_name(target)
        input_frames = ", ".join(_python_name(source) for source in sources)
        return [
            f"__sas2pyspark_merge_inputs = [{input_frames}]",
            f"__sas2pyspark_merge_keys = {list(keys)!r}",
            "__sas2pyspark_merge_frames = []",
            "__sas2pyspark_merge_payload = []",
            "for __sas2pyspark_merge_index, __sas2pyspark_merge_frame in enumerate(__sas2pyspark_merge_inputs):",
            "    __sas2pyspark_merge_actual_keys = [",
            "        next((__sas2pyspark_column for __sas2pyspark_column in __sas2pyspark_merge_frame.columns",
            "              if __sas2pyspark_column.lower() == __sas2pyspark_key.lower()), None)",
            "        for __sas2pyspark_key in __sas2pyspark_merge_keys",
            "    ]",
            "    if any(__sas2pyspark_key is None for __sas2pyspark_key in __sas2pyspark_merge_actual_keys):",
            "        raise ValueError('SAS MERGE BY key is missing from an input DataFrame.')",
            "    if (__sas2pyspark_merge_frame",
            "            .groupBy(*__sas2pyspark_merge_actual_keys)",
            "            .count().filter(F.col('count') > 1).limit(1).count()):",
            "        raise ValueError('SAS MERGE BY requires unique keys in every input DataFrame.')",
            "    __sas2pyspark_merge_columns = [",
            "        __sas2pyspark_column for __sas2pyspark_column in __sas2pyspark_merge_frame.columns",
            "        if __sas2pyspark_column.lower() not in {__sas2pyspark_key.lower() for __sas2pyspark_key in __sas2pyspark_merge_keys}",
            "    ]",
            "    __sas2pyspark_merge_payload.append(__sas2pyspark_merge_columns)",
            "    __sas2pyspark_merge_projection = [",
            "        F.col(__sas2pyspark_actual_key).alias(f'__sas2pyspark_m{__sas2pyspark_merge_index}_key{__sas2pyspark_key_index}')",
            "        for __sas2pyspark_key_index, __sas2pyspark_actual_key in enumerate(__sas2pyspark_merge_actual_keys)",
            "    ]",
            "    __sas2pyspark_merge_projection.extend(",
            "        F.col(__sas2pyspark_column).alias(f'__sas2pyspark_m{__sas2pyspark_merge_index}_col{__sas2pyspark_column_index}')",
            "        for __sas2pyspark_column_index, __sas2pyspark_column in enumerate(__sas2pyspark_merge_columns)",
            "    )",
            "    __sas2pyspark_merge_projection.append(F.lit(True).alias(f'__sas2pyspark_m{__sas2pyspark_merge_index}_present'))",
            "    __sas2pyspark_merge_frames.append(__sas2pyspark_merge_frame.select(*__sas2pyspark_merge_projection))",
            "__sas2pyspark_merge_joined = __sas2pyspark_merge_frames[0]",
            "for __sas2pyspark_merge_index in range(1, len(__sas2pyspark_merge_frames)):",
            "    __sas2pyspark_merge_conditions = [",
            "        F.coalesce(*[F.col(f'__sas2pyspark_m{__sas2pyspark_prior}_key{__sas2pyspark_key_index}')",
            "                     for __sas2pyspark_prior in range(__sas2pyspark_merge_index)])",
            "        .eqNullSafe(F.col(f'__sas2pyspark_m{__sas2pyspark_merge_index}_key{__sas2pyspark_key_index}'))",
            "        for __sas2pyspark_key_index in range(len(__sas2pyspark_merge_keys))",
            "    ]",
            "    __sas2pyspark_merge_condition = __sas2pyspark_merge_conditions[0]",
            "    for __sas2pyspark_next_condition in __sas2pyspark_merge_conditions[1:]:",
            "        __sas2pyspark_merge_condition = __sas2pyspark_merge_condition & __sas2pyspark_next_condition",
            "    __sas2pyspark_merge_joined = __sas2pyspark_merge_joined.join(",
            "        __sas2pyspark_merge_frames[__sas2pyspark_merge_index],",
            "        __sas2pyspark_merge_condition,",
            "        'full_outer',",
            "    )",
            "__sas2pyspark_merge_columns = list(dict.fromkeys(",
            "    __sas2pyspark_column",
            "    for __sas2pyspark_input_columns in __sas2pyspark_merge_payload",
            "    for __sas2pyspark_column in __sas2pyspark_input_columns",
            "))",
            "__sas2pyspark_merge_selection = [",
            "    F.coalesce(*[F.col(f'__sas2pyspark_m{__sas2pyspark_index}_key{__sas2pyspark_key_index}')",
            "                 for __sas2pyspark_index in range(len(__sas2pyspark_merge_frames))])",
            "    .alias(__sas2pyspark_key)",
            "    for __sas2pyspark_key_index, __sas2pyspark_key in enumerate(__sas2pyspark_merge_keys)",
            "]",
            "for __sas2pyspark_column_index, __sas2pyspark_column in enumerate(__sas2pyspark_merge_columns):",
            "    __sas2pyspark_value = F.lit(None)",
            "    for __sas2pyspark_merge_index, __sas2pyspark_input_columns in enumerate(__sas2pyspark_merge_payload):",
            "        if __sas2pyspark_column in __sas2pyspark_input_columns:",
            "            __sas2pyspark_source_value = F.col(f'__sas2pyspark_m{__sas2pyspark_merge_index}_col{__sas2pyspark_input_columns.index(__sas2pyspark_column)}')",
            "            __sas2pyspark_present = F.col(f'__sas2pyspark_m{__sas2pyspark_merge_index}_present').isNotNull()",
            "            __sas2pyspark_value = F.when(__sas2pyspark_present, __sas2pyspark_source_value).otherwise(__sas2pyspark_value)",
            "    __sas2pyspark_merge_selection.append(__sas2pyspark_value.alias(__sas2pyspark_column))",
            f"{target_frame} = __sas2pyspark_merge_joined.select(*__sas2pyspark_merge_selection).orderBy("
            + ", ".join(f"F.col({key!r}).asc()" for key in keys)
            + ")",
            "",
        ]

    def _convert_sort(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int, list[str]]:
        data: str | None = None
        target: str | None = None
        keys: list[tuple[str, bool]] = []
        nodupkey = False
        header_options = statements[start].text[len("proc sort") :].strip().split()
        for option in header_options:
            lower_option = option.lower()
            if lower_option.startswith("data="):
                data = _dataset_name(option[5:])
            elif lower_option.startswith("out="):
                target = _dataset_name(option[4:])
            elif lower_option == "nodupkey":
                nodupkey = True
            else:
                raise ConversionError(f"Unsupported PROC SORT option: {option!r}.")
        index = start + 1
        while index < len(statements) and statements[index].text.lower() != "run":
            source_statement = statements[index]
            statement = source_statement.text.strip()
            lower = statement.lower()
            try:
                if lower.startswith("data="):
                    data = _dataset_name(statement[5:].strip())
                elif lower.startswith("out="):
                    target = _dataset_name(statement[4:].strip())
                elif lower == "nodupkey":
                    nodupkey = True
                elif lower.startswith("by "):
                    descending = False
                    for token in statement[3:].split():
                        if token.lower() == "descending":
                            descending = True
                            continue
                        if token.lower() == "ascending":
                            descending = False
                            continue
                        if not re.fullmatch(r"[A-Za-z_]\w*", token):
                            raise ConversionError("Malformed PROC SORT BY statement.")
                        keys.append((token, not descending))
                        descending = False
                else:
                    raise ConversionError(
                        f"Unsupported PROC SORT option: {statement!r}."
                    )
            except ConversionError as error:
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1

        if index >= len(statements):
            raise ConversionError("PROC SORT is missing RUN.")
        if data is None or not keys:
            raise ConversionError("PROC SORT requires DATA= and BY.")
        target = target or data
        frame = _python_name(data)
        target_frame = _python_name(target)
        key_names = [name for name, _ in keys]
        order = ", ".join(
            f"F.col({name!r}).{'asc' if ascending else 'desc'}()" for name, ascending in keys
        )
        lines: list[str] = []
        warnings: list[str] = []
        if nodupkey:
            partition = ", ".join(f"F.col({name!r})" for name in key_names)
            ordering = ", ".join(
                f"F.col({name!r}).{'asc' if ascending else 'desc'}()"
                for name, ascending in keys
            )
            lines.extend(
                [
                    f"{target_frame} = {frame}.withColumn("
                    "'__sas2pyspark_row_number', "
                    f"F.row_number().over(__sas2pyspark_window.partitionBy({partition}).orderBy({ordering}))"
                    ")",
                    f"{target_frame} = {target_frame}.filter(F.col('__sas2pyspark_row_number') == 1).drop('__sas2pyspark_row_number')",
                ]
            )
            warnings.append(
                "PROC SORT NODUPKEY tie behavior may differ from SAS when BY keys tie."
            )
        else:
            lines.append(f"{target_frame} = {frame}")
        lines.append(f"{target_frame} = {target_frame}.orderBy({order})")
        lines.extend(
            [
                f"{target_frame}.createOrReplaceTempView({_sql_name(target)!r})",
                "",
            ]
        )
        if nodupkey:
            lines.insert(1, "from pyspark.sql import Window as __sas2pyspark_window")
        return lines, index + 1, warnings

    def _convert_freq(
        self, statements: list[_Statement], start: int, freq_count: int
    ) -> tuple[list[str], int, int]:
        data: str | None = None
        header_options = statements[start].text[len("proc freq") :].strip().split()
        for option in header_options:
            if option.lower().startswith("data="):
                data = _dataset_name(option[5:])
            else:
                raise ConversionError(f"Unsupported PROC FREQ option: {option!r}.")

        table_columns: tuple[str, ...] | None = None
        target: str | None = None
        index = start + 1
        while index < len(statements) and statements[index].text.lower() != "run":
            source_statement = statements[index]
            statement = source_statement.text.strip()
            try:
                if statement.lower().startswith("tables "):
                    if table_columns is not None:
                        raise ConversionError(
                            "PROC FREQ supports exactly one TABLES specification."
                        )
                    table_spec, separator, options = statement[7:].partition("/")
                    match = re.fullmatch(
                        r"\s*([A-Za-z_]\w*)\s*"
                        r"(?:\*\s*([A-Za-z_]\w*)\s*)?",
                        table_spec,
                    )
                    if not match:
                        raise ConversionError(
                            "PROC FREQ TABLES supports one-way or two-way tables "
                            "using simple column names."
                        )
                    table_columns = tuple(
                        column for column in match.groups() if column is not None
                    )
                    if separator:
                        output_match = re.fullmatch(
                            r"\s*out\s*=\s*((?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*)\s*",
                            options,
                            re.IGNORECASE,
                        )
                        if not output_match:
                            raise ConversionError(
                                "PROC FREQ TABLES supports only the OUT= option."
                            )
                        target = _dataset_name(output_match.group(1))
                else:
                    raise ConversionError(
                        f"Unsupported PROC FREQ statement: {statement[:80]!r}."
                    )
            except ConversionError as error:
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1

        if index >= len(statements):
            raise ConversionError("PROC FREQ is missing RUN.")
        if data is None:
            raise ConversionError("PROC FREQ requires DATA=.")
        if table_columns is None:
            raise ConversionError("PROC FREQ requires one TABLES statement.")

        freq_count += 1
        frame = _python_name(data)
        frequency_frame = _python_name(target) if target else f"df_freq_{freq_count}"
        column_selection = ", ".join(
            f"F.col({column!r})" for column in table_columns
        )
        order = ", ".join(
            f"F.col({column!r}).asc()" for column in table_columns
        )
        nonmissing = " & ".join(
            f"F.col({column!r}).isNotNull()" for column in table_columns
        )
        lines = [
            "from pyspark.sql import Window as __sas2pyspark_window",
            f"__sas2pyspark_freq_counts = {frame}.filter({nonmissing}).groupBy("
            f"{column_selection}).agg(F.count(F.lit(1)).alias('COUNT'))",
        ]
        total = "F.sum(F.col('COUNT')).over(__sas2pyspark_window.partitionBy())"
        lines.append(
            f"{frequency_frame} = __sas2pyspark_freq_counts.withColumn("
            f"'PERCENT', F.col('COUNT') / {total} * F.lit(100))"
        )
        if len(table_columns) == 2:
            row_total = (
                "F.sum(F.col('COUNT')).over("
                f"__sas2pyspark_window.partitionBy(F.col({table_columns[0]!r})))"
            )
            column_total = (
                "F.sum(F.col('COUNT')).over("
                f"__sas2pyspark_window.partitionBy(F.col({table_columns[1]!r})))"
            )
            lines.extend(
                [
                    f"{frequency_frame} = {frequency_frame}.withColumn("
                    f"'ROW_PERCENT', F.col('COUNT') / {row_total} * F.lit(100))",
                    f"{frequency_frame} = {frequency_frame}.withColumn("
                    f"'COL_PERCENT', F.col('COUNT') / {column_total} * F.lit(100))",
                ]
            )
        lines.extend(
            [
                f"{frequency_frame} = {frequency_frame}.orderBy({order})",
            ]
        )
        if target:
            lines.append(
                f"{frequency_frame}.createOrReplaceTempView({_sql_name(target)!r})"
            )
        lines.append("")
        return lines, index + 1, freq_count

    def _convert_means(
        self, statements: list[_Statement], start: int, means_count: int
    ) -> tuple[list[str], int, int]:
        header_options = statements[start].text.split()[2:]
        data: str | None = None
        statistics: list[str] = []
        supported_statistics = {
            "n",
            "nmiss",
            "sum",
            "mean",
            "std",
            "stderr",
            "var",
            "cv",
            "min",
            "max",
        }
        for option in header_options:
            if option.lower().startswith("data="):
                if data is not None:
                    raise ConversionError("PROC MEANS/SUMMARY accepts DATA= only once.")
                data = _dataset_name(option[5:])
            elif option.lower() in supported_statistics:
                statistic = option.lower()
                if statistic in statistics:
                    raise ConversionError(
                        f"PROC MEANS/SUMMARY statistic {option!r} is repeated."
                    )
                statistics.append(statistic)
            else:
                raise ConversionError(
                    f"Unsupported PROC MEANS/SUMMARY option: {option!r}."
                )

        class_columns: tuple[str, ...] | None = None
        var_columns: tuple[str, ...] | None = None
        target: str | None = None
        output_names: dict[str, tuple[str, ...]] = {}
        output_seen = False
        index = start + 1
        while index < len(statements) and statements[index].text.lower() != "run":
            source_statement = statements[index]
            statement = source_statement.text.strip()
            lower = statement.lower()
            try:
                if lower.startswith("class "):
                    if class_columns is not None:
                        raise ConversionError(
                            "PROC MEANS/SUMMARY supports one CLASS statement."
                        )
                    class_columns = self._parse_means_columns(statement[6:], "CLASS")
                elif lower.startswith("var "):
                    if var_columns is not None:
                        raise ConversionError(
                            "PROC MEANS/SUMMARY supports one VAR statement."
                        )
                    var_columns = self._parse_means_columns(statement[4:], "VAR")
                elif lower.startswith("output "):
                    if output_seen:
                        raise ConversionError(
                            "PROC MEANS/SUMMARY supports one OUTPUT statement."
                        )
                    output_options = statement.split()[1:]
                    current_statistic: str | None = None
                    for option in output_options:
                        option_match = re.fullmatch(
                            r"([A-Za-z_]\w*)=((?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*)",
                            option,
                        )
                        if option_match:
                            key, value = option_match.groups()
                            key = key.lower()
                            if key == "out":
                                if target is not None:
                                    raise ConversionError(
                                        "PROC MEANS/SUMMARY OUTPUT accepts OUT= only once."
                                    )
                                target = _dataset_name(value)
                                current_statistic = None
                            elif key in supported_statistics:
                                if key not in statistics:
                                    raise ConversionError(
                                        f"PROC MEANS/SUMMARY OUTPUT statistic "
                                        f"{key.upper()} must be requested on the PROC statement."
                                    )
                                if key in output_names:
                                    raise ConversionError(
                                        f"PROC MEANS/SUMMARY OUTPUT statistic "
                                        f"{key.upper()} is specified more than once."
                                    )
                                output_names[key] = (value,)
                                current_statistic = key
                            else:
                                raise ConversionError(
                                    f"Unsupported PROC MEANS/SUMMARY OUTPUT option: "
                                    f"{option!r}."
                                )
                        elif current_statistic is not None and re.fullmatch(
                            r"[A-Za-z_]\w*", option
                        ):
                            output_names[current_statistic] += (option,)
                        else:
                            raise ConversionError(
                                "PROC MEANS/SUMMARY OUTPUT supports OUT= and "
                                "statistic=output-name lists."
                            )
                    output_seen = True
                else:
                    raise ConversionError(
                        f"Unsupported PROC MEANS/SUMMARY statement: "
                        f"{statement[:80]!r}."
                    )
            except ConversionError as error:
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1

        if index >= len(statements):
            raise ConversionError("PROC MEANS/SUMMARY is missing RUN.")
        if data is None:
            raise ConversionError("PROC MEANS/SUMMARY requires DATA=.")
        if not statistics:
            raise ConversionError(
                "PROC MEANS/SUMMARY requires at least one supported statistic "
                "option: N, SUM, MEAN, STD, MIN, or MAX."
            )
        if var_columns is None:
            raise ConversionError("PROC MEANS/SUMMARY requires one VAR statement.")

        for statistic, names in output_names.items():
            if len(names) != len(var_columns):
                raise ConversionError(
                    f"PROC MEANS/SUMMARY OUTPUT {statistic.upper()}= requires "
                    f"exactly {len(var_columns)} output names for the VAR columns."
                )
            if len({name.lower() for name in names}) != len(names):
                raise ConversionError(
                    f"PROC MEANS/SUMMARY OUTPUT {statistic.upper()}= names "
                    "must be distinct."
                )

        output_columns = [
            output_names.get(statistic, tuple(
                f"{statistic}_{column}".lower() for column in var_columns
            ))
            for statistic in statistics
        ]
        flattened_output_columns = [
            column for columns in output_columns for column in columns
        ]
        if len({column.lower() for column in flattened_output_columns}) != len(
            flattened_output_columns
        ):
            raise ConversionError(
                "PROC MEANS/SUMMARY output statistic names must be distinct."
            )
        if class_columns and {
            column.lower() for column in flattened_output_columns
        }.intersection(column.lower() for column in class_columns):
            raise ConversionError(
                "PROC MEANS/SUMMARY output statistic names must not conflict "
                "with CLASS column names."
            )

        means_count += 1
        frame = _python_name(data)
        target_frame = _python_name(target) if target else f"df_means_{means_count}"
        aggregates: list[str] = []
        functions = {
            "n": "count",
            "nmiss": "nmiss",
            "sum": "sum",
            "mean": "avg",
            "std": "stddev_samp",
            "stderr": "stderr",
            "var": "var_samp",
            "cv": "cv",
            "min": "min",
            "max": "max",
        }
        for statistic, names in zip(statistics, output_columns):
            for column, output_name in zip(var_columns, names):
                column_ref = f"F.col({column!r})"
                if statistic == "nmiss":
                    aggregate = f"F.count(F.when({column_ref}.isNull(), F.lit(1)))"
                elif statistic == "stderr":
                    aggregate = (
                        f"(F.stddev_samp({column_ref}) / "
                        f"F.sqrt(F.count({column_ref})))"
                    )
                elif statistic == "cv":
                    mean = f"F.avg({column_ref})"
                    aggregate = (
                        f"F.when({mean} != F.lit(0), "
                        f"F.stddev_samp({column_ref}) / {mean} * F.lit(100))"
                    )
                else:
                    aggregate = f"F.{functions[statistic]}({column_ref})"
                aggregates.append(f"{aggregate}.alias({output_name!r})")

        lines: list[str] = []
        if class_columns:
            class_filter = " & ".join(
                f"F.col({column!r}).isNotNull()" for column in class_columns
            )
            class_selection = ", ".join(
                f"F.col({column!r})" for column in class_columns
            )
            lines.append(f"__sas2pyspark_means_input = {frame}.filter({class_filter})")
            lines.append(
                f"{target_frame} = __sas2pyspark_means_input.groupBy("
                f"{class_selection}).agg({', '.join(aggregates)})"
            )
            order = ", ".join(
                f"F.col({column!r}).asc()" for column in class_columns
            )
            lines.append(f"{target_frame} = {target_frame}.orderBy({order})")
        else:
            lines.append(f"{target_frame} = {frame}.agg({', '.join(aggregates)})")
        if target:
            lines.append(
                f"{target_frame}.createOrReplaceTempView({_sql_name(target)!r})"
            )
        lines.append("")
        return lines, index + 1, means_count

    @staticmethod
    def _parse_means_columns(statement: str, kind: str) -> tuple[str, ...]:
        columns = statement.split()
        if not columns or any(
            not re.fullmatch(r"[A-Za-z_]\w*", column) for column in columns
        ):
            raise ConversionError(
                f"PROC MEANS/SUMMARY {kind} supports simple column names."
            )
        if len({column.lower() for column in columns}) != len(columns):
            raise ConversionError(
                f"PROC MEANS/SUMMARY {kind} column names must be distinct."
            )
        return tuple(columns)

    def _convert_import(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int]:
        options: dict[str, str | None] = {}

        def parse_options(text: str, source_statement: _Statement) -> None:
            position = 0
            while position < len(text):
                whitespace = re.match(r"\s*", text[position:])
                position += whitespace.end() if whitespace else 0
                if position == len(text):
                    break
                match = _PROC_IMPORT_TOKEN.match(text, position)
                if match is None:
                    raise ConversionError(
                        "Malformed PROC IMPORT option.",
                        line=source_statement.line,
                        statement=source_statement.text,
                    )
                if text[position:match.start()].strip():
                    raise ConversionError(
                        "Malformed PROC IMPORT option.",
                        line=source_statement.line,
                        statement=source_statement.text,
                    )
                key = (match.group(1) or match.group(3)).lower()
                value = match.group(2)
                if key in options:
                    raise ConversionError(
                        f"PROC IMPORT option {key.upper()} is specified more than once.",
                        line=source_statement.line,
                        statement=source_statement.text,
                    )
                if value is not None and value[:1] in {"'", '"'}:
                    quote = value[0]
                    if value[-1] != quote:
                        raise ConversionError(
                            f"Unterminated quoted PROC IMPORT {key.upper()} value.",
                            line=source_statement.line,
                            statement=source_statement.text,
                        )
                    value = value[1:-1].replace(quote * 2, quote)
                options[key] = value
                position = match.end()

        header = statements[start]
        parse_options(header.text[len("proc import") :].strip(), header)
        index = start + 1
        while index < len(statements) and statements[index].text.lower() != "run":
            source_statement = statements[index]
            try:
                parse_options(source_statement.text.strip(), source_statement)
            except ConversionError as error:
                if error.line is not None:
                    raise
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1

        if index >= len(statements):
            raise ConversionError("PROC IMPORT is missing RUN.")
        datafile = options.get("datafile")
        target = options.get("out")
        dbms = options.get("dbms")
        if not datafile:
            raise ConversionError("PROC IMPORT requires DATAFILE=.")
        if not target:
            raise ConversionError("PROC IMPORT requires OUT=.")
        if dbms is None or dbms.lower() != "csv":
            raise ConversionError("PROC IMPORT supports only DBMS=CSV.")
        if "replace" in options and options["replace"] is not None:
            raise ConversionError("PROC IMPORT REPLACE does not accept a value.")
        for required in ("datafile", "out", "dbms"):
            if required not in options:
                raise ConversionError(
                    f"PROC IMPORT requires {required.upper()}=."
                )
        getnames = options.get("getnames", "yes")
        if getnames is None or getnames.lower() not in {"yes", "no"}:
            raise ConversionError("PROC IMPORT GETNAMES= must be YES or NO.")
        delimiter = options.get("delimiter", ",")
        if delimiter is None or len(delimiter) != 1:
            raise ConversionError(
                "PROC IMPORT DELIMITER= must be exactly one character."
            )
        if set(options).difference(
            {"datafile", "out", "dbms", "getnames", "delimiter", "replace"}
        ):
            unsupported = sorted(
                set(options).difference(
                    {"datafile", "out", "dbms", "getnames", "delimiter", "replace"}
                )
            )[0]
            raise ConversionError(
                f"Unsupported PROC IMPORT option: {unsupported.upper()}."
            )

        target_name = _dataset_name(target)
        frame = _python_name(target_name)
        lines = [
            f"{frame} = (spark.read.option('header', "
            f"{getnames.lower() == 'yes'!r}).option('inferSchema', True)"
            f".option('sep', {delimiter!r}).option('mode', 'FAILFAST')"
            f".csv({datafile!r}))",
            f"{frame}.createOrReplaceTempView({_sql_name(target_name)!r})",
            "",
        ]
        return lines, index + 1

    def _convert_export(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int]:
        options: dict[str, str | None] = {}

        def parse_options(text: str, source_statement: _Statement) -> None:
            position = 0
            while position < len(text):
                whitespace = re.match(r"\s*", text[position:])
                position += whitespace.end() if whitespace else 0
                if position == len(text):
                    break
                match = _PROC_IMPORT_TOKEN.match(text, position)
                if match is None or text[position:match.start()].strip():
                    raise ConversionError(
                        "Malformed PROC EXPORT option.",
                        line=source_statement.line,
                        statement=source_statement.text,
                    )
                key = (match.group(1) or match.group(3)).lower()
                value = match.group(2)
                if key in options:
                    raise ConversionError(
                        f"PROC EXPORT option {key.upper()} is specified more than once.",
                        line=source_statement.line,
                        statement=source_statement.text,
                    )
                if value is not None and value[:1] in {"'", '"'}:
                    quote = value[0]
                    if value[-1] != quote:
                        raise ConversionError(
                            f"Unterminated quoted PROC EXPORT {key.upper()} value.",
                            line=source_statement.line,
                            statement=source_statement.text,
                        )
                    value = value[1:-1].replace(quote * 2, quote)
                options[key] = value
                position = match.end()

        header = statements[start]
        parse_options(header.text[len("proc export") :].strip(), header)
        index = start + 1
        while index < len(statements) and statements[index].text.lower() != "run":
            source_statement = statements[index]
            parse_options(source_statement.text.strip(), source_statement)
            index += 1

        if index >= len(statements):
            raise ConversionError("PROC EXPORT is missing RUN.")
        data = options.get("data")
        outfile = options.get("outfile")
        dbms = options.get("dbms")
        if "data" not in options:
            raise ConversionError("PROC EXPORT requires DATA=.")
        if not data:
            raise ConversionError("PROC EXPORT DATA= must name a dataset.")
        if "outfile" not in options:
            raise ConversionError("PROC EXPORT requires OUTFILE=.")
        if not outfile:
            raise ConversionError("PROC EXPORT OUTFILE= must name a path.")
        if dbms is None or dbms.lower() != "csv":
            raise ConversionError("PROC EXPORT supports only DBMS=CSV.")
        if "replace" in options and options["replace"] is not None:
            raise ConversionError("PROC EXPORT REPLACE does not accept a value.")
        putnames = options.get("putnames", "yes")
        if putnames is None or putnames.lower() not in {"yes", "no"}:
            raise ConversionError("PROC EXPORT PUTNAMES= must be YES or NO.")
        delimiter = options.get("delimiter", ",")
        if delimiter is None or len(delimiter) != 1:
            raise ConversionError(
                "PROC EXPORT DELIMITER= must be exactly one character."
            )
        supported = {"data", "outfile", "dbms", "putnames", "delimiter", "replace"}
        unsupported = set(options).difference(supported)
        if unsupported:
            raise ConversionError(
                f"Unsupported PROC EXPORT option: {sorted(unsupported)[0].upper()}."
            )

        replace = "replace" in options
        write_mode = "overwrite" if replace else "error"
        frame = _python_name(data)
        lines = [
            f"{frame}.write.mode({write_mode!r}).option('header', "
            f"{putnames.lower() == 'yes'!r}).option('sep', {delimiter!r})"
            f".csv({outfile!r})",
            "",
        ]
        return lines, index + 1

    def _convert_transpose(
        self, statements: list[_Statement], start: int, transpose_count: int
    ) -> tuple[list[str], int, int]:
        data: str | None = None
        target: str | None = None
        order_column: str | None = None
        for option in statements[start].text.split()[2:]:
            lower_option = option.lower()
            if lower_option.startswith("data="):
                if data is not None:
                    raise ConversionError("PROC TRANSPOSE accepts DATA= only once.")
                data = _dataset_name(option[5:])
            elif lower_option.startswith("out="):
                if target is not None:
                    raise ConversionError("PROC TRANSPOSE accepts OUT= only once.")
                target = _dataset_name(option[4:])
            elif lower_option.startswith("order="):
                if order_column is not None:
                    raise ConversionError("PROC TRANSPOSE accepts ORDER= only once.")
                order_column = option[6:]
                if not re.fullmatch(r"[A-Za-z_]\w*", order_column):
                    raise ConversionError(
                        "PROC TRANSPOSE ORDER= requires one simple ordering column."
                    )
            else:
                raise ConversionError(
                    f"Unsupported PROC TRANSPOSE option: {option!r}."
                )

        by_columns: tuple[str, ...] = ()
        var_columns: tuple[str, ...] | None = None
        index = start + 1
        while index < len(statements) and statements[index].text.lower() != "run":
            source_statement = statements[index]
            statement = source_statement.text.strip()
            lower = statement.lower()
            try:
                if lower.startswith("by "):
                    if by_columns:
                        raise ConversionError(
                            "PROC TRANSPOSE supports one BY statement."
                        )
                    by_columns = self._parse_means_columns(statement[3:], "BY")
                elif lower.startswith("var "):
                    if var_columns is not None:
                        raise ConversionError(
                            "PROC TRANSPOSE supports one VAR statement."
                        )
                    var_columns = self._parse_means_columns(statement[4:], "VAR")
                else:
                    raise ConversionError(
                        f"Unsupported PROC TRANSPOSE statement: {statement[:80]!r}."
                    )
            except ConversionError as error:
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1

        if index >= len(statements):
            raise ConversionError("PROC TRANSPOSE is missing RUN.")
        if data is None:
            raise ConversionError("PROC TRANSPOSE requires DATA=.")
        if target is None:
            transpose_count += 1
            target_frame = f"df_transpose_{transpose_count}"
        else:
            target_frame = _python_name(target)
        if var_columns is None:
            raise ConversionError("PROC TRANSPOSE requires one VAR statement.")
        if order_column is None:
            raise ConversionError(
                "PROC TRANSPOSE requires ORDER= to assign source observations "
                "deterministically to COL1, COL2, etc."
            )
        transpose_input_columns = [
            *by_columns,
            order_column,
            *var_columns,
        ]
        if len({column.lower() for column in transpose_input_columns}) != len(
            transpose_input_columns
        ):
            raise ConversionError(
                "PROC TRANSPOSE BY, VAR, and ORDER= columns must be distinct."
            )
        frame = _python_name(data)
        required_columns = tuple(dict.fromkeys((*by_columns, order_column, *var_columns)))
        required = repr(list(required_columns))
        partition = ", ".join(f"F.col({column!r})" for column in by_columns)
        window = (
            f"__sas2pyspark_window.partitionBy({partition}).orderBy("
            f"F.col({order_column!r}).asc())"
            if by_columns
            else f"__sas2pyspark_window.orderBy(F.col({order_column!r}).asc())"
        )
        grouping = ", ".join(
            f"F.col({column!r})" for column in (*by_columns, "_NAME_")
        )
        ordering = ", ".join(
            f"F.col({column!r}).asc()" for column in (*by_columns, "_NAME_")
        )
        lines = [
            "from pyspark.sql import Window as __sas2pyspark_window",
            f"__sas2pyspark_transpose_source = {frame}",
            f"__sas2pyspark_transpose_missing = [column for column in {required} "
            "if column not in __sas2pyspark_transpose_source.columns]",
            "if __sas2pyspark_transpose_missing:",
            "    raise ValueError('PROC TRANSPOSE input is missing required columns: ' + ', '.join(__sas2pyspark_transpose_missing))",
            f"__sas2pyspark_transpose_types = [__sas2pyspark_transpose_source.schema[column].dataType "
            f"for column in {list(var_columns)!r}]",
            "if any(data_type != __sas2pyspark_transpose_types[0] for data_type in __sas2pyspark_transpose_types[1:]):",
            "    raise TypeError('PROC TRANSPOSE VAR columns must have the same Spark data type.')",
            f"if __sas2pyspark_transpose_source.groupBy("
            f"{', '.join(f'F.col({column!r})' for column in (*by_columns, order_column))}"
            ").count().filter(F.col('count') > 1).limit(1).count():",
            "    raise ValueError('PROC TRANSPOSE ORDER= values must be unique within each BY group.')",
            "if any(column.lower() == '_name_' or (column.lower().startswith('col') and column[3:].isdigit()) for column in "
            f"{list(by_columns)!r}):",
            "    raise ValueError('PROC TRANSPOSE BY columns conflict with generated output columns.')",
            "__sas2pyspark_transpose_ranked = __sas2pyspark_transpose_source.withColumn("
            "'__sas2pyspark_transpose_row', F.row_number().over("
            f"{window}))",
        ]
        long_parts: list[str] = []
        for column in var_columns:
            selected = [
                f"F.col({by_column!r})" for by_column in by_columns
            ]
            selected.extend(
                [
                    f"F.lit({column!r}).alias('_NAME_')",
                    "F.col('__sas2pyspark_transpose_row').alias('__sas2pyspark_transpose_index')",
                    f"F.col({column!r}).alias('__sas2pyspark_transpose_value')",
                ]
            )
            long_parts.append(
                "__sas2pyspark_transpose_ranked.select("
                + ", ".join(selected)
                + ")"
            )
        lines.append(
            "__sas2pyspark_transpose_long = "
            + " .unionByName(".join(long_parts)
            + ")" * (len(long_parts) - 1)
        )
        lines.append(
            "__sas2pyspark_transpose_width = "
            "__sas2pyspark_transpose_long.agg("
            "F.max('__sas2pyspark_transpose_index').alias('width')"
            ").first()['width'] or 0"
        )
        lines.append(
            "if __sas2pyspark_transpose_width > 10000:"
        )
        lines.append(
            "    raise ValueError('PROC TRANSPOSE output exceeds the supported 10000 COLn columns.')"
        )
        lines.append(
            "__sas2pyspark_transpose_aggregates = ["
            "F.max(F.when(F.col('__sas2pyspark_transpose_index') == index, "
            "F.col('__sas2pyspark_transpose_value'))).alias(f'COL{index}') "
            "for index in range(1, __sas2pyspark_transpose_width + 1)]"
        )
        lines.extend(
            [
                "if __sas2pyspark_transpose_width:",
                f"    {target_frame} = __sas2pyspark_transpose_long.groupBy("
                f"{grouping}).agg(*__sas2pyspark_transpose_aggregates)",
                "else:",
                f"    {target_frame} = __sas2pyspark_transpose_long.select("
                + ", ".join(
                    [*(f"F.col({column!r})" for column in by_columns), "F.col('_NAME_')"]
                )
                + ").distinct()",
            ]
        )
        lines.append(f"{target_frame} = {target_frame}.orderBy({ordering})")
        if target:
            lines.append(
                f"{target_frame}.createOrReplaceTempView({_sql_name(target)!r})"
            )
        lines.append("")
        return lines, index + 1, transpose_count

    def _convert_sql(
        self, statements: list[_Statement], start: int, sql_count: int
    ) -> tuple[list[str], int, int]:
        lines: list[str] = []
        index = start + 1
        while index < len(statements) and statements[index].text.lower() != "quit":
            source_statement = statements[index]
            statement = source_statement.text.strip()
            if not statement:
                index += 1
                continue
            try:
                create = _SQL_CREATE.fullmatch(statement)
                if create:
                    target = _dataset_name(create.group(1))
                    query = self._normalize_sql(create.group(2))
                    lines.extend(
                        [
                            f"{_python_name(target)} = spark.sql({query!r})",
                            f"{_python_name(target)}.createOrReplaceTempView({target!r})",
                            "",
                        ]
                    )
                elif re.match(r"^(select|with)\b", statement, re.I):
                    sql_count += 1
                    query = self._normalize_sql(statement)
                    lines.extend(
                        [
                            f"df_sql_{sql_count} = spark.sql({query!r})",
                            "",
                        ]
                    )
                else:
                    raise ConversionError(
                        f"Unsupported PROC SQL statement: {statement[:80]!r}."
                    )
            except ConversionError as error:
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1
        if index >= len(statements):
            raise ConversionError("PROC SQL is missing QUIT.")
        return lines, index + 1, sql_count

    @staticmethod
    def _normalize_sql(query: str) -> str:
        """Normalize common SAS SQL table references and SAS comparison words."""
        normalized = _expression(query)
        return _map_unquoted(
            normalized,
            lambda part: re.sub(
                r"\b(FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b",
                lambda match: f"{match.group(1)} {match.group(3)}",
                part,
                flags=re.IGNORECASE,
            ),
        ).strip()


def convert_sas(source: str) -> ConversionResult:
    """Convenience function for converting SAS source text."""
    return SasToPySparkConverter().convert(source)
