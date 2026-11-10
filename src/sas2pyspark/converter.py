"""SAS-to-PySpark conversion for a deliberately bounded SAS subset."""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .includes import IncludeError, inline_includes
from .user_formats import (
    FormatError,
    cntlin_dataset,
    has_format,
    register_invalue,
    register_cntlin,
    register_picture,
    register_value,
    reset_formats,
)

from .macro_processor import (
    MacroExpansionError,
    MacroStatement as _Statement,
    expand_macros,
)
from .data_blocks import rewrite_if_blocks
from .data_length import rewrite_length
from .data_retain import rewrite_retain
from .data_unroll import parse_constant_integer, rewrite_arrays_and_loops
from .data_order import OrderByError, OrderKey, parse_order_by
from .ir import DataAction as _DataAction, DataStepIR
from .data_outputs import rewrite_outputs
from .data_lines import DATALINES_MARKER, rewrite_datalines
from .data_lag import rewrite_lag
from .set_options import SETOPTS_MARKER, rewrite_set_options
from .data_rownum import rewrite_row_number
from .data_symput import SYMPUT_MARKER, VALUE_COLUMN, rewrite_symput
from .date_functions import (
    DATE_FUNCTIONS,
    DateFunctionError,
    render_date_function,
    render_format,
    translate_date_literals,
)
from .sas_parser import SasParseError, SasStatement, parse_program
from .sql_calculated import rewrite_calculated

# Source-to-source passes applied in order after macro expansion.
_REWRITE_PASSES = (
    rewrite_set_options,
    rewrite_arrays_and_loops,
    rewrite_lag,
    rewrite_row_number,
    rewrite_datalines,
    rewrite_symput,
    rewrite_outputs,
    rewrite_if_blocks,
    rewrite_retain,
    rewrite_length,
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
    source_version: str = "Viya"
    version_source: str = "default"


_SAS_VERSION_PROFILES = {
    "9.4m4": "9.4M4",
    "9.4m8": "9.4M8",
    "viya": "Viya",
}
_SAS_VERSION_MARKER = re.compile(
    r"^\s*sas2pyspark\s*:\s*sas-version\s*=\s*([^*\s]+)\s*$",
    re.IGNORECASE,
)
_REMOTE_SUBMIT_CONTROL = re.compile(r"^\s*(signon|signoff|rsubmit|endrsubmit)\b", re.I)
_REMOTE_SUBMIT_OPTION = re.compile(r"^\s*options\b.*\bremote\s*=", re.I)


def _sas_version_markers(source: str) -> list[str]:
    markers: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(source):
        char = source[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(source) and source[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char
        elif source.startswith("/*", index):
            end = source.find("*/", index + 2)
            if end < 0:
                break
            body = source[index + 2 : end]
            if "sas2pyspark" in body.casefold() and "sas-version" in body.casefold():
                match = _SAS_VERSION_MARKER.fullmatch(body)
                if match is None:
                    raise ConversionError("Malformed sas2pyspark SAS version marker.")
                markers.append(match.group(1).casefold())
            index = end + 1
        index += 1
    return markers


def _resolve_sas_version(
    source: str, requested_version: str
) -> tuple[str, str, str | None]:
    if not isinstance(requested_version, str):
        raise ConversionError("SAS version selection must be a string.")
    requested_key = requested_version.casefold()
    if requested_key != "auto" and requested_key not in _SAS_VERSION_PROFILES:
        supported = ", ".join(("auto", *_SAS_VERSION_PROFILES.values()))
        raise ConversionError(
            f"Unsupported SAS source version {requested_version!r}; "
            f"choose one of {supported}."
        )

    marker_values = _sas_version_markers(source)
    marker_versions = {
        _SAS_VERSION_PROFILES.get(value) for value in marker_values
    }
    if None in marker_versions:
        invalid_value = next(
            value for value in marker_values if value not in _SAS_VERSION_PROFILES
        )
        raise ConversionError(
            f"Unsupported SAS version in sas2pyspark source marker: "
            f"{invalid_value!r}."
        )
    if len(marker_versions) > 1:
        raise ConversionError(
            "SAS source contains conflicting sas2pyspark version markers."
        )

    marker_version = next(iter(marker_versions), None)
    if requested_key != "auto":
        selected_version = _SAS_VERSION_PROFILES[requested_key]
        if marker_version is not None and marker_version != selected_version:
            raise ConversionError(
                f"Requested SAS version {selected_version} conflicts with source "
                f"marker {marker_version}."
            )
        return (
            selected_version,
            "override",
            f"SAS version profile: {selected_version} selected by override. "
            "Per-release syntax and semantic gates are not implemented yet; "
            "the selection records the asserted source release only.",
        )
    if marker_version is not None:
        return (
            marker_version,
            "marker",
            f"SAS version profile: {marker_version} selected by source marker. "
            "Per-release syntax and semantic gates are not implemented yet; "
            "the marker records the asserted source release only.",
        )
    return (
        "Viya",
        "default",
        "SAS version profile: source version was not declared; selected the "
        "Viya compatibility profile. Per-release syntax and semantic gates "
        "are not implemented yet.",
    )


def _reject_sas_connect_statement(statement: SasStatement | _Statement) -> None:
    if _REMOTE_SUBMIT_CONTROL.match(statement.text) or _REMOTE_SUBMIT_OPTION.match(
        statement.text
    ):
        command = statement.text.strip().split(None, 1)[0].upper()
        raise ConversionError(
            f"{command} is a SAS/CONNECT control statement and cannot be "
            "flattened into local PySpark. Resolve its remote server and "
            "convert remote code separately.",
            line=(
                statement.span.start_line
                if isinstance(statement, SasStatement)
                else statement.line
            ),
            statement=statement.text,
        )


_DATASET = re.compile(r"^(?:[A-Za-z_]\w*\.)?([A-Za-z_]\w*)$")
_ASSIGNMENT = re.compile(r"^([A-Za-z_]\w*)\s*=(?!=|>)\s*(.+)$", re.DOTALL)
_DATA_ASSIGNMENT = re.compile(
    r"^([A-Za-z_]\w*(?:\[\s*[^\]]+\s*\])?)\s*=(?!=|>)\s*(.+)$",
    re.DOTALL,
)
_ARRAY_REFERENCE = re.compile(r"\b([A-Za-z_]\w*)\s*\[\s*([^\]]+)\s*\]")
_IF_THEN = re.compile(r"^if\s+(.+?)\s+then(?:\s+(.+))?$", re.IGNORECASE | re.DOTALL)
_DO_LOOP = re.compile(
    r"^do\s+([A-Za-z_]\w*)\s*=\s*(.+?)\s+to\s*(.+?)(?:\s+by\s+(.+))?$",
    re.IGNORECASE,
)
_SQL_CREATE = re.compile(
    r"^\s*create\s+table\s+((?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*)\s+as\s+(.+)$",
    re.IGNORECASE | re.DOTALL,
)
_SQL_INTO = re.compile(
    r"\binto\s+((?::\s*[A-Za-z_]\w*\s*,?\s*)+)(?:(separated\s+by)\b|(?:trimmed|notrim)\b)?",
    re.IGNORECASE,
)
_PROC_IMPORT_TOKEN = re.compile(
    r"([A-Za-z_]\w*)\s*=\s*(\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*'|[^\s]+)"
    r"|([A-Za-z_]\w*)"
)


def _dataset_name(value: str) -> str:
    match = _DATASET.fullmatch(value.strip())
    if not match:
        raise ConversionError(f"Unsupported SAS dataset reference: {value!r}.")
    return match.group(1).lower()


def _call_missing_names(statement: str) -> tuple[str, ...]:
    call = re.fullmatch(
        r"call\s+missing\s*\((.*)\)", statement.strip(), re.IGNORECASE | re.DOTALL
    )
    names = [name.strip() for name in call.group(1).split(",")] if call else []
    if not names or any(not re.fullmatch(r"[A-Za-z_]\w*", name) for name in names):
        raise ConversionError(
            "CALL MISSING supports one or more simple variable names, "
            "for example CALL MISSING(score, label)."
        )
    unique_names: dict[str, str] = {}
    for name in names:
        unique_names.setdefault(name.casefold(), name)
    return tuple(unique_names.values())


def _python_name(dataset: str) -> str:
    return f"df_{_dataset_name(dataset)}"


def _sql_name(dataset: str) -> str:
    """Use the table component of a two-level SAS name as a Spark view name."""
    return _dataset_name(dataset)


def _parse_merge_inputs(
    text: str,
) -> tuple[
    tuple[str, ...],
    tuple[str, ...],
    tuple[tuple[OrderKey, ...], ...],
]:
    sources: list[str] = []
    in_vars: list[str] = []
    order_keys: list[tuple[OrderKey, ...]] = []
    position = 0
    while position < len(text):
        while position < len(text) and text[position].isspace():
            position += 1
        if position == len(text):
            break
        dataset = re.match(r"(?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*", text[position:])
        if dataset is None:
            raise ConversionError(f"Unsupported SAS dataset reference: {text[position:]!r}.")
        sources.append(_dataset_name(dataset.group(0)))
        position += dataset.end()
        input_flag = ""
        input_order: tuple[OrderKey, ...] = ()
        while position < len(text) and text[position].isspace():
            position += 1
        if position < len(text) and text[position] == "(":
            position += 1
            while True:
                while position < len(text) and text[position].isspace():
                    position += 1
                if position >= len(text):
                    raise ConversionError("Unclosed MERGE dataset options.")
                if text[position] == ")":
                    position += 1
                    break
                flag = re.match(r"in\s*=\s*([A-Za-z_]\w*)", text[position:], re.I)
                if flag is not None:
                    if input_flag:
                        raise ConversionError("MERGE input supports only one IN= option.")
                    input_flag = flag.group(1)
                    position += flag.end()
                    continue
                order_option = re.match(r"order_by\s*=\s*\(", text[position:], re.I)
                if order_option is not None:
                    if input_order:
                        raise ConversionError(
                            "MERGE input supports only one ORDER_BY option."
                        )
                    position += order_option.end()
                    end = text.find(")", position)
                    if end < 0:
                        raise ConversionError("Unclosed MERGE ORDER_BY option.")
                    try:
                        parsed = parse_order_by("order by " + text[position:end])
                    except OrderByError as error:
                        raise ConversionError(str(error)) from error
                    if parsed is None:
                        raise ConversionError("MERGE ORDER_BY requires simple column names.")
                    input_order = parsed
                    position = end + 1
                    continue
                raise ConversionError(
                    "Unsupported SAS dataset reference; MERGE dataset options "
                    "support IN= and ORDER_BY=(key, ...) only."
                )
        in_vars.append(input_flag)
        order_keys.append(input_order)
    if order_keys and any(order_keys) and not all(order_keys):
        raise ConversionError(
            "Specify ORDER_BY for every MERGE input or omit it from all inputs."
        )
    return tuple(sources), tuple(in_vars), tuple(order_keys)


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
    try:
        expression = translate_date_literals(expression)
    except DateFunctionError as error:
        raise ConversionError(str(error)) from error
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
        (r"\btoday\s*\(\s*\)", "datediff(current_date(), DATE'1960-01-01')"),
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
    value: str,
    arrays: dict[str, tuple[str, ...]],
    *,
    allow_dynamic_read: bool = False,
    on_dynamic_read: Callable[[tuple[str, ...], str, tuple[str, ...]], None] | None = None,
) -> str:
    """Replace SAS array references with columns or checked dynamic reads."""

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
                if allow_dynamic_read:
                    index_expression, index_columns = _dynamic_array_index(index_text)
                    elements = arrays[array_name]
                    if on_dynamic_read is not None:
                        on_dynamic_read(elements, index_expression, index_columns)
                    values = ", ".join(f"`{element}`" for element in elements)
                    return (
                        f"element_at(array({values}), "
                        f"CAST(({index_expression}) AS INT))"
                    )
                raise ConversionError(
                    "SAS array references require a positive integer literal index."
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


def _dynamic_array_index(value: str) -> tuple[str, tuple[str, ...]]:
    try:
        parsed = ast.parse(value, mode="eval")
    except SyntaxError as error:
        raise ConversionError(
            "Dynamic SAS array indexes support numeric arithmetic expressions only."
        ) from error

    allowed_nodes = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Mod,
        ast.UAdd,
        ast.USub,
        ast.Constant,
        ast.Name,
        ast.Load,
    )
    names: dict[str, str] = {}
    for node in ast.walk(parsed):
        if not isinstance(node, allowed_nodes):
            raise ConversionError(
                "Dynamic SAS array indexes support identifiers, numeric literals, "
                "parentheses, and +, -, *, /, or % arithmetic only."
            )
        if isinstance(node, ast.Constant) and (
            isinstance(node.value, bool) or not isinstance(node.value, (int, float))
        ):
            raise ConversionError("Dynamic SAS array indexes must be numeric.")
        if isinstance(node, ast.Name):
            names.setdefault(node.id.casefold(), node.id)
    if not names:
        raise ConversionError(
            "Dynamic SAS array indexes must reference at least one numeric variable."
        )
    return value, tuple(names.values())


def _dynamic_loop_bound(value: str) -> tuple[str, tuple[str, ...]]:
    try:
        parsed = ast.parse(value, mode="eval")
    except SyntaxError as error:
        raise ConversionError(
            "Dynamic DO loop bounds support numeric arithmetic over input columns only."
        ) from error

    allowed_nodes = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Mod,
        ast.UAdd,
        ast.USub,
        ast.Constant,
        ast.Name,
        ast.Load,
    )
    names: dict[str, str] = {}
    for node in ast.walk(parsed):
        if not isinstance(node, allowed_nodes):
            raise ConversionError(
                "Dynamic DO loop bounds support identifiers, numeric literals, "
                "parentheses, and +, -, *, /, or % arithmetic only."
            )
        if isinstance(node, ast.Constant) and (
            isinstance(node.value, bool) or not isinstance(node.value, (int, float))
        ):
            raise ConversionError("Dynamic DO loop bounds must be numeric.")
        if isinstance(node, ast.Name):
            names.setdefault(node.id.casefold(), node.id)
    if not names:
        raise ConversionError(
            "Dynamic DO loop bounds must reference an input column."
        )
    return value, tuple(names.values())


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
        } | DATE_FUNCTIONS or (
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
    if name in DATE_FUNCTIONS:
        try:
            return render_date_function(name, arguments)
        except DateFunctionError as error:
            raise ConversionError(str(error)) from error
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

    def __init__(self, *, allow_sysget: bool = False) -> None:
        self.allow_sysget = allow_sysget

    def convert(
        self,
        source: str,
        *,
        sas_version: str = "auto",
        include_dir: str | Path | None = None,
        include_root: str | Path | None = None,
    ) -> ConversionResult:
        selected_version, version_source, version_warning = _resolve_sas_version(
            source, sas_version
        )
        reset_formats()
        try:
            source = inline_includes(source, include_dir, include_root)
        except IncludeError as error:
            raise ConversionError(error.message, line=error.line) from error
        try:
            parsed_program = parse_program(source)
            for statement in parsed_program.statements:
                _reject_sas_connect_statement(statement)
            statements = expand_macros(
                [
                    _Statement(statement.text, statement.span.start_line)
                    for statement in parsed_program.statements
                ],
                allow_sysget=self.allow_sysget,
            )
            for statement in statements:
                _reject_sas_connect_statement(statement)
            for rewrite in _REWRITE_PASSES:
                statements = rewrite(statements)
        except SasParseError as error:
            raise ConversionError(
                error.message,
                line=error.line,
                statement=error.statement,
            ) from error
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
        if version_warning:
            warnings.append(version_warning)
        index = 0
        sql_count = 0
        freq_count = 0
        means_count = 0
        transpose_count = 0

        while index < len(statements):
            statement = statements[index]
            lower = statement.text.lower()
            try:
                if lower.startswith(SETOPTS_MARKER + " "):
                    payload = json.loads(statement.text[len(SETOPTS_MARKER) + 1 :])
                    frame = _python_name(payload["name"])
                    lines = [f"{frame} = {_python_name(_dataset_name(payload['source']))}"]
                    if payload["where"]:
                        try:
                            condition = _expression(payload["where"])
                        except ConversionError as error:
                            raise ConversionError(
                                error.message,
                                line=statement.line,
                                statement=statement.text,
                            ) from error
                        lines.append(f"{frame} = {frame}.filter(F.expr({condition!r}))")
                    first, last = payload["firstobs"], payload["obs"]
                    if first is not None or last is not None:
                        number = "__sas2pyspark_obs"
                        bounds = []
                        if first is not None:
                            bounds.append(f"(F.col({number!r}) >= {first})")
                        if last is not None:
                            bounds.append(f"(F.col({number!r}) <= {last})")
                        lines.append(
                            f"{frame} = {frame}.withColumn({number!r}, F.expr("
                            "'row_number() over (order by monotonically_increasing_id())'))"
                            f".filter({' & '.join(bounds)}).drop({number!r})"
                        )
                    if payload["keep"]:
                        lines.append(f"{frame} = {frame}.select(*{payload['keep']!r})")
                    if payload["drop"]:
                        lines.append(f"{frame} = {frame}.drop(*{payload['drop']!r})")
                    for old, new in payload["rename"]:
                        lines.append(f"{frame} = {frame}.withColumnRenamed({old!r}, {new!r})")
                    output.extend([*lines, ""])
                    index += 1
                elif lower.startswith(DATALINES_MARKER + " "):
                    payload = json.loads(statement.text[len(DATALINES_MARKER) + 1 :])
                    name = _dataset_name(payload["name"])
                    schema = ", ".join(
                        f"{column} {'string' if kind == 'char' else 'double'}"
                        for column, kind in payload["columns"]
                    )
                    output.extend(
                        [
                            f"{_python_name(name)} = spark.createDataFrame("
                            f"{[tuple(row) for row in payload['rows']]!r}, {schema!r})",
                            f"{_python_name(name)}.createOrReplaceTempView({_sql_name(name)!r})",
                            "",
                        ]
                    )
                    index += 1
                elif lower.startswith(SYMPUT_MARKER + " "):
                    _, variable, derived = statement.text.split()
                    frame = _python_name(derived)
                    output.extend(
                        [
                            f"_symput_rows = {frame}.select({VALUE_COLUMN!r}).tail(1)",
                            "spark.sql("
                            f"{'DECLARE OR REPLACE VARIABLE sas_macro_' + variable + ' DEFAULT :v'!r}, "
                            "args={'v': _symput_rows[0][0] if _symput_rows else None})",
                            "",
                        ]
                    )
                    index += 1
                elif lower.startswith("data "):
                    block, index, block_warnings = self._convert_data_step(
                        statements, index
                    )
                    output.extend(block)
                    warnings.extend(block_warnings)
                elif lower == "proc sort" or lower.startswith("proc sort "):
                    block, index, block_warnings = self._convert_sort(statements, index)
                    output.extend(block)
                    warnings.extend(block_warnings)
                elif lower == "proc sql" or lower == "proc sql noprint":
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
                elif lower == "proc univariate" or lower.startswith("proc univariate "):
                    options = statement.text.split()[2:]
                    if any(not item.lower().startswith("data=") for item in options):
                        raise ConversionError(
                            "PROC UNIVARIATE supports only the DATA= option."
                        )
                    statements = list(statements)
                    statements[index] = _Statement(
                        "proc means "
                        + " ".join(options)
                        + " n nmiss mean std min median max",
                        statement.line,
                    )
                    block, index, means_count = self._convert_means(
                        statements, index, means_count
                    )
                    output.extend(block)
                    warnings.append(
                        "PROC UNIVARIATE is approximated with N, NMISS, MEAN, STD, "
                        "MIN, MEDIAN and MAX; distribution tests, quantile tables "
                        "and plots are not generated."
                    )
                elif lower == "proc transpose" or lower.startswith("proc transpose "):
                    block, index, transpose_count = self._convert_transpose(
                        statements, index, transpose_count
                    )
                    output.extend(block)
                elif lower == "proc format" or lower.startswith("proc format "):
                    block, index = self._convert_format(statements, index)
                    output.extend(block)
                elif lower.startswith("libname "):
                    libref = self._parse_libname(statement.text)
                    output.extend([f"# LIBNAME {libref}: library prefixes are dropped; datasets map to Spark views.", ""])
                    warnings.append(
                        f"LIBNAME {libref} is not materialized; {libref}.table "
                        "references resolve to the table name only."
                    )
                    index += 1
                elif lower == "proc append" or lower.startswith("proc append "):
                    block, index, block_warnings = self._convert_append(statements, index)
                    output.extend(block)
                    warnings.extend(block_warnings)
                elif lower == "proc datasets" or lower.startswith("proc datasets "):
                    block, index = self._convert_datasets(statements, index)
                    output.extend(block)
                elif lower == "proc import" or lower.startswith("proc import "):
                    block, index = self._convert_import(statements, index)
                    output.extend(block)
                elif lower == "proc export" or lower.startswith("proc export "):
                    block, index = self._convert_export(statements, index)
                    output.extend(block)
                elif lower.split(None, 1)[0] in {"title", "footnote", "ods"} or re.match(
                    r"(title|footnote)\d+(\s|$)", lower
                ):
                    output.extend([f"# Ignored report statement: {statement.text[:70]}", ""])
                    if not any("TITLE/FOOTNOTE/ODS" in item for item in warnings):
                        warnings.append(
                            "TITLE/FOOTNOTE/ODS statements are ignored; no report "
                            "formatting is generated."
                        )
                    index += 1
                elif lower in {"proc print", "proc contents", "proc copy", "proc compare"} or lower.startswith(
                    ("proc print ", "proc contents ", "proc copy ", "proc compare ")
                ):
                    block, index, block_warnings = self._convert_report_proc(statements, index)
                    output.extend(block)
                    warnings.extend(block_warnings)
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

        return ConversionResult(
            "\n".join(output).rstrip() + "\n",
            tuple(warnings),
            source_version=selected_version,
            version_source=version_source,
        )

    def _convert_data_step(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int, list[str]]:
        target_statement = statements[start].text
        match = re.fullmatch(r"data\s+((?:[\w]+\.)?[\w]+)", target_statement, re.I)
        if not match:
            raise ConversionError(f"Unsupported DATA statement: {target_statement!r}.")
        target = _dataset_name(match.group(1))
        sources: tuple[str, ...] | None = None
        merge_sources: tuple[str, ...] | None = None
        merge_in_vars: tuple[str, ...] = ()
        merge_order_keys: tuple[tuple[OrderKey, ...], ...] = ()
        merge_keys: tuple[str, ...] | None = None
        merge_directions: tuple[bool, ...] | None = None
        where_filters: list[str] = []
        actions: list[_DataAction] = []
        keep: list[str] | None = None
        drop: list[str] = []
        retain_vars: dict[str, tuple[str, str]] = {}
        retain_seen: set[str] = set()
        retain_resets: dict[str, tuple[str, str]] = {}
        arrays: dict[str, tuple[str, ...]] = {}
        by_columns: tuple[str, ...] | None = None
        by_flags_used: set[str] = set()
        display_formats: set[str] = set()
        order_by_keys: tuple[OrderKey, ...] | None = None
        dynamic_array_reads: dict[
            tuple[tuple[str, ...], str, str | None],
            tuple[
                tuple[str, ...],
                str,
                tuple[str, ...],
                tuple[str, int | str, int | str, int] | None,
            ],
        ] = {}
        current_dynamic_loop: tuple[str, int | str, int | str, int] | None = None

        def register_dynamic_array_read(
            elements: tuple[str, ...],
            index_expression: str,
            index_columns: tuple[str, ...],
        ) -> None:
            loop_key = (
                current_dynamic_loop[0].casefold()
                if current_dynamic_loop is not None
                else None
            )
            dynamic_array_reads.setdefault(
                (
                    tuple(element.casefold() for element in elements),
                    index_expression.casefold(),
                    loop_key,
                ),
                (
                    elements,
                    index_expression,
                    index_columns,
                    current_dynamic_loop,
                ),
            )

        def data_expression(expression: str) -> str:
            expanded = _expand_array_references(
                expression,
                arrays,
                allow_dynamic_read=True,
                on_dynamic_read=register_dynamic_array_read,
            )
            for in_var in merge_in_vars:
                if in_var:
                    expanded = _map_unquoted(
                        expanded,
                        lambda part, name=in_var: re.sub(
                            rf"\b{re.escape(name)}\s*=\s*0\b",
                            f"(NOT {name})",
                            re.sub(
                                rf"\b{re.escape(name)}\s*=\s*1\b",
                                name,
                                part,
                                flags=re.IGNORECASE,
                            ),
                            flags=re.IGNORECASE,
                        ),
                    )

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

        def dynamic_array_target(
            column: str,
        ) -> tuple[tuple[str, ...], str, tuple[str, ...], str | None] | None:
            reference = _ARRAY_REFERENCE.fullmatch(column.strip())
            if reference is None:
                return None
            array_name, index_text = reference.groups()
            array_key = next(
                (name for name in arrays if name.casefold() == array_name.casefold()),
                None,
            )
            if array_key is None:
                raise ConversionError(
                    f"SAS array reference {array_name!r} requires a preceding "
                    "supported ARRAY declaration."
                )
            if index_text.strip().isdigit():
                return None
            elements = arrays[array_key]
            index_expression, index_columns = _dynamic_array_index(index_text)
            register_dynamic_array_read(
                elements, index_expression, index_columns
            )
            return (
                elements,
                index_expression,
                index_columns,
                current_dynamic_loop[0] if current_dynamic_loop else None,
            )

        index = start + 1
        output_loop_seen = False

        while index < len(statements) and statements[index].text.lower() != "run":
            source_statement = statements[index]
            statement = source_statement.text.strip()
            lower = statement.lower()
            try:
                parsed_order = parse_order_by(statement)
                if parsed_order is not None:
                    if order_by_keys is not None:
                        raise ConversionError(
                            "A DATA step supports only one ORDER BY statement."
                        )
                    order_by_keys = parsed_order
                    index += 1
                    continue
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
                    pending_names: list[str] = []
                    number_pattern = (
                        r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
                    )
                    for token in statement[7:].split():
                        if re.fullmatch(number_pattern, token):
                            if not pending_names:
                                raise ConversionError(
                                    "RETAIN initial value must follow a variable name."
                                )
                            for name in pending_names:
                                if name.lower() in retain_vars:
                                    raise ConversionError(
                                        f"RETAIN variable {name!r} is declared more than once."
                                    )
                                retain_vars[name.lower()] = (name, token)
                            pending_names = []
                        elif re.fullmatch(r"[A-Za-z_]\w*", token):
                            pending_names.append(token)
                        else:
                            raise ConversionError(
                                "RETAIN supports variable names with numeric "
                                "literal initial values, such as RETAIN a b 0 c 10;"
                            )
                    if pending_names or not statement[7:].strip():
                        raise ConversionError(
                            "Every RETAIN variable needs a numeric literal initial "
                            "value; character and uninitialized RETAIN are unsupported."
                        )
                elif lower.startswith("call missing"):
                    actions.append(
                        _DataAction(
                            kind="missing",
                            expression="",
                            columns=_call_missing_names(statement),
                        )
                    )
                elif lower.startswith("set "):
                    if sources is not None or merge_sources is not None:
                        raise ConversionError(
                            "A DATA step must use either one SET statement or one "
                            "supported MERGE statement, not both."
                        )
                    input_names = statement[4:].split()
                    if not input_names:
                        raise ConversionError("SET requires at least one input dataset.")
                    sources = tuple(_dataset_name(name) for name in input_names)
                    if len(set(sources)) != len(sources):
                        raise ConversionError("SET input datasets must have distinct names.")
                elif lower.startswith("merge "):
                    if sources is not None or merge_sources is not None:
                        raise ConversionError(
                            "A DATA step must use either one SET statement or one "
                            "supported MERGE statement, not both."
                        )
                    if actions or where_filters:
                        raise ConversionError(
                            "MERGE and BY must appear before DATA-step processing "
                            "statements in the supported subset."
                        )
                    (
                        merge_sources,
                        merge_in_vars,
                        merge_order_keys,
                    ) = _parse_merge_inputs(statement[6:])
                    if len(merge_sources) < 2:
                        raise ConversionError(
                            "MERGE requires at least two input datasets."
                        )
                    if len(set(merge_sources)) != len(merge_sources):
                        raise ConversionError(
                            "MERGE input datasets must have distinct names."
                        )
                    named_flags = [flag.lower() for flag in merge_in_vars if flag]
                    if len(set(named_flags)) != len(named_flags):
                        raise ConversionError("MERGE IN= variable names must be distinct.")
                elif lower.startswith("by "):
                    tokens = statement[3:].split()
                    keys: list[tuple[str, bool]] = []
                    descending = False
                    direction_pending = False
                    for token in tokens:
                        if token.lower() in {"ascending", "descending"}:
                            if direction_pending:
                                raise ConversionError(
                                    "BY order direction must be followed by one column."
                                )
                            descending = token.lower() == "descending"
                            direction_pending = True
                            continue
                        if not re.fullmatch(r"[A-Za-z_]\w*", token):
                            raise ConversionError("BY requires simple column names.")
                        keys.append((token, not descending))
                        descending = False
                        direction_pending = False
                    if direction_pending:
                        raise ConversionError(
                            "BY order direction must be followed by one column."
                        )
                    if not keys:
                        raise ConversionError("BY requires simple column names.")
                    key_names = tuple(key for key, _ in keys)
                    key_directions = tuple(ascending for _, ascending in keys)
                    if len({key.lower() for key in key_names}) != len(keys):
                        raise ConversionError("BY column names must be distinct.")
                    if merge_sources is not None:
                        if merge_keys is not None:
                            raise ConversionError(
                                "MERGE supports only one BY statement."
                            )
                        merge_keys = key_names
                        merge_directions = key_directions
                        actions.append(
                            _DataAction(
                                kind="merge",
                                expression="",
                                merge_sources=merge_sources,
                                merge_keys=merge_keys,
                                merge_directions=merge_directions,
                                merge_in_vars=merge_in_vars,
                                merge_order_keys=merge_order_keys,
                            )
                        )
                    elif sources is not None and by_columns is None:
                        by_columns = key_names
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
                            "Only DO index = start TO stop [BY step] loops with "
                            "integer literals or compile-time integer expressions "
                            "are supported."
                        )
                    loop_variable = loop_match.group(1)
                    loop_start = parse_constant_integer(loop_match.group(2))
                    loop_stop = parse_constant_integer(loop_match.group(3))
                    loop_step = (
                        parse_constant_integer(loop_match.group(4))
                        if loop_match.group(4)
                        else 1
                    )
                    if loop_step is None:
                        raise ConversionError(
                            "DO loop BY step must be an integer literal or "
                            "compile-time integer expression."
                        )
                    assert loop_step is not None
                    if loop_step == 0:
                        raise ConversionError("DO loop BY step cannot be zero.")
                    bound_columns: list[str] = []
                    if loop_start is None:
                        raw_start, start_columns = _dynamic_loop_bound(
                            loop_match.group(2)
                        )
                        loop_start = data_expression(raw_start)
                        bound_columns.extend(start_columns)
                    if loop_stop is None:
                        raw_stop, stop_columns = _dynamic_loop_bound(
                            loop_match.group(3)
                        )
                        loop_stop = data_expression(raw_stop)
                        bound_columns.extend(stop_columns)
                    bound_columns = list(
                        dict.fromkeys(column.casefold() for column in bound_columns)
                    )
                    body_assignments: list[tuple[str, str]] = []
                    loop_delete_conditions: list[str] = []
                    loop_operations: list[tuple[str, int | str]] = []
                    loop_array_write = False
                    loop_conditional_assignment = False
                    output_count = 0
                    body_index = index + 1
                    current_dynamic_loop = (
                        (loop_variable, loop_start, loop_stop, loop_step)
                        if bound_columns
                        else None
                    )
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
                        elif (
                            delete_match := re.fullmatch(
                                r"if\s+coalesce\s*\(\s*not\s*\((.*)\)\s*,\s*true\s*\)",
                                body_text,
                                re.IGNORECASE | re.DOTALL,
                            )
                        ) is not None:
                            condition = data_expression(delete_match.group(1))
                            loop_delete_conditions.append(condition)
                            loop_operations.append(("delete", condition))
                        elif body_text.lower().startswith("if "):
                            loop_conditional_assignment = True
                            conditional = _IF_THEN.fullmatch(body_text)
                            if conditional is None or conditional.group(2) is None:
                                raise ConversionError(
                                    "Runtime-bound DO loops support only IF "
                                    "conditions with simple assignment branches."
                                )
                            chain_index = body_index
                            chain_updates: list[tuple[str, str, str]] = []
                            branch_columns: list[str] = []
                            current = conditional
                            while True:
                                branch_assignment = _DATA_ASSIGNMENT.fullmatch(
                                    (current.group(2) or "").strip()
                                )
                                if branch_assignment is None:
                                    raise ConversionError(
                                        "Runtime-bound DO loop IF branches must be "
                                        "simple assignments."
                                    )
                                branch_condition = data_expression(current.group(1))
                                branch_expression = data_expression(
                                    branch_assignment.group(2)
                                )
                                dynamic_target = dynamic_array_target(
                                    branch_assignment.group(1)
                                )
                                if dynamic_target is not None:
                                    if not bound_columns:
                                        raise ConversionError(
                                            "Dynamic array assignment targets inside "
                                            "DO loops require runtime bounds."
                                        )
                                    elements, index_expression, _, _ = dynamic_target
                                    loop_array_write = True
                                    for element_index, element in enumerate(
                                        elements, start=1
                                    ):
                                        branch_columns.append(element)
                                        chain_updates.append(
                                            (
                                                f"({branch_condition}) AND "
                                                f"(({index_expression}) = "
                                                f"{element_index})",
                                                element,
                                                branch_expression,
                                            )
                                        )
                                else:
                                    branch_column = data_column(
                                        branch_assignment.group(1)
                                    )
                                    if branch_column.casefold() == loop_variable.casefold():
                                        raise ConversionError(
                                            "DO loop body cannot reassign its index variable."
                                        )
                                    branch_columns.append(branch_column)
                                    chain_updates.append(
                                        (
                                            branch_condition,
                                            branch_column,
                                            branch_expression,
                                        )
                                    )
                                if chain_index + 1 >= len(statements):
                                    break
                                else_match = re.fullmatch(
                                    r"else\s+(.+)",
                                    statements[chain_index + 1].text.strip(),
                                    re.IGNORECASE | re.DOTALL,
                                )
                                if else_match is None:
                                    if statements[
                                        chain_index + 1
                                    ].text.strip().lower().startswith("else"):
                                        raise ConversionError(
                                            "Runtime-bound DO loop ELSE branches "
                                            "must be simple assignments."
                                        )
                                    break
                                chain_index += 1
                                else_body = else_match.group(1).strip()
                                next_if = _IF_THEN.fullmatch(else_body)
                                if next_if is not None:
                                    if next_if.group(2) is None:
                                        raise ConversionError(
                                            "Runtime-bound DO loop ELSE IF "
                                            "branches must be simple assignments."
                                        )
                                    current = next_if
                                    continue
                                else_assignment = _DATA_ASSIGNMENT.fullmatch(else_body)
                                if else_assignment is None:
                                    raise ConversionError(
                                        "Runtime-bound DO loop ELSE branches must "
                                        "be simple assignments."
                                    )
                                else_expression = data_expression(
                                    else_assignment.group(2)
                                )
                                prior_conditions = " OR ".join(
                                    f"coalesce(({condition}), false)"
                                    for condition, _, _ in chain_updates
                                    if condition
                                )
                                else_condition = (
                                    f"NOT ({prior_conditions})"
                                    if prior_conditions
                                    else "true"
                                )
                                dynamic_target = dynamic_array_target(
                                    else_assignment.group(1)
                                )
                                if dynamic_target is not None:
                                    if not bound_columns:
                                        raise ConversionError(
                                            "Dynamic array assignment targets inside "
                                            "DO loops require runtime bounds."
                                        )
                                    elements, index_expression, _, _ = dynamic_target
                                    loop_array_write = True
                                    for element_index, element in enumerate(
                                        elements, start=1
                                    ):
                                        branch_columns.append(element)
                                        chain_updates.append(
                                            (
                                                f"({else_condition}) AND "
                                                f"(({index_expression}) = "
                                                f"{element_index})",
                                                element,
                                                else_expression,
                                            )
                                        )
                                else:
                                    else_column = data_column(
                                        else_assignment.group(1)
                                    )
                                    if else_column.casefold() == loop_variable.casefold():
                                        raise ConversionError(
                                            "DO loop body cannot reassign its index variable."
                                        )
                                    branch_columns.append(else_column)
                                    chain_updates.append(
                                        (
                                            else_condition,
                                            else_column,
                                            else_expression,
                                        )
                                    )
                                break
                            body_index = chain_index
                            target_columns = list(
                                dict.fromkeys(
                                    [
                                        *branch_columns,
                                    ]
                                )
                            )
                            for target_column in target_columns:
                                case_branches = " ".join(
                                    "WHEN ({condition}) THEN ({value})".format(
                                        condition=condition,
                                        value=(
                                            expression
                                            if branch_column.casefold() == target_column.casefold()
                                            else target_column
                                        ),
                                    )
                                    for condition, branch_column, expression in chain_updates
                                )
                                body_assignments.append(
                                    (
                                        target_column,
                                        f"CASE {case_branches} ELSE "
                                        f"({target_column}) END",
                                    )
                                )
                                loop_operations.append(
                                    ("assignment", len(body_assignments) - 1)
                                )
                        else:
                            body_assignment = _DATA_ASSIGNMENT.fullmatch(body_text)
                            if not body_assignment:
                                raise ConversionError(
                                    "DO loop bodies support only simple assignments "
                                    "and an optional final OUTPUT."
                                )
                            expression = data_expression(body_assignment.group(2))
                            dynamic_target = dynamic_array_target(
                                body_assignment.group(1)
                            )
                            if dynamic_target is not None:
                                if not bound_columns:
                                    raise ConversionError(
                                        "Dynamic array assignment targets inside "
                                        "DO loops require runtime bounds."
                                    )
                                elements, index_expression, _, _ = dynamic_target
                                loop_array_write = True
                                for element_index, element in enumerate(
                                    elements, start=1
                                ):
                                    body_assignments.append(
                                        (
                                            element,
                                            f"CASE WHEN ({index_expression}) = "
                                            f"{element_index} THEN ({expression}) "
                                            f"ELSE {element} END",
                                        )
                                    )
                                    loop_operations.append(
                                        ("assignment", len(body_assignments) - 1)
                                    )
                            else:
                                column = data_column(body_assignment.group(1))
                                if column.lower() == loop_variable.lower():
                                    raise ConversionError(
                                        "DO loop body cannot reassign its index variable."
                                    )
                                body_assignments.append((column, expression))
                                loop_operations.append(
                                    ("assignment", len(body_assignments) - 1)
                                )
                        body_index += 1
                    current_dynamic_loop = None
                    if body_index >= len(statements):
                        raise ConversionError("DO loop is missing END.")
                    if output_count > 1:
                        raise ConversionError(
                            "Supported DO loops allow at most one final OUTPUT."
                        )
                    if output_count and loop_array_write:
                        raise ConversionError(
                            "Dynamic array writes are not supported in "
                            "output-producing DO loops."
                        )
                    if output_count and loop_conditional_assignment:
                        raise ConversionError(
                            "Conditional assignments are supported only in "
                            "runtime-bounded non-OUTPUT DO loops."
                        )
                    if output_count and loop_delete_conditions:
                        raise ConversionError(
                            "Conditional DELETE is supported only in "
                            "runtime-bounded non-OUTPUT DO loops."
                        )
                    if output_count and any(
                        re.search(
                            rf"\b{re.escape(column)}\b",
                            expression,
                            re.IGNORECASE,
                        )
                        for column, expression in body_assignments
                    ):
                        raise ConversionError(
                            "DO loop assignments cannot depend on their own "
                            "previous-iteration value."
                        )
                    if output_count == 0 and not bound_columns:
                        raise ConversionError(
                            "DO loops without OUTPUT require compile-time bounds."
                        )
                    if output_count == 0 and not body_assignments and not loop_delete_conditions:
                        raise ConversionError(
                            "Runtime-bound DO loops without OUTPUT require an "
                            "assignment or conditional DELETE."
                        )
                    if output_count == 0 and any(
                        column.casefold() in bound_columns
                        for column, _ in body_assignments
                    ):
                        raise ConversionError(
                            "Runtime-bound DO loop bounds cannot be assigned in "
                            "the loop body."
                        )
                    if actions and actions[-1].kind in {"do_output", "do_runtime"}:
                        raise ConversionError(
                            "Only one output-generating DO loop is supported per DATA step."
                        )
                    actions.append(
                        _DataAction(
                            kind="do_output" if output_count else "do_runtime",
                            expression="",
                            column=loop_variable,
                            loop_start=loop_start,
                            loop_stop=loop_stop,
                            loop_step=loop_step,
                            loop_bound_columns=tuple(bound_columns),
                            loop_assignments=tuple(body_assignments),
                            loop_delete_conditions=tuple(loop_delete_conditions),
                            loop_operations=tuple(loop_operations),
                        )
                    )
                    output_loop_seen = output_count == 1
                    index = body_index
                elif lower == "output":
                    raise ConversionError(
                        "OUTPUT is supported only as the final statement inside a "
                        "supported indexed DO loop."
                    )
                elif lower.startswith("if "):
                    conditional = _IF_THEN.fullmatch(statement)
                    if (
                        conditional is not None
                        and conditional.group(2) is not None
                        and conditional.group(2).strip().casefold().startswith(
                            "call missing"
                        )
                    ):
                        actions.append(
                            _DataAction(
                                kind="missing",
                                expression="",
                                columns=_call_missing_names(conditional.group(2)),
                                condition=data_expression(conditional.group(1)),
                            )
                        )
                    elif conditional:
                        else_match = (
                            re.fullmatch(
                                r"else\s+(.+)",
                                statements[index + 1].text,
                                re.IGNORECASE | re.DOTALL,
                            )
                            if index + 1 < len(statements)
                            else None
                        )
                        if (
                            else_match is None
                            and conditional.group(2)
                            and re.fullmatch(
                                r"first\s*\.\s*[A-Za-z_]\w*",
                                conditional.group(1).strip(),
                                re.IGNORECASE,
                            )
                        ):
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
                            if not retain_vars:
                                raise ConversionError(
                                    "FIRST.<BY key> RETAIN resets require a preceding "
                                    "RETAIN declaration."
                                )
                            reset_column = data_column(reset_assignment.group(1))
                            reset_name = reset_column.lower()
                            if reset_name not in retain_vars:
                                raise ConversionError(
                                    "FIRST.<BY key> reset must assign a retained variable."
                                )
                            if reset_name in retain_seen:
                                raise ConversionError(
                                    "A RETAIN reset must precede the accumulator assignment."
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
                            if reset_name in retain_resets:
                                raise ConversionError(
                                    "Only one FIRST.<BY key> RETAIN reset is supported "
                                    "per retained variable."
                                )
                            data_expression(conditional.group(1))
                            retain_resets[reset_name] = (reset_key, reset_value)
                        else:
                            chain_branches: list[tuple[str, str]] = []
                            chain_column: str | None = None
                            chain_otherwise: str | None = None
                            chain_has_else = False
                            chain_index = index
                            current = conditional
                            while True:
                                if not current.group(2):
                                    raise ConversionError(
                                        "IF THEN branch must be a simple assignment."
                                    )
                                branch_assignment = _DATA_ASSIGNMENT.fullmatch(
                                    current.group(2).strip()
                                )
                                if not branch_assignment:
                                    raise ConversionError(
                                        "IF THEN and ELSE branches must each be a simple assignment."
                                        if chain_has_else or chain_index + 1 < len(statements)
                                        and re.match(
                                            r"else\b",
                                            statements[chain_index + 1].text,
                                            re.IGNORECASE,
                                        )
                                        else "IF THEN branch must be a simple assignment."
                                    )
                                branch_column = data_column(branch_assignment.group(1))
                                if (
                                    chain_column is not None
                                    and chain_column.lower() != branch_column.lower()
                                ):
                                    raise ConversionError(
                                        "IF THEN and ELSE branches must assign the same column."
                                    )
                                chain_column = branch_column
                                chain_branches.append(
                                    (
                                        data_expression(current.group(1)),
                                        data_expression(branch_assignment.group(2)),
                                    )
                                )
                                next_else = (
                                    re.fullmatch(
                                        r"else\s+(.+)",
                                        statements[chain_index + 1].text,
                                        re.IGNORECASE | re.DOTALL,
                                    )
                                    if chain_index + 1 < len(statements)
                                    else None
                                )
                                if next_else is None:
                                    break
                                chain_index += 1
                                chain_has_else = True
                                else_body = next_else.group(1).strip()
                                else_if = _IF_THEN.fullmatch(else_body)
                                if else_if is not None:
                                    current = else_if
                                    continue
                                final_assignment = _DATA_ASSIGNMENT.fullmatch(else_body)
                                if not final_assignment:
                                    raise ConversionError(
                                        "IF THEN and ELSE branches must each be a simple assignment."
                                    )
                                final_column = data_column(final_assignment.group(1))
                                if chain_column.lower() != final_column.lower():
                                    raise ConversionError(
                                        "IF THEN and ELSE branches must assign the same column."
                                    )
                                chain_otherwise = data_expression(final_assignment.group(2))
                                break
                            assert chain_column is not None
                            if len(chain_branches) == 1 and chain_otherwise is not None:
                                actions.append(
                                    _DataAction(
                                        kind="conditional_assign",
                                        expression=chain_branches[0][1],
                                        column=chain_column,
                                        condition=chain_branches[0][0],
                                        otherwise=chain_otherwise,
                                    )
                                )
                            elif chain_otherwise is not None:
                                actions.append(
                                    _DataAction(
                                        kind="select_assign",
                                        expression=chain_otherwise,
                                        column=chain_column,
                                        branches=tuple(chain_branches),
                                    )
                                )
                            else:
                                actions.append(
                                    _DataAction(
                                        kind="conditional_keep",
                                        expression="",
                                        column=chain_column,
                                        branches=tuple(chain_branches),
                                    )
                                )
                            index = chain_index
                    elif re.match(r"^if\b", statement, re.IGNORECASE):
                        actions.append(_DataAction("filter", data_expression(statement[2:].strip())))
                    else:
                        raise ConversionError(
                            f"Unsupported DATA step IF statement: {statement!r}."
                        )
                elif lower.startswith("else"):
                    raise ConversionError("ELSE must immediately follow a supported IF THEN assignment.")
                elif lower.startswith("format "):
                    format_match = re.fullmatch(
                        r"format\s+((?:[A-Za-z_]\w*\s+)+)(\$?[A-Za-z_]\w*\.\d*)",
                        statement,
                        re.IGNORECASE,
                    )
                    if format_match is None:
                        raise ConversionError(
                            "FORMAT supports one or more variables followed by a "
                            "single format, such as FORMAT birth_date date9.;"
                        )
                    try:
                        if not has_format(format_match.group(2)):
                            render_format("x", format_match.group(2))
                    except DateFunctionError as error:
                        raise ConversionError(str(error)) from error
                    display_formats.update(format_match.group(1).split())
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
                    dynamic_target = dynamic_array_target(assignment.group(1))
                    if dynamic_target is not None:
                        (
                            elements,
                            index_expression,
                            index_columns,
                            loop_variable,
                        ) = dynamic_target
                        actions.append(
                            _DataAction(
                                kind="array_assign",
                                expression=data_expression(assignment.group(2)),
                                columns=elements,
                                array_index=index_expression,
                                array_index_columns=index_columns,
                                array_loop_variable=loop_variable,
                            )
                        )
                    else:
                        column = data_column(assignment.group(1))
                        column_key = column.lower()
                        if column_key in retain_vars:
                            retain_column, retain_initial = retain_vars[column_key]
                            reset_info = retain_resets.get(column_key)
                            retain_reset_key = reset_info[0] if reset_info else None
                            retain_reset_value = reset_info[1] if reset_info else None
                            if column_key in retain_seen:
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
                            retain_seen.add(column_key)
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
            except OrderByError as error:
                raise ConversionError(
                    str(error),
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1

        if index >= len(statements):
            raise ConversionError(f"DATA step for {target!r} is missing RUN.")
        if sources is None and (merge_sources is None or merge_keys is None):
            raise ConversionError(
                "DATA steps require one SET statement or a MERGE followed by BY."
            )
        if order_by_keys is not None and merge_sources is not None:
            raise ConversionError(
                "DATA-step ORDER BY is not supported with MERGE; each MERGE input "
                "requires its own stable row-order key."
            )
        if retain_vars:
            missing = [name for key, (name, _) in retain_vars.items() if key not in retain_seen]
            if missing:
                raise ConversionError(
                    "RETAIN variable must have one supported accumulator assignment: "
                    + ", ".join(missing)
                    + "."
                )
            if (
                sources is None
                or len(sources) != 1
                or merge_sources is not None
                or where_filters
                or any(action.kind not in {"retain_sum", "assign"} for action in actions)
            ):
                raise ConversionError(
                    "RETAIN currently supports one SET input with accumulator and "
                    "simple assignment statements, without other DATA-step processing."
                )
            if by_columns is not None and len(retain_resets) != len(retain_vars):
                raise ConversionError(
                    "BY-group RETAIN requires an IF FIRST.<BY key> reset before "
                    "each accumulator assignment."
                )
        for _, _, index_columns, loop_context in dynamic_array_reads.values():
            for index_column in index_columns:
                if (
                    loop_context is not None
                    and index_column.casefold() == loop_context[0].casefold()
                ):
                    continue
                if any(
                    (
                        action.column is not None
                        and action.column.casefold() == index_column.casefold()
                    )
                    or (
                        action.kind in {"missing", "array_assign"}
                        and any(
                            column.casefold() == index_column.casefold()
                            for column in action.columns
                        )
                    )
                    or any(
                        column.casefold() == index_column.casefold()
                        for column, _ in action.loop_assignments
                    )
                    for action in actions
                ):
                    raise ConversionError(
                        f"Dynamic array index variable {index_column!r} cannot be "
                        "assigned in the same DATA step."
                    )

        dynamic_array_access_ids = {
            (
                tuple(element.casefold() for element in elements),
                index_expression.casefold(),
                loop_context[0].casefold() if loop_context is not None else None,
            ): access_index
                for access_index, (
                    elements,
                    index_expression,
                    _,
                    loop_context,
                ) in enumerate(
                dynamic_array_reads.values()
            )
        }
        lines: list[str] = []
        if sources is not None:
            lines.append(f"{_python_name(target)} = {_python_name(sources[0])}")
            for source in sources[1:]:
                lines.append(
                    f"{_python_name(target)} = {_python_name(target)}.unionByName("
                    f"{_python_name(source)}, allowMissingColumns=True)"
                )
        for read_index, (
            elements,
            index_expression,
            index_columns,
            loop_context,
        ) in enumerate(dynamic_array_reads.values()):
            frame = _python_name(target)
            element_matches = f"__sas2pyspark_array_elements_{read_index}"
            index_matches = f"__sas2pyspark_array_index_matches_{read_index}"
            resolved_elements = f"__sas2pyspark_array_columns_{read_index}"
            resolved_indexes = f"__sas2pyspark_array_index_columns_{read_index}"
            raw_index = f"__sas2pyspark_array_index_value_{read_index}"
            integer_index = f"{raw_index}_integer"
            input_index_columns = tuple(
                name
                for name in index_columns
                if loop_context is None
                or name.casefold() != loop_context[0].casefold()
            )
            lines.extend(
                [
                    f"{element_matches} = [[column for column in {frame}.columns "
                    f"if column.casefold() == name.casefold()] for name in {list(elements)!r}]",
                    f"if any(len(matches) != 1 for matches in {element_matches}):",
                    "    raise ValueError('Dynamic SAS array elements must exist exactly once in the input DataFrame.')",
                    f"{resolved_elements} = [matches[0] for matches in {element_matches}]",
                    f"{index_matches} = [[column for column in {frame}.columns "
                    f"if column.casefold() == name.casefold()] "
                    f"for name in {list(input_index_columns)!r}]",
                    f"if any(len(matches) != 1 for matches in {index_matches}):",
                    "    raise ValueError('Dynamic SAS array index columns must exist exactly once in the input DataFrame.')",
                    f"{resolved_indexes} = [matches[0] for matches in {index_matches}]",
                    "from pyspark.sql.types import NumericType as __sas2pyspark_numeric_type",
                    f"if any(not isinstance({frame}.schema[column].dataType, "
                    f"__sas2pyspark_numeric_type) for column in {resolved_indexes}):",
                    "    raise TypeError('Dynamic SAS array index variables must be numeric.')",
                    f"if any({frame}.schema[column].dataType != "
                    f"{frame}.schema[{resolved_elements}[0]].dataType "
                    f"for column in {resolved_elements}[1:]):",
                    "    raise TypeError('Dynamic SAS array elements must have one Spark data type.')",
                ]
            )
            if loop_context is None:
                lines.extend(
                    [
                        f"{raw_index} = F.expr({index_expression!r})",
                        f"{integer_index} = {raw_index}.cast('int')",
                        f"if {frame}.filter({raw_index}.isNull() | "
                        f"{integer_index}.isNull() | ({raw_index} != {integer_index}) | "
                        f"({integer_index} < 1) | ({integer_index} > {len(elements)}))"
                        ".limit(1).count():",
                        "    raise ValueError('Dynamic SAS array index is missing, non-integer, or outside the declared bounds.')",
                    ]
                )
            else:
                loop_variable, loop_start, loop_stop, loop_step = loop_context
                loop_alias = f"__sas2pyspark_array_loop_{read_index}"
                loop_state_alias = f"__sas2pyspark_array_valid_{read_index}"
                start_sql = (
                    str(loop_start)
                    if isinstance(loop_start, int)
                    else loop_start
                )
                stop_sql = (
                    str(loop_stop)
                    if isinstance(loop_stop, int)
                    else loop_stop
                )
                if loop_step > 0:
                    loop_sequence = (
                        f"CASE WHEN ({start_sql}) <= ({stop_sql}) "
                        f"THEN sequence(CAST(({start_sql}) AS BIGINT), "
                        f"CAST(({stop_sql}) AS BIGINT), {loop_step}) "
                        "ELSE CAST(array() AS ARRAY<BIGINT>) END"
                    )
                else:
                    loop_sequence = (
                        f"CASE WHEN ({start_sql}) >= ({stop_sql}) "
                        f"THEN sequence(CAST(({start_sql}) AS BIGINT), "
                        f"CAST(({stop_sql}) AS BIGINT), {loop_step}) "
                        "ELSE CAST(array() AS ARRAY<BIGINT>) END"
                    )
                loop_index_expression = re.sub(
                    rf"(?<![\w.]){re.escape(loop_variable)}(?![\w.(])",
                    loop_alias,
                    index_expression,
                    flags=re.IGNORECASE,
                )
                raw_sql = f"({loop_index_expression})"
                integer_sql = f"CAST({raw_sql} AS INT)"
                start_integer_sql = f"CAST(({start_sql}) AS BIGINT)"
                stop_integer_sql = f"CAST(({stop_sql}) AS BIGINT)"
                start_measure_sql = f"CAST(({start_sql}) AS DECIMAL(38, 0))"
                stop_measure_sql = f"CAST(({stop_sql}) AS DECIMAL(38, 0))"
                if loop_step > 0:
                    loop_active = (
                        f"({start_sql}) IS NOT NULL AND ({stop_sql}) IS NOT NULL "
                        f"AND ({start_sql}) = {start_integer_sql} "
                        f"AND ({stop_sql}) = {stop_integer_sql} "
                        f"AND ({start_integer_sql}) <= ({stop_integer_sql}) "
                        f"AND FLOOR(({stop_measure_sql} - {start_measure_sql}) / "
                        f"{loop_step}) + 1 <= 5000"
                    )
                else:
                    loop_active = (
                        f"({start_sql}) IS NOT NULL AND ({stop_sql}) IS NOT NULL "
                        f"AND ({start_sql}) = {start_integer_sql} "
                        f"AND ({stop_sql}) = {stop_integer_sql} "
                        f"AND ({start_integer_sql}) >= ({stop_integer_sql}) "
                        f"AND FLOOR(({start_measure_sql} - {stop_measure_sql}) / "
                        f"{abs(loop_step)}) + 1 <= 5000"
                    )
                loop_sequence = loop_sequence.replace(
                    f"CASE WHEN ",
                    f"CASE WHEN {loop_active} AND ",
                    1,
                )
                invalid_sql = (
                    f"aggregate({loop_sequence}, false, "
                    f"({loop_state_alias}, {loop_alias}) -> {loop_state_alias} OR "
                    f"({raw_sql} IS NULL OR {integer_sql} IS NULL OR "
                    f"{raw_sql} <> {integer_sql} OR {integer_sql} < 1 OR "
                    f"{integer_sql} > {len(elements)}))"
                )
                lines.extend(
                    [
                        f"if {frame}.filter(F.expr({invalid_sql!r})).limit(1).count():",
                        "    raise ValueError('Dynamic SAS array index is missing, non-integer, or outside the declared bounds.')",
                    ]
                )
        for condition in where_filters:
            lines.append(f"{_python_name(target)} = {_python_name(target)}.filter(F.expr({condition!r}))")
        row_number_action = next(
            (
                action
                for action in actions
                if action.kind == "assign"
                and action.column == "__sas2pyspark_n"
                and action.expression.lower().startswith("row_number() over ")
            ),
            None,
        )
        data_step_ir = DataStepIR(
            target=target,
            source_line=statements[start].line,
            actions=tuple(actions),
            order_by_keys=order_by_keys,
            has_retained_state=bool(retain_vars),
            by_flags=frozenset(by_flags_used),
            has_row_number=row_number_action is not None,
        )
        order_sensitive = data_step_ir.requires_deterministic_order
        if order_sensitive and data_step_ir.order_by_keys is None:
            raise ConversionError(
                f"DATA step {data_step_ir.target!r} uses order-sensitive "
                "operations, which require an explicit ORDER BY with unique "
                "keys because Spark does not preserve SAS observation order.",
                line=data_step_ir.source_line,
            )
        if data_step_ir.order_by_keys is not None and order_sensitive:
            order_names = [name for name, _ in data_step_ir.order_by_keys]
            lines.extend(
                [
                    f"__sas2pyspark_order_matches = [[column for column in "
                    f"{_python_name(target)}.columns if column.casefold() == "
                    f"key.casefold()] for key in {order_names!r}]",
                    "if any(len(matches) != 1 for matches in __sas2pyspark_order_matches):",
                    "    raise ValueError('DATA-step ORDER BY columns must exist exactly once in the input DataFrame.')",
                    "__sas2pyspark_order_columns = [matches[0] for matches in __sas2pyspark_order_matches]",
                    f"if {_python_name(target)}.count() != "
                    f"{_python_name(target)}.dropDuplicates("
                    "__sas2pyspark_order_columns).count():",
                    "    raise ValueError('DATA-step ORDER BY keys must be unique for deterministic observation order.')",
                ]
            )
        if row_number_action is not None:
            lines.append(
                f"{_python_name(target)} = {_python_name(target)}.withColumn("
                f"'{row_number_action.column}', F.expr({row_number_action.expression!r}))"
            )
        by_flag_columns: list[str] = []
        if by_columns is not None and by_flags_used:
            order_column = "__sas2pyspark_by_order"
            by_flag_columns = [
                f"__sas2pyspark_{flag}_{column.lower()}"
                for flag in ("first", "last")
                if flag in by_flags_used
                for column in by_columns
            ]
            helper_columns = [
                *([] if order_by_keys is not None else [order_column]),
                *by_flag_columns,
            ]
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
                    *(
                        []
                        if order_by_keys is not None
                        else [
                            f"{_python_name(target)} = {_python_name(target)}.withColumn("
                            f"{order_column!r}, F.monotonically_increasing_id())",
                        ]
                    ),
                ]
            )
            flag_order = (
                ", ".join(
                    f"F.col({name!r}).{'asc' if ascending else 'desc'}()"
                    for name, ascending in order_by_keys
                )
                if order_by_keys is not None
                else f"F.col({order_column!r}).asc()"
            )
            last_flag_order = (
                ", ".join(
                    f"F.col({name!r}).{'desc' if ascending else 'asc'}()"
                    for name, ascending in order_by_keys
                )
                if order_by_keys is not None
                else f"F.col({order_column!r}).desc()"
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
                        f"{flag_order})) == 1)"
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
                        f"{last_flag_order})) == 1)"
                    )
        for action_index, action in enumerate(actions):
            if action is row_number_action:
                continue
            if action.kind == "merge":
                lines.extend(
                    self._render_merge(
                        target,
                        action.merge_sources,
                        action.merge_keys,
                        action.merge_directions,
                        action.merge_in_vars,
                        action.merge_order_keys,
                    )
                )
            elif action.kind == "filter":
                lines.append(
                    f"{_python_name(target)} = {_python_name(target)}.filter(F.expr({action.expression!r}))"
                )
            elif action.kind == "assign" and action.column is not None:
                lines.append(
                    f"{_python_name(target)} = {_python_name(target)}.withColumn({action.column!r}, F.expr({action.expression!r}))"
                )
            elif action.kind == "missing" and action.columns:
                frame = _python_name(target)
                for column_index, column in enumerate(action.columns):
                    matches = (
                        f"__sas2pyspark_missing_matches_{action_index}_{column_index}"
                    )
                    value = (
                        f"__sas2pyspark_missing_value_{action_index}_{column_index}"
                    )
                    lines.extend(
                        [
                            f"{matches} = [name for name in {frame}.columns "
                            f"if name.casefold() == {column!r}.casefold()]",
                            f"if len({matches}) > 1:",
                            "    raise ValueError('CALL MISSING variable is "
                            "ambiguous in the input DataFrame.')",
                            f"{value} = F.lit(None).cast("
                            f"{frame}.schema[{matches}[0]].dataType "
                            f"if {matches} else 'double')",
                        ]
                    )
                    if action.condition is not None:
                        lines.append(
                            f"{value} = F.when(F.expr({action.condition!r}), {value})"
                            f".otherwise(F.col({matches}[0]) if {matches} "
                            f"else F.lit(None).cast('double'))"
                        )
                    lines.append(
                        f"{frame} = {frame}.withColumn("
                        f"{matches}[0] if {matches} else {column!r}, {value})"
                    )
            elif action.kind == "array_assign" and action.array_index is not None:
                frame = _python_name(target)
                access_index = dynamic_array_access_ids[
                    (
                        tuple(element.casefold() for element in action.columns),
                        action.array_index.casefold(),
                        (
                            action.array_loop_variable.casefold()
                            if action.array_loop_variable is not None
                            else None
                        ),
                    )
                ]
                resolved_elements = f"__sas2pyspark_array_columns_{access_index}"
                raw_index = f"__sas2pyspark_array_index_value_{access_index}"
                integer_index = f"{raw_index}_integer"
                value = f"__sas2pyspark_array_assignment_{action_index}"
                lines.extend(
                    [
                        f"if any(column.casefold() == {value!r}.casefold() "
                        f"for column in {frame}.columns):",
                        "    raise ValueError('Dynamic SAS array assignment temporary column conflicts with an input column.')",
                        f"{frame} = {frame}.withColumn({value!r}, "
                        f"F.expr({action.expression!r}))",
                    ]
                )
                for element_index in range(1, len(action.columns) + 1):
                    lines.append(
                        f"{frame} = {frame}.withColumn("
                        f"{resolved_elements}[{element_index - 1}], "
                        f"F.when({integer_index} == {element_index}, "
                        f"F.col({value!r})).otherwise("
                        f"F.col({resolved_elements}[{element_index - 1}])))"
                    )
                lines.append(f"{frame} = {frame}.drop({value!r})")
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
                    if action.retain_partition or order_by_keys is not None
                    else [retain_order, retain_delta]
                )
                window_spec = "__sas2pyspark_window"
                if action.retain_partition:
                    partition_columns = ", ".join(
                        f"F.col({column!r})" for column in action.retain_partition
                    )
                    window_spec += f".partitionBy({partition_columns})"
                if order_by_keys is not None:
                    order_columns = ", ".join(
                        f"F.col({column!r}).{'asc' if ascending else 'desc'}()"
                        for column, ascending in order_by_keys
                    )
                    window_spec += f".orderBy({order_columns})"
                else:
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
                            if action.retain_partition or order_by_keys is not None
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
                action.kind == "conditional_keep"
                and action.column is not None
                and action.branches
            ):
                frame = _python_name(target)
                expression = (
                    f"F.col({action.column!r}) if {action.column!r} in {frame}.columns "
                    "else F.lit(None)"
                )
                expression = f"({expression})"
                for condition, branch_expression in reversed(action.branches):
                    expression = (
                        f"F.when(F.expr({condition!r}), "
                        f"F.expr({branch_expression!r})).otherwise({expression})"
                    )
                lines.append(
                    f"{frame} = {frame}.withColumn({action.column!r}, {expression})"
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
                action.kind in {"do_output", "do_runtime"}
                and action.column is not None
                and action.loop_start is not None
                and action.loop_stop is not None
                and action.loop_step is not None
            ):
                if action.loop_bound_columns:
                    frame = _python_name(target)
                    matches = f"__sas2pyspark_loop_bound_matches_{action_index}"
                    resolved = f"__sas2pyspark_loop_bound_columns_{action_index}"
                    start_value = f"__sas2pyspark_loop_start_{action_index}"
                    stop_value = f"__sas2pyspark_loop_stop_{action_index}"
                    start_integer = f"{start_value}_integer"
                    stop_integer = f"{stop_value}_integer"
                    lines.extend(
                        [
                            f"{matches} = [[column for column in {frame}.columns "
                            f"if column.casefold() == name.casefold()] "
                            f"for name in {list(action.loop_bound_columns)!r}]",
                            f"if any(len(found) != 1 for found in {matches}):",
                            "    raise ValueError('Dynamic DO loop bound columns must exist exactly once in the input DataFrame.')",
                            f"{resolved} = [found[0] for found in {matches}]",
                            "from pyspark.sql.types import NumericType as __sas2pyspark_numeric_type",
                            f"if any(not isinstance({frame}.schema[column].dataType, "
                            f"__sas2pyspark_numeric_type) for column in {resolved}):",
                            "    raise TypeError('Dynamic DO loop bound columns must be numeric.')",
                        ]
                    )
                    start_column = (
                        f"F.lit({action.loop_start})"
                        if isinstance(action.loop_start, int)
                        else f"F.expr({action.loop_start!r})"
                    )
                    stop_column = (
                        f"F.lit({action.loop_stop})"
                        if isinstance(action.loop_stop, int)
                        else f"F.expr({action.loop_stop!r})"
                    )
                    lines.extend(
                        [
                            f"{start_value} = {start_column}",
                            f"{stop_value} = {stop_column}",
                            f"{start_integer} = {start_value}.cast('long')",
                            f"{stop_integer} = {stop_value}.cast('long')",
                            f"if {frame}.filter({start_value}.isNull() | "
                            f"{stop_value}.isNull() | {start_integer}.isNull() | "
                            f"{stop_integer}.isNull() | ({start_value} != {start_integer}) | "
                            f"({stop_value} != {stop_integer})).limit(1).count():",
                            "    raise ValueError('Dynamic DO loop bounds must be present integers.')",
                        ]
                    )
                    if action.loop_step > 0:
                        active = f"({start_integer} <= {stop_integer})"
                        start_measure = f"{start_integer}.cast('decimal(38, 0)')"
                        stop_measure = f"{stop_integer}.cast('decimal(38, 0)')"
                        count = (
                            f"F.floor(({stop_measure} - {start_measure}) / "
                            f"{action.loop_step}) + 1"
                        )
                    else:
                        active = f"({start_integer} >= {stop_integer})"
                        start_measure = f"{start_integer}.cast('decimal(38, 0)')"
                        stop_measure = f"{stop_integer}.cast('decimal(38, 0)')"
                        count = (
                            f"F.floor(({start_measure} - {stop_measure}) / "
                            f"{abs(action.loop_step)}) + 1"
                        )
                    lines.extend(
                        [
                            f"if {frame}.filter(F.when({active}, {count})"
                            f".otherwise(F.lit(0)) > 5000).limit(1).count():",
                            "    raise ValueError('Dynamic DO loop exceeds the 5000-iteration limit.')",
                        ]
                    )
                    if action.kind == "do_runtime":
                        last_value = f"__sas2pyspark_loop_last_value_{action_index}"
                        lines.extend(
                            [
                                f"if any(column.casefold() == {last_value!r}.casefold() "
                                f"for column in {frame}.columns):",
                                "    raise ValueError('Dynamic DO loop temporary column conflicts with an input column.')",
                            ]
                        )
                        if action.loop_step > 0:
                            last_offset = (
                                f"F.floor(({stop_measure} - {start_measure}) / "
                                f"{action.loop_step}) * {action.loop_step}"
                            )
                        else:
                            last_offset = (
                                f"F.floor(({start_measure} - {stop_measure}) / "
                                f"{abs(action.loop_step)}) * {abs(action.loop_step)}"
                            )
                            last_offset = f"-({last_offset})"
                        last_expression = (
                            f"F.when({active}, {start_integer} + "
                            f"({last_offset}).cast('long')).otherwise({start_integer})"
                        )
                        lines.append(
                            f"{frame} = {frame}.withColumn({last_value!r}, "
                            f"{last_expression})"
                        )
                        assigned_names = tuple(
                            dict.fromkeys(
                                column.casefold()
                                for column, _ in action.loop_assignments
                            )
                        )
                        stateful_delete = any(
                            re.search(
                                rf"(?<![\w.]){re.escape(assigned_name)}(?![\w.(])",
                                condition,
                                re.IGNORECASE,
                            )
                            for condition in action.loop_delete_conditions
                            for assigned_name in assigned_names
                        )
                        stateful = stateful_delete or (
                            len(action.loop_assignments) > 1
                            or any(
                                re.search(
                                    rf"(?<![\w.]){re.escape(assigned_name)}(?![\w.(])",
                                    expression,
                                    re.IGNORECASE,
                                )
                                for _, expression in action.loop_assignments
                                for assigned_name in assigned_names
                            )
                        )
                        if action.loop_delete_conditions and not stateful_delete:
                            loop_iteration = (
                                f"__sas2pyspark_delete_iteration_{action_index}"
                            )
                            loop_state = (
                                f"__sas2pyspark_delete_state_{action_index}"
                            )
                            start_sql = (
                                str(action.loop_start)
                                if isinstance(action.loop_start, int)
                                else action.loop_start
                            )
                            stop_sql = (
                                str(action.loop_stop)
                                if isinstance(action.loop_stop, int)
                                else action.loop_stop
                            )
                            direction = "<=" if action.loop_step > 0 else ">="
                            delete_sequence = (
                                f"CASE WHEN ({start_sql}) {direction} ({stop_sql}) "
                                f"THEN sequence(CAST(({start_sql}) AS BIGINT), "
                                f"CAST(({stop_sql}) AS BIGINT), {action.loop_step}) "
                                "ELSE CAST(array() AS ARRAY<BIGINT>) END"
                            )
                            delete_expressions = []
                            for condition in action.loop_delete_conditions:
                                iteration_condition = _map_unquoted(
                                    condition,
                                    lambda part: re.sub(
                                        rf"(?<![\w.]){re.escape(action.column)}"
                                        rf"(?![\w.(])",
                                        loop_iteration,
                                        part,
                                        flags=re.IGNORECASE,
                                    ),
                                )
                                delete_expressions.append(
                                    f"aggregate({delete_sequence}, false, "
                                    f"({loop_state}, {loop_iteration}) -> "
                                    f"{loop_state} OR coalesce(("
                                    f"{iteration_condition}), false))"
                                )
                            delete_sql = " OR ".join(
                                f"({expression})"
                                for expression in delete_expressions
                            )
                            lines.append(
                                f"{frame} = {frame}.filter(~F.expr("
                                f"{delete_sql!r}))"
                            )
                        if stateful:
                            target_matches = (
                                f"__sas2pyspark_loop_target_matches_{action_index}"
                            )
                            resolved_targets = (
                                f"__sas2pyspark_loop_targets_{action_index}"
                            )
                            target_types = (
                                f"__sas2pyspark_loop_target_types_{action_index}"
                            )
                            state_value = (
                                f"__sas2pyspark_loop_state_{action_index}"
                            )
                            post_value = (
                                f"__sas2pyspark_loop_post_value_{action_index}"
                            )
                            for helper in (state_value, post_value):
                                lines.extend(
                                    [
                                        f"if any(column.casefold() == {helper!r}.casefold() "
                                        f"for column in {frame}.columns):",
                                        "    raise ValueError('Dynamic DO loop temporary column conflicts with an input column.')",
                                    ]
                                )
                            lines.extend(
                                [
                                    f"{target_matches} = [[column for column in "
                                    f"{frame}.columns if column.casefold() == name.casefold()] "
                                    f"for name in {list(assigned_names)!r}]",
                                    f"if any(len(found) != 1 for found in {target_matches}):",
                                    "    raise ValueError('Stateful dynamic DO loop assignment columns must exist exactly once in the input DataFrame.')",
                                    f"{resolved_targets} = [found[0] for found in {target_matches}]",
                                    f"{target_types} = [{frame}.schema[column].dataType.simpleString() "
                                    f"for column in {resolved_targets}]",
                                    f"if any(data_type == 'void' for data_type in {target_types}):",
                                    "    raise TypeError('Stateful dynamic DO loop assignment columns must have concrete Spark data types.')",
                                    f"{state_value}_types = []",
                                    f"for data_type in {target_types}:",
                                    "    if data_type.startswith('decimal('):",
                                    f"        {state_value}_types.append("
                                    "'decimal(38,' + data_type.split(',')[1].rstrip(')') + ')')",
                                    "    else:",
                                    f"        {state_value}_types.append(data_type)",
                                    f"{frame} = {frame}.withColumn({post_value!r}, "
                                    f"F.when({active}, F.col({last_value!r}) + "
                                    f"F.lit({action.loop_step})).otherwise("
                                    f"{start_integer}))",
                                ]
                            )
                            field_by_name = {
                                name: field_index
                                for field_index, name in enumerate(assigned_names)
                            }
                            loop_iteration = (
                                f"__sas2pyspark_loop_iteration_{action_index}"
                            )
                            step_state = f"__sas2pyspark_step_state_{action_index}"
                            step_number = f"__sas2pyspark_step_number_{action_index}"
                            state_fields = tuple(
                                f"_v{field_index}"
                                for field_index in range(len(assigned_names))
                            )

                            def replace_state_identifiers(part: str) -> str:
                                translated = re.sub(
                                    rf"(?<![\w.]){re.escape(action.column)}"
                                    rf"(?![\w.(])",
                                    loop_iteration,
                                    part,
                                    flags=re.IGNORECASE,
                                )
                                for field_name, field_index in sorted(
                                    field_by_name.items(),
                                    key=lambda entry: -len(entry[0]),
                                ):
                                    translated = re.sub(
                                        rf"`{re.escape(field_name)}`",
                                        f"{step_state}._v{field_index}",
                                        translated,
                                        flags=re.IGNORECASE,
                                    )
                                    translated = re.sub(
                                        rf"(?<![\w.]){re.escape(field_name)}"
                                        rf"(?![\w.(])",
                                        f"{step_state}._v{field_index}",
                                        translated,
                                        flags=re.IGNORECASE,
                                    )
                                return translated

                            rewritten_assignments = [
                                (
                                    field_by_name[column.casefold()],
                                    _map_unquoted(
                                        expression, replace_state_identifiers
                                    ),
                                )
                                for column, expression in action.loop_assignments
                            ]
                            start_sql = (
                                str(action.loop_start)
                                if isinstance(action.loop_start, int)
                                else action.loop_start
                            )
                            stop_sql = (
                                str(action.loop_stop)
                                if isinstance(action.loop_stop, int)
                                else action.loop_stop
                            )
                            sequence_sql = (
                                f"CASE WHEN ({start_sql}) "
                                f"{'<=' if action.loop_step > 0 else '>='} "
                                f"({stop_sql}) "
                                f"THEN sequence(CAST(({start_sql}) AS BIGINT), "
                                f"CAST(({stop_sql}) AS BIGINT), {action.loop_step}) "
                                "ELSE CAST(array() AS ARRAY<BIGINT>) END"
                            )
                            loop_operations = action.loop_operations or tuple(
                                ("assignment", assignment_index)
                                for assignment_index in range(
                                    len(action.loop_assignments)
                                )
                            )
                            lines.extend(
                                [
                                    f"{state_value}_sequence_sql = {sequence_sql!r}",
                                    f"{state_value}_initial_parts = []",
                                    f"for __loop_field_{action_index}, "
                                    f"__loop_column_{action_index} in enumerate({resolved_targets}):",
                                    f"    __loop_quoted_{action_index} = "
                                    f"'`' + __loop_column_{action_index}.replace('`', '``') + '`'",
                                    f"    {state_value}_initial_parts.extend(("
                                    f"repr({list(state_fields)!r}[__loop_field_{action_index}]), "
                                    f"'CAST(' + __loop_quoted_{action_index} + ' AS ' + "
                                    f"{state_value}_types[__loop_field_{action_index}] + ')'))",
                                    f"{state_value}_initial_parts.extend((repr('_deleted'), 'false'))",
                                    f"{state_value}_initial = 'named_struct(' + ', '.join("
                                    f"{state_value}_initial_parts) + ')'",
                                    f"{state_value}_branches = []",
                                ]
                            )
                            for operation_index, (operation, value) in enumerate(
                                loop_operations
                            ):
                                if operation == "assignment":
                                    updated_field, expression = rewritten_assignments[
                                        int(value)
                                    ]
                                    lines.extend(
                                        [
                                            f"{state_value}_fields_{operation_index} = []",
                                            f"for __loop_state_field_{operation_index} in range({len(assigned_names)}):",
                                            f"    if __loop_state_field_{operation_index} == {updated_field}:",
                                            f"        __loop_field_value_{operation_index} = "
                                            f"'CAST((' + {expression!r} + ') AS ' + "
                                            f"{state_value}_types[{updated_field}] + ')'",
                                            "    else:",
                                            f"        __loop_field_value_{operation_index} = "
                                            f"f'{step_state}._v{{__loop_state_field_{operation_index}}}'",
                                            f"    {state_value}_fields_{operation_index}.extend(("
                                            f"repr({list(state_fields)!r}[__loop_state_field_{operation_index}]), "
                                            f"__loop_field_value_{operation_index}))",
                                            f"{state_value}_fields_{operation_index}.extend(("
                                            f"repr('_deleted'), f'{step_state}._deleted'))",
                                            f"{state_value}_branches.append("
                                            f"f'WHEN {step_number} = {operation_index} THEN "
                                            f"named_struct(' + ', '.join("
                                            f"{state_value}_fields_{operation_index}) + ')')",
                                        ]
                                    )
                                elif operation == "delete":
                                    delete_condition = _map_unquoted(
                                        str(value), replace_state_identifiers
                                    )
                                    delete_fields = ", ".join(
                                        f"repr({field_name!r}), "
                                        f"f'{step_state}._v{field_index}'"
                                        for field_index, field_name in enumerate(
                                            state_fields
                                        )
                                    )
                                    lines.extend(
                                        [
                                            f"{state_value}_delete_fields_{operation_index} = "
                                            f"[{delete_fields}]",
                                            f"{state_value}_delete_fields_{operation_index}.extend(("
                                            f"repr('_deleted'), "
                                            f"{f'{step_state}._deleted OR coalesce(({delete_condition}), false)'!r}))",
                                            f"{state_value}_branches.append("
                                            f"f'WHEN {step_number} = {operation_index} THEN "
                                            f"named_struct(' + ', '.join("
                                            f"{state_value}_delete_fields_{operation_index}) + ')')",
                                        ]
                                    )
                                else:
                                    raise AssertionError(
                                        f"Unknown runtime DO loop operation: {operation!r}"
                                    )
                            lines.extend(
                                [
                                    f"{state_value}_step_sql = 'aggregate(array(' + "
                                    f"', '.join(str(number) for number in "
                                    f"range({len(loop_operations)})) + "
                                    f"'), {state_value}_state, ({step_state}, "
                                    f"{step_number}) -> CASE WHEN {step_state}._deleted "
                                    f"THEN {step_state} ' + ' '.join("
                                    f"{state_value}_branches) + ' ELSE {step_state} END)'",
                                    f"{state_value}_sql = 'aggregate(' + "
                                    f"{state_value}_sequence_sql + ', ' + "
                                    f"{state_value}_initial + ', ({state_value}_state, "
                                    f"{loop_iteration}) -> ' + {state_value}_step_sql + ')'",
                                    f"{frame} = {frame}.withColumn({state_value!r}, "
                                    f"F.expr({state_value}_sql))",
                                    f"{frame} = {frame}.filter("
                                    f"~F.col({state_value!r}).getField('_deleted'))",
                                ]
                            )
                            for field_index in range(len(assigned_names)):
                                lines.append(
                                    f"{frame} = {frame}.withColumn("
                                    f"{resolved_targets}[{field_index}], "
                                    f"F.col({state_value!r}).getField("
                                    f"{state_fields[field_index]!r}))"
                                )
                            lines.extend(
                                [
                                    f"{frame} = {frame}.withColumn({action.column!r}, "
                                    f"F.col({post_value!r}))",
                                    f"{frame} = {frame}.drop({state_value!r}, "
                                    f"{post_value!r}, {last_value!r})",
                                ]
                            )
                        elif action.loop_assignments:
                            assigned_column, expression = action.loop_assignments[0]
                            loop_reference = f"`{last_value}`"
                            expression = _map_unquoted(
                                expression,
                                lambda part: re.sub(
                                    rf"(?<![\w.]){re.escape(action.column)}"
                                    rf"(?![\w.(])",
                                    lambda _: loop_reference,
                                    part,
                                    flags=re.IGNORECASE,
                                ),
                            )
                            lines.append(
                                f"{frame} = {frame}.withColumn({assigned_column!r}, "
                                f"F.when({active}, F.expr({expression!r})).otherwise("
                                f"F.col({assigned_column!r}) if {assigned_column!r} "
                                f"in {frame}.columns else F.lit(None)))"
                            )
                            lines.extend(
                                [
                                    f"{frame} = {frame}.withColumn({action.column!r}, "
                                    f"F.when({active}, F.col({last_value!r}) + "
                                    f"F.lit({action.loop_step})).otherwise("
                                    f"{start_integer}))",
                                    f"{frame} = {frame}.drop({last_value!r})",
                                ]
                            )
                        else:
                            lines.extend(
                                [
                                    f"{frame} = {frame}.withColumn("
                                    f"{action.column!r}, F.when({active}, "
                                    f"F.col({last_value!r}) + "
                                    f"F.lit({action.loop_step})).otherwise("
                                    f"{start_integer}))",
                                    f"{frame} = {frame}.drop({last_value!r})",
                                ]
                            )
                    else:
                        lines.append(
                            f"{frame} = {frame}.withColumn({action.column!r}, "
                            f"F.explode(F.when({active}, F.sequence("
                            f"{start_integer}, {stop_integer}, F.lit({action.loop_step})))"
                            ".otherwise(F.array().cast('array<int>'))))"
                        )
                elif (
                    action.loop_step > 0
                    and isinstance(action.loop_start, int)
                    and isinstance(action.loop_stop, int)
                    and action.loop_start > action.loop_stop
                ) or (
                    action.loop_step < 0
                    and isinstance(action.loop_start, int)
                    and isinstance(action.loop_stop, int)
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
                if action.kind == "do_output":
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
            order_helpers = (
                []
                if order_by_keys is not None
                else ["__sas2pyspark_by_order"]
            )
            lines.append(
                f"{_python_name(target)} = {_python_name(target)}.drop("
                f"*{[*order_helpers, *by_flag_columns]!r})"
            )
        in_names = [name for name in merge_in_vars if name]
        if in_names:
            lines.append(f"{_python_name(target)} = {_python_name(target)}.drop(*{in_names!r})")
        lines.extend(
            [
                f"{_python_name(target)}.createOrReplaceTempView({target!r})",
                "",
            ]
        )
        data_step_warnings = []
        if order_by_keys is not None and not order_sensitive:
            data_step_warnings.append(
                "DATA-step ORDER BY does not sort output rows and has no effect "
                "unless the step uses an order-sensitive operation."
            )
        if retain_vars and order_by_keys is None:
            data_step_warnings.append(
                "RETAIN uses Spark monotonically_increasing_id() as a best-effort "
                "row order; Spark does not guarantee SAS observation order, so "
                "stateful results may differ or change across executions."
            )
        if sources is not None and len(sources) > 1:
            data_step_warnings.append(
                "Multiple SET inputs are concatenated with Spark unionByName; "
                "row order is unspecified and Spark schema coercion may differ "
                "from SAS variable type and length rules."
            )
        if by_flag_columns and order_by_keys is None:
            data_step_warnings.append(
                "BY-group FIRST./LAST. flags use Spark "
                "monotonically_increasing_id() as a best-effort observation "
                "order; row order is unspecified and results may differ from "
                "SAS or change across executions."
            )
        merge_action = next(
            (action for action in actions if action.kind == "merge"),
            None,
        )
        if merge_action is not None and not any(merge_action.merge_order_keys):
            data_step_warnings.append(
                "MERGE execution rejects duplicate BY-key groups unless every "
                "input supplies ORDER_BY=(...) for deterministic pairing."
            )
        if display_formats:
            data_step_warnings.append(
                "FORMAT statement for "
                + ", ".join(sorted(display_formats))
                + " is display-only in SAS; values are unchanged here. Use "
                "PUT(var, format.) where formatted text is required."
            )
        return lines, index + 1, data_step_warnings

    @staticmethod
    def _render_merge(
        target: str,
        sources: tuple[str, ...],
        keys: tuple[str, ...],
        directions: tuple[bool, ...],
        in_vars: tuple[str, ...] = (),
        order_keys: tuple[tuple[OrderKey, ...], ...] = (),
    ) -> list[str]:
        """Generate a SAS BY-group merge, repeating the last row of shorter groups."""
        target_frame = _python_name(target)
        input_frames = ", ".join(_python_name(source) for source in sources)
        input_order_keys = (
            [list(input_keys) for input_keys in order_keys]
            if order_keys and any(order_keys)
            else [[] for _ in sources]
        )
        in_selection = [
            f"__sas2pyspark_merge_selection.append(F.col('__sas2pyspark_m{index}_present').isNotNull().alias({name!r}))"
            for index, name in enumerate(in_vars)
            if name
        ]
        return [
            "from pyspark.sql import Window as __sas2pyspark_window",
            f"__sas2pyspark_merge_inputs = [{input_frames}]",
            f"__sas2pyspark_merge_keys = {list(keys)!r}",
            f"__sas2pyspark_merge_order_keys = {input_order_keys!r}",
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
            "    __sas2pyspark_merge_order_names = __sas2pyspark_merge_order_keys[__sas2pyspark_merge_index]",
            "    __sas2pyspark_merge_order_actual = [",
            "        next((__sas2pyspark_column for __sas2pyspark_column in __sas2pyspark_merge_frame.columns",
            "              if __sas2pyspark_column.casefold() == __sas2pyspark_name.casefold()), None)",
            "        for __sas2pyspark_name, _ in __sas2pyspark_merge_order_names",
            "    ]",
            "    if any(__sas2pyspark_column is None for __sas2pyspark_column in __sas2pyspark_merge_order_actual):",
            "        raise ValueError('MERGE ORDER_BY column is missing from its input DataFrame.')",
            "    if __sas2pyspark_merge_order_actual:",
            "        __sas2pyspark_order_uniqueness_columns = list(dict.fromkeys([*__sas2pyspark_merge_actual_keys, *__sas2pyspark_merge_order_actual]))",
            "        if (__sas2pyspark_merge_frame.count() != __sas2pyspark_merge_frame",
            "                .dropDuplicates(__sas2pyspark_order_uniqueness_columns).count()):",
            "            raise ValueError('MERGE ORDER_BY keys must be unique within each BY group.')",
            "        __sas2pyspark_merge_order_by = [",
            "            F.col(__sas2pyspark_column).asc() if __sas2pyspark_ascending else F.col(__sas2pyspark_column).desc()",
            "            for __sas2pyspark_column, (_, __sas2pyspark_ascending) in zip(__sas2pyspark_merge_order_actual, __sas2pyspark_merge_order_names)",
            "        ]",
            "    else:",
            "        __sas2pyspark_group_count_name = '__sas2pyspark_group_count'",
            "        while __sas2pyspark_group_count_name.casefold() in {",
            "                __sas2pyspark_column.casefold()",
            "                for __sas2pyspark_column in __sas2pyspark_merge_frame.columns}:",
            "            __sas2pyspark_group_count_name += '_'",
            "        __sas2pyspark_group_counts = (",
            "            __sas2pyspark_merge_frame",
            "            .groupBy(*__sas2pyspark_merge_actual_keys)",
            "            .agg(F.count(F.lit(1)).alias(__sas2pyspark_group_count_name)))",
            "        if (__sas2pyspark_group_counts",
            "                .filter(F.col(__sas2pyspark_group_count_name) > 1)",
            "                .limit(1).count()):",
            "            raise ValueError('SAS MERGE duplicate BY keys require ORDER_BY=(...) on every input for deterministic pairing.')",
            "        __sas2pyspark_merge_order_by = [F.monotonically_increasing_id()]",
            "    __sas2pyspark_merge_window = __sas2pyspark_window.partitionBy(*__sas2pyspark_merge_actual_keys)",
            "    __sas2pyspark_merge_numbered = (__sas2pyspark_merge_frame",
            "        .withColumn('__sas2pyspark_rn', F.row_number().over(__sas2pyspark_merge_window.orderBy(*__sas2pyspark_merge_order_by)))",
            "        .withColumn('__sas2pyspark_cnt', F.count(F.lit(1)).over(__sas2pyspark_merge_window)))",
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
            "    __sas2pyspark_merge_projection.append(F.col('__sas2pyspark_rn').alias(f'__sas2pyspark_m{__sas2pyspark_merge_index}_rn'))",
            "    __sas2pyspark_merge_projection.append(F.col('__sas2pyspark_cnt').alias(f'__sas2pyspark_m{__sas2pyspark_merge_index}_cnt'))",
            "    __sas2pyspark_merge_frames.append(__sas2pyspark_merge_numbered.select(*__sas2pyspark_merge_projection))",
            "__sas2pyspark_merge_base = None",
            "for __sas2pyspark_merge_index, __sas2pyspark_merge_frame in enumerate(__sas2pyspark_merge_frames):",
            "    __sas2pyspark_merge_counts = __sas2pyspark_merge_frame.select(",
            "        *[F.col(f'__sas2pyspark_m{__sas2pyspark_merge_index}_key{__sas2pyspark_key_index}').alias(f'__sas2pyspark_base_key{__sas2pyspark_key_index}')",
            "          for __sas2pyspark_key_index in range(len(__sas2pyspark_merge_keys))],",
            "        F.col(f'__sas2pyspark_m{__sas2pyspark_merge_index}_cnt').alias('__sas2pyspark_n'))",
            "    __sas2pyspark_merge_base = __sas2pyspark_merge_counts if __sas2pyspark_merge_base is None else __sas2pyspark_merge_base.unionByName(__sas2pyspark_merge_counts)",
            "__sas2pyspark_merge_joined = (__sas2pyspark_merge_base",
            "    .groupBy(*[f'__sas2pyspark_base_key{__sas2pyspark_key_index}' for __sas2pyspark_key_index in range(len(__sas2pyspark_merge_keys))])",
            "    .agg(F.max('__sas2pyspark_n').alias('__sas2pyspark_n'))",
            "    .withColumn('__sas2pyspark_k', F.explode(F.sequence(F.lit(1), F.col('__sas2pyspark_n'))))",
            "    .drop('__sas2pyspark_n'))",
            "for __sas2pyspark_merge_index in range(len(__sas2pyspark_merge_frames)):",
            "    __sas2pyspark_merge_condition = (F.col(f'__sas2pyspark_m{__sas2pyspark_merge_index}_rn')",
            "        == F.least(F.col('__sas2pyspark_k'), F.col(f'__sas2pyspark_m{__sas2pyspark_merge_index}_cnt')))",
            "    for __sas2pyspark_key_index in range(len(__sas2pyspark_merge_keys)):",
            "        __sas2pyspark_merge_condition = __sas2pyspark_merge_condition & F.col(f'__sas2pyspark_base_key{__sas2pyspark_key_index}').eqNullSafe(F.col(f'__sas2pyspark_m{__sas2pyspark_merge_index}_key{__sas2pyspark_key_index}'))",
            "    __sas2pyspark_merge_joined = __sas2pyspark_merge_joined.join(",
            "        __sas2pyspark_merge_frames[__sas2pyspark_merge_index],",
            "        __sas2pyspark_merge_condition,",
            "        'left',",
            "    )",
            "__sas2pyspark_merge_columns = list(dict.fromkeys(",
            "    __sas2pyspark_column",
            "    for __sas2pyspark_input_columns in __sas2pyspark_merge_payload",
            "    for __sas2pyspark_column in __sas2pyspark_input_columns",
            "))",
            "__sas2pyspark_merge_selection = [",
            "    F.col(f'__sas2pyspark_base_key{__sas2pyspark_key_index}').alias(__sas2pyspark_key)",
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
            *in_selection,
            "__sas2pyspark_merge_selection.append(F.col('__sas2pyspark_k'))",
            f"{target_frame} = __sas2pyspark_merge_joined.select(*__sas2pyspark_merge_selection).orderBy("
            + ", ".join(
                f"F.col({key!r}).{'asc' if ascending else 'desc'}()"
                for key, ascending in zip(keys, directions)
            )
            + ", F.col('__sas2pyspark_k').asc()).drop('__sas2pyspark_k')",
            "",
        ]

    def _convert_sort(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int, list[str]]:
        data: str | None = None
        target: str | None = None
        keys: list[tuple[str, bool]] = []
        nodupkey = False
        nodup = False
        dupout: str | None = None
        where_filters: list[str] = []
        header_options = statements[start].text[len("proc sort") :].strip().split()
        for option in header_options:
            lower_option = option.lower()
            if lower_option.startswith("data="):
                data = _dataset_name(option[5:])
            elif lower_option.startswith("out="):
                target = _dataset_name(option[4:])
            elif lower_option.startswith("dupout="):
                dupout = _dataset_name(option[7:])
            elif lower_option == "nodupkey":
                nodupkey = True
            elif lower_option in {"nodup", "noduprecs"}:
                nodup = True
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
                elif lower.startswith("dupout="):
                    dupout = _dataset_name(statement[7:].strip())
                elif lower == "nodupkey":
                    nodupkey = True
                elif lower in {"nodup", "noduprecs"}:
                    nodup = True
                elif lower.startswith("where "):
                    where_filters.append(_expression(statement[6:]))
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
        if dupout is not None and not (nodupkey or nodup):
            raise ConversionError("PROC SORT DUPOUT= requires NODUPKEY or NODUP.")
        if dupout is not None and nodup and nodupkey:
            raise ConversionError(
                "PROC SORT DUPOUT= with both NODUPKEY and NODUP is unsupported."
            )
        target = target or data
        frame = _python_name(data)
        target_frame = _python_name(target)
        key_names = [name for name, _ in keys]
        order = ", ".join(
            f"F.col({name!r}).{'asc' if ascending else 'desc'}()" for name, ascending in keys
        )
        lines: list[str] = []
        warnings: list[str] = []
        source_frame = frame
        if where_filters:
            condition = " AND ".join(f"({item})" for item in where_filters)
            source_frame = f"{target_frame}__filtered"
            lines.append(f"{source_frame} = {frame}.filter(F.expr({condition!r}))")
        if nodupkey:
            partition = ", ".join(f"F.col({name!r})" for name in key_names)
            ordering = ", ".join(
                f"F.col({name!r}).{'asc' if ascending else 'desc'}()"
                for name, ascending in keys
            )
            ranked = f"{target_frame}__ranked"
            lines.extend(
                [
                    f"{ranked} = {source_frame}.withColumn("
                    "'__sas2pyspark_row_number', "
                    f"F.row_number().over(__sas2pyspark_window.partitionBy({partition}).orderBy({ordering}))"
                    ")",
                    f"{target_frame} = {ranked}.filter(F.col('__sas2pyspark_row_number') == 1).drop('__sas2pyspark_row_number')",
                ]
            )
            if dupout is not None:
                lines.extend(
                    [
                        f"{_python_name(dupout)} = {ranked}.filter(F.col('__sas2pyspark_row_number') > 1).drop('__sas2pyspark_row_number').orderBy({order})",
                        f"{_python_name(dupout)}.createOrReplaceTempView({_sql_name(dupout)!r})",
                    ]
                )
            warnings.append(
                "PROC SORT NODUPKEY tie behavior may differ from SAS when BY keys tie."
            )
        elif nodup:
            lines.append(f"{target_frame} = {source_frame}.dropDuplicates()")
            if dupout is not None:
                lines.extend(
                    [
                        f"{_python_name(dupout)} = {source_frame}.exceptAll({target_frame})",
                        f"{_python_name(dupout)}.createOrReplaceTempView({_sql_name(dupout)!r})",
                    ]
                )
            warnings.append(
                "PROC SORT NODUP removes only adjacent identical observations in "
                "SAS; Spark removes all fully identical rows, which can differ when "
                "duplicates are not adjacent after sorting."
            )
        else:
            lines.append(f"{target_frame} = {source_frame}")
        lines.append(f"{target_frame} = {target_frame}.orderBy({order})")
        lines.extend(
            [
                f"{target_frame}.createOrReplaceTempView({_sql_name(target)!r})",
                "",
            ]
        )
        if nodupkey:
            lines.insert(0, "from pyspark.sql import Window as __sas2pyspark_window")
        return lines, index + 1, warnings

    def _convert_format(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int]:
        header = statements[start].text[len("proc format") :].strip()
        cntlin_match = re.fullmatch(
            r"cntlin\s*=\s*((?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*)",
            header,
            re.I,
        )
        if header and cntlin_match is None:
            raise ConversionError(f"Unsupported PROC FORMAT option: {header!r}.")
        if cntlin_match:
            dataset = _dataset_name(cntlin_match.group(1))
            register_cntlin(dataset)
        names: list[str] = []
        index = start + 1
        while index < len(statements):
            source_statement = statements[index]
            lower = source_statement.text.lower().strip()
            if lower in {"run", "quit"}:
                break
            try:
                if cntlin_match:
                    raise ConversionError(
                        "PROC FORMAT CNTLIN= does not accept VALUE, INVALUE or PICTURE "
                        "statements in the same step."
                    )
                if lower.startswith("value "):
                    try:
                        names.append(register_value(source_statement.text).name)
                    except FormatError as error:
                        raise ConversionError(str(error)) from error
                elif lower.startswith("invalue "):
                    try:
                        names.append(register_invalue(source_statement.text).name)
                    except FormatError as error:
                        raise ConversionError(str(error)) from error
                elif lower.startswith("picture "):
                    try:
                        names.append(register_picture(source_statement.text).name)
                    except FormatError as error:
                        raise ConversionError(str(error)) from error
                else:
                    raise ConversionError(
                        f"Unsupported PROC FORMAT statement: {source_statement.text[:60]!r}."
                    )
            except ConversionError as error:
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1
        else:
            raise ConversionError("PROC FORMAT is missing RUN.")
        if cntlin_match:
            source = _python_name(dataset)
            lines = [
                "# PROC FORMAT CNTLIN: rows are collected to the driver; use a small control dataset.",
                "from pyspark.sql.types import StringType as __sas2pyspark_format_string_type",
                f"__sas2pyspark_cntlin_rows = {source}.select("
                "'FMTNAME', 'TYPE', 'START', 'END', 'LABEL', 'HLO').collect()",
                "__sas2pyspark_cntlin_formats = {}",
                "for __row in __sas2pyspark_cntlin_rows:",
                "    __name = str(__row['FMTNAME']).casefold().lstrip('$')",
                "    __sas2pyspark_cntlin_formats.setdefault(__name, []).append((",
                "        str(__row['TYPE']).upper(), __row['START'], __row['END'],",
                "        __row['LABEL'], str(__row['HLO'] or '').upper()))",
                "def __sas2pyspark_cntlin_apply(__name, __value):",
                "    if __value is None:",
                "        return None",
                "    __entries = __sas2pyspark_cntlin_formats.get(str(__name).casefold().lstrip('$'))",
                "    if __entries is None:",
                "        raise ValueError(f'CNTLIN format {__name!r} is not defined.')",
                "    __other = None",
                "    for __kind, __start, __end, __label, __hlo in __entries:",
                "        if 'O' in __hlo:",
                "            __other = __label",
                "            continue",
                "        if __kind == 'N':",
                "            __value_cmp = float(__value)",
                "            __start_cmp = float(__start) if __start is not None else float('-inf')",
                "            __end_cmp = float(__end) if __end is not None else __start_cmp",
                "        else:",
                "            __value_cmp = str(__value)",
                "            __start_cmp = str(__start) if __start is not None else ''",
                "            __end_cmp = str(__end) if __end is not None else __start_cmp",
                "        if __start_cmp <= __value_cmp <= __end_cmp:",
                "            return None if __label is None else str(__label)",
                "    return None if __other is None else str(__other)",
                "spark.udf.register('sas2pyspark_cntlin_format', "
                "__sas2pyspark_cntlin_apply, __sas2pyspark_format_string_type())",
                "",
            ]
            return lines, index + 1
        return [f"# PROC FORMAT: defined {', '.join(names)}; applied where PUT() uses them.", ""], index + 1

    def _parse_libname(self, text: str) -> str:
        match = re.fullmatch(
            r"libname\s+([A-Za-z_]\w{0,7})\s+(?:clear|(?:[A-Za-z]\w*\s+)?(?:'[^']*'|\"[^\"]*\")(?:\s+[\w=\-./\\]+)*)",
            text.strip(),
            re.I,
        )
        if not match:
            raise ConversionError(f"Unsupported LIBNAME statement: {text!r}.")
        return match.group(1).lower()

    def _append_lines(
        self, base: str, data: str, force: bool
    ) -> tuple[list[str], list[str]]:
        base_frame = _python_name(base)
        data_frame = _python_name(data)
        lines = [
            f"{base_frame} = {base_frame}.unionByName({data_frame}, allowMissingColumns={force})",
            f"{base_frame}.createOrReplaceTempView({_sql_name(base)!r})",
        ]
        warnings = []
        if force:
            warnings.append(
                "PROC APPEND FORCE fills columns missing on either side with null; "
                "SAS truncates or drops mismatched variables instead."
            )
        return lines, warnings

    def _convert_append(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int, list[str]]:
        base: str | None = None
        data: str | None = None
        force = False
        for option in statements[start].text[len("proc append") :].split():
            lower_option = option.lower()
            if lower_option.startswith("base="):
                base = _dataset_name(option[5:])
            elif lower_option.startswith("data="):
                data = _dataset_name(option[5:])
            elif lower_option == "force":
                force = True
            else:
                raise ConversionError(f"Unsupported PROC APPEND option: {option!r}.")
        if base is None or data is None:
            raise ConversionError("PROC APPEND requires BASE= and DATA=.")
        index = start + 1
        if index < len(statements) and statements[index].text.lower() in {"run", "quit"}:
            index += 1
        lines, warnings = self._append_lines(base, data, force)
        lines.append("")
        return lines, index, warnings

    def _convert_datasets(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int]:
        library = "work"
        for option in statements[start].text[len("proc datasets") :].split():
            lower_option = option.lower()
            if lower_option.startswith(("lib=", "library=")):
                library = option.split("=", 1)[1].lower()
            elif lower_option in {"nolist", "nodetails", "nowarn"}:
                continue
            else:
                raise ConversionError(f"Unsupported PROC DATASETS option: {option!r}.")
        lines: list[str] = []
        index = start + 1
        terminated = False
        while index < len(statements):
            source_statement = statements[index]
            statement = source_statement.text.strip()
            lower = statement.lower()
            if lower in {"run", "quit"}:
                terminated = True
                index += 1
                if lower == "run" and index < len(statements) and statements[index].text.lower() == "quit":
                    index += 1
                break
            try:
                if lower.startswith("delete "):
                    for name in statement[7:].split():
                        if not re.fullmatch(r"[A-Za-z_]\w*", name):
                            raise ConversionError(f"Unsupported DELETE member: {name!r}.")
                        qualified = _dataset_name(f"{library}.{name}")
                        lines.extend(
                            [
                                f"{_python_name(qualified)} = None",
                                f"spark.catalog.dropTempView({_sql_name(qualified)!r})",
                            ]
                        )
                elif lower.startswith("change "):
                    pairs = re.findall(r"(\w+)\s*=\s*(\w+)", statement[7:])
                    if not pairs:
                        raise ConversionError("Malformed CHANGE statement.")
                    for old, new in pairs:
                        old_name = _dataset_name(f"{library}.{old}")
                        new_name = _dataset_name(f"{library}.{new}")
                        lines.extend(
                            [
                                f"{_python_name(new_name)} = {_python_name(old_name)}",
                                f"{_python_name(new_name)}.createOrReplaceTempView({_sql_name(new_name)!r})",
                                f"spark.catalog.dropTempView({_sql_name(old_name)!r})",
                                f"{_python_name(old_name)} = None",
                            ]
                        )
                elif lower.startswith("append "):
                    base = data = None
                    force = False
                    for option in statement[7:].split():
                        lower_option = option.lower()
                        if lower_option.startswith("base="):
                            base = _dataset_name(option[5:])
                        elif lower_option.startswith("data="):
                            data = _dataset_name(option[5:])
                        elif lower_option == "force":
                            force = True
                        else:
                            raise ConversionError(f"Unsupported APPEND option: {option!r}.")
                    if base is None or data is None:
                        raise ConversionError("APPEND requires BASE= and DATA=.")
                    append_lines, _ = self._append_lines(base, data, force)
                    lines.extend(append_lines)
                else:
                    raise ConversionError(
                        f"Unsupported PROC DATASETS statement: {statement!r}."
                    )
            except ConversionError as error:
                raise ConversionError(
                    error.message,
                    line=source_statement.line,
                    statement=source_statement.text,
                ) from error
            index += 1
        if not terminated:
            raise ConversionError("PROC DATASETS is missing RUN or QUIT.")
        lines.append("")
        return lines, index

    def _convert_report_proc(
        self, statements: list[_Statement], start: int
    ) -> tuple[list[str], int, list[str]]:
        header = statements[start].text
        proc = header.split()[1].lower()
        options: dict[str, str] = {}
        flags: set[str] = set()
        for token in re.findall(r"[\w.]+(?:\s*=\s*\(?[\w.]+\)?)?", header[len("proc " + proc) :]):
            if "=" in token:
                key, value = (part.strip() for part in token.split("=", 1))
                options[key.lower()] = value.strip("()")
            else:
                flags.add(token.lower())
        if re.search(r"=\s*[\w.]+\s*\(", header):
            raise ConversionError(f"PROC {proc.upper()} does not support dataset options.")

        body: list[_Statement] = []
        index = start + 1
        while index < len(statements):
            lower = statements[index].text.strip().lower()
            if lower in {"run", "quit"}:
                index += 1
                break
            if lower.startswith(("proc ", "data ")):
                break
            body.append(statements[index])
            index += 1

        def frame(name: str) -> str:
            if not re.fullmatch(r"(?:\w+\.)?\w+", name):
                raise ConversionError(f"Unsupported dataset reference: {name!r}.")
            return _python_name(_dataset_name(name))

        def reject(item: _Statement) -> ConversionError:
            return ConversionError(
                f"Unsupported PROC {proc.upper()} statement: {item.text[:60]!r}.",
                line=item.line,
                statement=item.text,
            )

        lines: list[str] = []
        warnings: list[str] = []
        if proc == "print":
            allowed_flags = {"noobs"}
            if "data" not in options or not flags <= allowed_flags or set(options) - {"data", "obs"}:
                raise ConversionError(
                    "PROC PRINT supports DATA=, OBS=, NOOBS and the VAR and WHERE statements."
                )
            if "obs" in options and not options["obs"].isdigit():
                raise ConversionError("PROC PRINT OBS= requires a positive integer.")
            target = frame(options["data"])
            columns: list[str] = []
            conditions: list[str] = []
            for item in body:
                text = item.text.strip()
                if re.match(r"var\s", text, re.I):
                    columns = text.split()[1:]
                    if any(not re.fullmatch(r"[A-Za-z_]\w*", name) for name in columns):
                        raise reject(item)
                elif re.match(r"where\s", text, re.I):
                    try:
                        conditions.append(_expression(text[6:].strip()))
                    except ConversionError as error:
                        raise ConversionError(
                            error.message, line=item.line, statement=item.text
                        ) from error
                else:
                    raise reject(item)
            expression = target
            for condition in conditions:
                expression += f".filter(F.expr({condition!r}))"
            if columns:
                expression += f".select(*{columns!r})"
            limit = int(options["obs"]) if "obs" in options else 1000
            lines.append(f"{expression}.show({limit}, truncate=False)")
            warnings.append(
                "PROC PRINT is converted to DataFrame.show(); output is capped at "
                f"{limit} rows and is not formatted like SAS listing output."
            )
        elif proc == "contents":
            if (
                "data" not in options
                or not flags <= {"noprint", "short", "varnum"}
                or set(options) - {"data", "out"}
            ):
                raise ConversionError(
                    "PROC CONTENTS supports DATA=, OUT=, NOPRINT, SHORT and VARNUM."
                )
            if body:
                raise reject(body[0])
            target = frame(options["data"])
            if "out" in options:
                metadata = frame(options["out"])
                lines.extend(
                    [
                        f"{metadata} = spark.createDataFrame("
                        f"[(field.name, field.dataType.simpleString(), "
                        f"field.nullable, position) for position, field in "
                        f"enumerate({target}.schema.fields, start=1)], "
                        "'NAME STRING, SPARK_TYPE STRING, NULLABLE BOOLEAN, "
                        "POSITION INT')",
                        f"{metadata}.createOrReplaceTempView("
                        f"{_sql_name(_dataset_name(options['out']))!r})",
                    ]
                )
                warnings.append(
                    "PROC CONTENTS OUT= emits Spark schema metadata "
                    "(NAME, SPARK_TYPE, NULLABLE, POSITION), not the SAS "
                    "PROC CONTENTS output schema."
                )
            if "noprint" not in flags:
                if "short" in flags:
                    lines.append(f"print({target}.columns)")
                else:
                    lines.append(f"{target}.printSchema()")
            else:
                lines.append("# PROC CONTENTS NOPRINT: nothing to display.")
        elif proc == "copy":
            if set(options) - {"in", "out"} or flags - {"memtype", "nolist"} or "in" not in options or "out" not in options:
                raise ConversionError("PROC COPY requires IN= and OUT= and supports no other options.")
            members: list[str] = []
            for item in body:
                text = item.text.strip()
                if re.match(r"select\s", text, re.I):
                    members.extend(text.split()[1:])
                else:
                    raise reject(item)
            if not members:
                raise ConversionError(
                    "PROC COPY requires a SELECT statement; copying a whole library "
                    "cannot be resolved at conversion time."
                )
            for member in members:
                source = _dataset_name(f"{options['in']}.{member}")
                destination = _dataset_name(f"{options['out']}.{member}")
                lines.extend(
                    [
                        f"{_python_name(destination)} = {_python_name(source)}",
                        f"{_python_name(destination)}.createOrReplaceTempView({_sql_name(destination)!r})",
                    ]
                )
        else:
            if "base" not in options or "compare" not in options or flags - {"noprint", "brief", "listall"} or set(options) - {"base", "compare"}:
                raise ConversionError("PROC COMPARE supports BASE= and COMPARE= only.")
            if body:
                raise reject(body[0])
            base, other = frame(options["base"]), frame(options["compare"])
            lines.extend(
                [
                    f"__only_base = {base}.exceptAll({other}.select(*{base}.columns)).count() if set({base}.columns) <= set({other}.columns) else None",
                    f"__only_compare = {other}.select(*{base}.columns).exceptAll({base}).count() if set({base}.columns) <= set({other}.columns) else None",
                    f"print('PROC COMPARE: base rows', {base}.count(), 'compare rows', {other}.count(),",
                    "      'only in base', __only_base, 'only in compare', __only_compare,",
                    f"      'columns only in base', sorted(set({base}.columns) - set({other}.columns)),",
                    f"      'columns only in compare', sorted(set({other}.columns) - set({base}.columns)))",
                ]
            )
            warnings.append(
                "PROC COMPARE prints a row and column difference summary only; "
                "SAS's per-variable value comparison report is not generated."
            )
        lines.append("")
        return lines, index, warnings

    def _convert_freq(
        self, statements: list[_Statement], start: int, freq_count: int
    ) -> tuple[list[str], int, int]:
        data: str | None = None
        include_missing = False
        header_options = statements[start].text[len("proc freq") :].strip().split()
        for option in header_options:
            if option.lower().startswith("data="):
                data = _dataset_name(option[5:])
            elif option.lower() == "missing":
                include_missing = True
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
        input_frame = frame
        if not include_missing:
            nonmissing = " & ".join(
                f"F.col({column!r}).isNotNull()" for column in table_columns
            )
            input_frame = f"__sas2pyspark_freq_input_{freq_count}"
        lines = [
            "from pyspark.sql import Window as __sas2pyspark_window",
        ]
        if not include_missing:
            lines.append(f"{input_frame} = {frame}.filter({nonmissing})")
        lines.append(
            f"__sas2pyspark_freq_counts = {input_frame}.groupBy("
            f"{column_selection}).agg(F.count(F.lit(1)).alias('COUNT'))",
        )
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
        include_missing_class = False
        default_statistics_used = False
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
            "median",
            "range",
        }
        for option in header_options:
            if option.lower().startswith("data="):
                if data is not None:
                    raise ConversionError("PROC MEANS/SUMMARY accepts DATA= only once.")
                data = _dataset_name(option[5:])
            elif option.lower() == "missing":
                include_missing_class = True
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
            statistics = ["n", "mean", "std", "min", "max"]
            default_statistics_used = True
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
            "median": "median",
        }
        for statistic, names in zip(statistics, output_columns):
            for column, output_name in zip(var_columns, names):
                column_ref = f"F.col({column!r})"
                if statistic == "range":
                    aggregate = f"(F.max({column_ref}) - F.min({column_ref}))"
                elif statistic == "nmiss":
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
        if default_statistics_used:
            lines.append(
                "# PROC MEANS/SUMMARY defaulted to N, MEAN, STD, MIN and MAX."
            )
        if class_columns:
            class_selection = ", ".join(
                f"F.col({column!r})" for column in class_columns
            )
            if include_missing_class:
                lines.append(f"__sas2pyspark_means_input = {frame}")
            else:
                class_filter = " & ".join(
                    f"F.col({column!r}).isNotNull()" for column in class_columns
                )
                lines.append(
                    f"__sas2pyspark_means_input = {frame}.filter({class_filter})"
                )
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
                    into = _SQL_INTO.search(statement)
                    if into is not None:
                        if into.group(2):
                            raise ConversionError(
                                "PROC SQL SELECT ... INTO :macro supports only "
                                "single-row results; SEPARATED BY is unsupported."
                            )
                        names = re.findall(r":\s*([A-Za-z_]\w*)", into.group(1))
                        query = self._normalize_sql(
                            statement[: into.start()] + " " + statement[into.end() :]
                        )
                        lines.append(f"_into_row = spark.sql({query!r}).first()")
                        for position, name in enumerate(names):
                            declare = (
                                f"DECLARE OR REPLACE VARIABLE sas_macro_{name.lower()} "
                                "DEFAULT :v"
                            )
                            lines.append(
                                f"spark.sql({declare!r}, args={{'v': "
                                f"_into_row[{position}] if _into_row is not None else None}})"
                            )
                        lines.append("")
                        index += 1
                        continue
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
        try:
            normalized = rewrite_calculated(normalized)
        except ValueError as error:
            raise ConversionError(str(error)) from error
        return _map_unquoted(
            normalized,
            lambda part: re.sub(
                r"\b(FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b",
                lambda match: f"{match.group(1)} {match.group(3)}",
                part,
                flags=re.IGNORECASE,
            ),
        ).strip()


def convert_sas(
    source: str, *, sas_version: str = "auto", allow_sysget: bool = False
) -> ConversionResult:
    """Convenience function for converting SAS source text."""
    return SasToPySparkConverter(allow_sysget=allow_sysget).convert(
        source, sas_version=sas_version
    )
