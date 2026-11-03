"""SAS date functions, date literals and common formats/informats as Spark SQL."""

from __future__ import annotations

import re

from .user_formats import FormatError, has_format, render_user_format
DATE_FUNCTIONS = frozenset(
    {"MDY", "INTCK", "INTNX", "DATDIF", "QTR", "WEEKDAY", "PUT", "INPUT"}
)

_MONTHS = {
    name: number
    for number, name in enumerate(
        "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), start=1
    )
}
_DATE_LITERAL = re.compile(
    r"""(['"])(\d{1,2})([A-Za-z]{3})(\d{4})\1\s*d\b""", re.IGNORECASE
)

# SAS format name -> (Spark datetime pattern, upper-case the result)
_DATE_FORMATS = {
    "DATE9": ("ddMMMyyyy", True),
    "YYMMDD10": ("yyyy-MM-dd", False),
    "YYMMDDN8": ("yyyyMMdd", False),
    "MMDDYY10": ("MM/dd/yyyy", False),
    "DDMMYY10": ("dd/MM/yyyy", False),
}
_FORMAT_NAME = re.compile(r"([A-Za-z]*)(\d*)\.(\d*)")
_SAS_EPOCH_SUNDAY = "DATE'1960-01-03'"


class DateFunctionError(ValueError):
    """Raised for unsupported or malformed date/format usage."""


def translate_date_literals(expression: str) -> str:
    def replace(match: re.Match[str]) -> str:
        day = int(match.group(2))
        month = _MONTHS.get(match.group(3).upper())
        year = int(match.group(4))
        if month is None or not 1 <= day <= 31:
            raise DateFunctionError(f"Invalid SAS date literal: {match.group(0)!r}.")
        return f"DATE'{year:04d}-{month:02d}-{day:02d}'"

    return _DATE_LITERAL.sub(replace, expression)


def _literal(argument: str, purpose: str) -> str:
    text = argument.strip()
    if len(text) < 2 or text[0] != text[-1] or text[0] not in {"'", '"'}:
        raise DateFunctionError(f"{purpose} must be a quoted literal.")
    return text[1:-1].strip()


def _as_date(argument: str) -> str:
    return f"CAST({argument} AS DATE)"


def _interval(argument: str, function: str) -> str:
    value = _literal(argument, f"{function} interval").upper().rstrip(".")
    aliases = {"QUARTER": "QTR", "WEEK": "WEEK", "DAY": "DAY", "MONTH": "MONTH", "YEAR": "YEAR", "QTR": "QTR"}
    if value not in aliases:
        raise DateFunctionError(
            f"{function} interval {value!r} is unsupported; supported: "
            "DAY, WEEK, MONTH, QTR, YEAR."
        )
    return aliases[value]


def _intck(arguments: list[str]) -> str:
    if len(arguments) != 3:
        raise DateFunctionError("INTCK requires an interval, a start and an end date.")
    unit = _interval(arguments[0], "INTCK")
    start, end = _as_date(arguments[1]), _as_date(arguments[2])
    if unit == "DAY":
        return f"datediff({end}, {start})"
    if unit == "WEEK":
        return (
            f"(floor(datediff({end}, {_SAS_EPOCH_SUNDAY}) / 7) - "
            f"floor(datediff({start}, {_SAS_EPOCH_SUNDAY}) / 7))"
        )
    months = f"((year({end}) - year({start})) * 12 + month({end}) - month({start}))"
    if unit == "MONTH":
        return months
    if unit == "QTR":
        return f"((year({end}) - year({start})) * 4 + quarter({end}) - quarter({start}))"
    return f"(year({end}) - year({start}))"


def _intnx(arguments: list[str]) -> str:
    if len(arguments) not in (3, 4):
        raise DateFunctionError(
            "INTNX requires an interval, a date, an increment and an optional alignment."
        )
    unit = _interval(arguments[0], "INTNX")
    date, count = _as_date(arguments[1]), f"({arguments[2]})"
    alignment = "B"
    if len(arguments) == 4:
        alignment = _literal(arguments[3], "INTNX alignment").upper()
        alignment = {
            "BEGIN": "B",
            "BEGINNING": "B",
            "END": "E",
            "SAME": "S",
            "SAMEDAY": "S",
        }.get(alignment, alignment)
        if alignment not in {"B", "E", "S"}:
            raise DateFunctionError(
                "INTNX alignment must be BEGINNING, END or SAME."
            )
    if unit == "DAY":
        return f"date_add({date}, {count})"
    if unit == "WEEK":
        start = f"date_sub({date}, dayofweek({date}) - 1)"
        if alignment == "B":
            return f"date_add({start}, 7 * {count})"
        if alignment == "E":
            return f"date_add({start}, 7 * {count} + 6)"
        return f"date_add({date}, 7 * {count})"
    months_per = {"MONTH": 1, "QTR": 3, "YEAR": 12}[unit]
    shift = f"{months_per} * {count}"
    if alignment == "S":
        return f"add_months({date}, {shift})"
    truncation = {"MONTH": "MM", "QTR": "quarter", "YEAR": "YEAR"}[unit]
    base = f"trunc({date}, '{truncation}')"
    if alignment == "B":
        return f"add_months({base}, {shift})"
    return f"last_day(add_months({base}, {shift} + {months_per - 1}))"


def _format_spec(argument: str, function: str) -> tuple[str, int | None, int | None]:
    text = argument.strip()
    match = _FORMAT_NAME.fullmatch(text)
    if match is None:
        raise DateFunctionError(
            f"{function} requires a format or informat name such as DATE9. or 8.2."
        )
    name, width, decimals = match.groups()
    return (
        name.upper(),
        int(width) if width else None,
        int(decimals) if decimals else None,
    )


def render_format(value: str, spec: str) -> str:
    name, width, decimals = _format_spec(spec, "PUT")
    key = f"{name}{width}" if width is not None else name
    if key in _DATE_FORMATS:
        pattern, upper = _DATE_FORMATS[key]
        rendered = f"date_format({_as_date(value)}, '{pattern}')"
        return f"upper({rendered})" if upper else rendered
    if name == "Z" and width and decimals is None:
        return (
            f"lpad(CAST(CAST(round({value}, 0) AS BIGINT) AS STRING), {width}, '0')"
        )
    if name == "" and width:
        places = decimals or 0
        text = f"CAST(CAST(round({value}, {places}) AS DECIMAL(38, {places})) AS STRING)"
        return f"lpad(coalesce({text}, '.'), {width}, ' ')"
    raise DateFunctionError(
        f"Format {spec.strip()!r} is unsupported; supported: DATE9., YYMMDD10., "
        "YYMMDDN8., MMDDYY10., DDMMYY10., Zw. and w.d."
    )


def render_informat(value: str, spec: str) -> str:
    name, width, decimals = _format_spec(spec, "INPUT")
    key = f"{name}{width}" if width is not None else name
    if key in _DATE_FORMATS:
        pattern, _ = _DATE_FORMATS[key]
        return f"to_date(trim(CAST({value} AS STRING)), '{pattern}')"
    if name == "" and width:
        return f"try_cast(trim(CAST({value} AS STRING)) AS DOUBLE)"
    raise DateFunctionError(
        f"Informat {spec.strip()!r} is unsupported; supported: DATE9., YYMMDD10., "
        "YYMMDDN8., MMDDYY10., DDMMYY10. and w.d."
    )


def render_date_function(name: str, arguments: list[str]) -> str:
    if any(not argument for argument in arguments):
        raise DateFunctionError(f"{name} arguments cannot be empty.")
    if name == "MDY":
        if len(arguments) != 3:
            raise DateFunctionError("MDY requires month, day and year.")
        month, day, year = arguments
        return (
            f"CASE WHEN ({year}) BETWEEN 1 AND 9999 AND ({month}) BETWEEN 1 AND 12 "
            f"THEN CASE WHEN ({day}) >= 1 AND ({day}) <= "
            f"day(last_day(make_date({year}, {month}, 1))) "
            f"THEN make_date({year}, {month}, {day}) END END"
        )
    if name == "INTCK":
        return _intck(arguments)
    if name == "INTNX":
        return _intnx(arguments)
    if name == "DATDIF":
        if len(arguments) != 3:
            raise DateFunctionError("DATDIF requires start, end and a basis.")
        basis = _literal(arguments[2], "DATDIF basis").upper()
        if basis not in {"ACT/ACT", "ACTUAL"}:
            raise DateFunctionError("DATDIF supports only the ACT/ACT basis.")
        return f"datediff({_as_date(arguments[1])}, {_as_date(arguments[0])})"
    if name in {"QTR", "WEEKDAY"}:
        if len(arguments) != 1:
            raise DateFunctionError(f"{name} requires exactly one argument.")
        function = "quarter" if name == "QTR" else "dayofweek"
        return f"{function}({_as_date(arguments[0])})"
    if name in {"PUT", "INPUT"}:
        if len(arguments) != 2:
            raise DateFunctionError(f"{name} requires a value and a format.")
        if name == "PUT":
            if has_format(arguments[1]):
                try:
                    return render_user_format(arguments[0], arguments[1])
                except FormatError as error:
                    raise DateFunctionError(str(error)) from error
            return render_format(arguments[0], arguments[1])
        return render_informat(arguments[0], arguments[1])
    raise AssertionError(f"Unknown date function: {name}")
