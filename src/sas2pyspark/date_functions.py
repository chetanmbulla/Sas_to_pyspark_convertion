"""SAS date functions, date literals and common formats/informats as Spark SQL."""

from __future__ import annotations

import re
from datetime import datetime

from .user_formats import (
    FormatError,
    has_cntlin,
    has_format,
    has_informat,
    render_user_format,
    render_user_informat,
)
DATE_FUNCTIONS = frozenset(
    {
        "MDY",
        "INTCK",
        "INTNX",
        "DATDIF",
        "QTR",
        "WEEKDAY",
        "PUT",
        "INPUT",
        "DATEPART",
        "TIMEPART",
        "DHMS",
        "DATETIME",
        "TIME",
        "DATE",
        "YEAR",
        "MONTH",
        "DAY",
    }
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
_DATETIME_LITERAL = re.compile(
    r"""(['"])(\d{1,2})([A-Za-z]{3})(\d{4}):(\d{1,2}):(\d{1,2}):(\d{1,2})\1\s*dt\b""",
    re.IGNORECASE,
)

# SAS format name -> (Spark datetime pattern, upper-case the result)
_DATE_FORMATS = {
    "DATE9": ("ddMMMyyyy", True),
    "YYMMDD10": ("yyyy-MM-dd", False),
    "YYMMDDN8": ("yyyyMMdd", False),
    "MMDDYY10": ("MM/dd/yyyy", False),
    "DDMMYY10": ("dd/MM/yyyy", False),
    "DATETIME20": ("ddMMMyyyy:HH:mm:ss", True),
    "E8601DT19": ("yyyy-MM-dd''T''HH:mm:ss", False),
}
_FORMAT_NAME = re.compile(r"(\$?[A-Za-z]+(?:\d+[A-Za-z]+)*)?(\d*)\.(\d*)")
_SAS_EPOCH = "DATE'1960-01-01'"
_SAS_EPOCH_SUNDAY = "DATE'1960-01-03'"


class DateFunctionError(ValueError):
    """Raised for unsupported or malformed date/format usage."""


def translate_date_literals(expression: str) -> str:
    def replace_datetime(match: re.Match[str]) -> str:
        day, month, year = int(match.group(2)), _MONTHS.get(match.group(3).upper()), int(match.group(4))
        hour, minute, second = int(match.group(5)), int(match.group(6)), int(match.group(7))
        try:
            if month is None:
                raise ValueError
            value = datetime(year, month, day, hour, minute, second)
        except ValueError as error:
            raise DateFunctionError(
                f"Invalid SAS datetime literal: {match.group(0)!r}."
            ) from error
        day_count = (value.date() - datetime(1960, 1, 1).date()).days
        seconds = value.hour * 3600 + value.minute * 60 + value.second
        return str(day_count * 86400 + seconds)

    def replace(match: re.Match[str]) -> str:
        day = int(match.group(2))
        month = _MONTHS.get(match.group(3).upper())
        year = int(match.group(4))
        try:
            if month is None:
                raise ValueError
            value = datetime(year, month, day)
        except ValueError as error:
            raise DateFunctionError(
                f"Invalid SAS date literal: {match.group(0)!r}."
            ) from error
        return f"datediff(DATE'{value:%Y-%m-%d}', {_SAS_EPOCH})"

    return _DATE_LITERAL.sub(replace, _DATETIME_LITERAL.sub(replace_datetime, expression))


def _literal(argument: str, purpose: str) -> str:
    text = argument.strip()
    if len(text) < 2 or text[0] != text[-1] or text[0] not in {"'", '"'}:
        raise DateFunctionError(f"{purpose} must be a quoted literal.")
    return text[1:-1].strip()


def _as_date(argument: str) -> str:
    return f"date_add({_SAS_EPOCH}, CAST(({argument}) AS INT))"


def _date_count(argument: str) -> str:
    return f"datediff({argument}, {_SAS_EPOCH})"


def _datetime_format(value: str, pattern: str) -> str:
    day_count = f"CAST(floor(({value}) / 86400) AS INT)"
    second_of_day = f"pmod(CAST(({value}) AS BIGINT), 86400)"
    date_pattern = (
        "ddMMMyyyy" if pattern == "ddMMMyyyy:HH:mm:ss" else "yyyy-MM-dd"
    )
    date = f"date_format(date_add({_SAS_EPOCH}, {day_count}), '{date_pattern}')"
    hour = f"lpad(CAST(floor({second_of_day} / 3600) AS STRING), 2, '0')"
    minute = f"lpad(CAST(floor(pmod({second_of_day}, 3600) / 60) AS STRING), 2, '0')"
    second = f"lpad(CAST(pmod({second_of_day}, 60) AS STRING), 2, '0')"
    if pattern == "ddMMMyyyy:HH:mm:ss":
        return f"concat(upper({date}), ':', {hour}, ':', {minute}, ':', {second})"
    return f"concat({date}, 'T', {hour}, ':', {minute}, ':', {second})"


def _datetime_input(value: str, pattern: str) -> str:
    text = f"trim(CAST({value} AS STRING))"
    if pattern == "ddMMMyyyy:HH:mm:ss":
        date_text = f"substring({text}, 1, 9)"
        hour_text, minute_text, second_text = (
            f"substring({text}, {start}, 2)" for start in (11, 14, 17)
        )
        date_pattern = "ddMMMyyyy"
    else:
        date_text = f"substring({text}, 1, 10)"
        hour_text, minute_text, second_text = (
            f"substring({text}, {start}, 2)" for start in (12, 15, 18)
        )
        date_pattern = "yyyy-MM-dd"
    days = _date_count(f"to_date({date_text}, '{date_pattern}')")
    return (
        f"({days} * 86400 + CAST({hour_text} AS INT) * 3600 + "
        f"CAST({minute_text} AS INT) * 60 + CAST({second_text} AS INT))"
    )


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
        return _date_count(f"date_add({date}, {count})")
    if unit == "WEEK":
        start = f"date_sub({date}, dayofweek({date}) - 1)"
        if alignment == "B":
            return _date_count(f"date_add({start}, 7 * {count})")
        if alignment == "E":
            return _date_count(f"date_add({start}, 7 * {count} + 6)")
        return _date_count(f"date_add({date}, 7 * {count})")
    months_per = {"MONTH": 1, "QTR": 3, "YEAR": 12}[unit]
    shift = f"{months_per} * {count}"
    if alignment == "S":
        return _date_count(f"add_months({date}, {shift})")
    truncation = {"MONTH": "MM", "QTR": "quarter", "YEAR": "YEAR"}[unit]
    base = f"trunc({date}, '{truncation}')"
    if alignment == "B":
        return _date_count(f"add_months({base}, {shift})")
    return _date_count(f"last_day(add_months({base}, {shift} + {months_per - 1}))")


def _format_spec(argument: str, function: str) -> tuple[str, int | None, int | None]:
    text = argument.strip()
    match = _FORMAT_NAME.fullmatch(text)
    if match is None:
        raise DateFunctionError(
            f"{function} requires a format or informat name such as DATE9. or 8.2."
        )
    name, width, decimals = match.groups()
    return (
        (name or "").upper(),
        int(width) if width else None,
        int(decimals) if decimals else None,
    )


def render_format(value: str, spec: str) -> str:
    name, width, decimals = _format_spec(spec, "PUT")
    key = f"{name}{width}" if width is not None else name
    if key in _DATE_FORMATS:
        pattern, upper = _DATE_FORMATS[key]
        date_time = name in {"DATETIME", "E8601DT"}
        rendered = (
            _datetime_format(value, pattern)
            if date_time
            else f"date_format({_as_date(value)}, '{pattern}')"
        )
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
        if name in {"DATETIME", "E8601DT"}:
            return _datetime_input(value, pattern)
        return _date_count(
            f"to_date(trim(CAST({value} AS STRING)), '{pattern}')"
        )
    if name in {"", "BEST"} and width:
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
            f"THEN datediff(make_date({year}, {month}, {day}), {_SAS_EPOCH}) END END"
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
    if name == "DATEPART":
        if len(arguments) != 1:
            raise DateFunctionError("DATEPART requires exactly one argument.")
        return f"CAST(floor(({arguments[0]}) / 86400) AS INT)"
    if name == "TIMEPART":
        if len(arguments) != 1:
            raise DateFunctionError("TIMEPART requires exactly one argument.")
        return f"pmod(CAST(({arguments[0]}) AS BIGINT), 86400)"
    if name == "DHMS":
        if len(arguments) != 4:
            raise DateFunctionError("DHMS requires a date, hour, minute and second.")
        date, hour, minute, second = arguments
        return f"(({date}) * 86400 + ({hour}) * 3600 + ({minute}) * 60 + ({second}))"
    if name == "DATETIME":
        if arguments:
            raise DateFunctionError("DATETIME takes no arguments.")
        return (
            f"(datediff(current_date(), {_SAS_EPOCH}) * 86400 + "
            "hour(current_timestamp()) * 3600 + minute(current_timestamp()) * 60 + "
            "second(current_timestamp()))"
        )
    if name == "TIME":
        if arguments:
            raise DateFunctionError("TIME takes no arguments.")
        return "CAST(date_format(current_timestamp(), 'HH') AS INT) * 3600 + CAST(date_format(current_timestamp(), 'mm') AS INT) * 60 + CAST(date_format(current_timestamp(), 'ss') AS INT)"
    if name == "DATE":
        if arguments:
            raise DateFunctionError("DATE takes no arguments.")
        return f"datediff(current_date(), {_SAS_EPOCH})"
    if name in {"YEAR", "MONTH", "DAY"}:
        if len(arguments) != 1:
            raise DateFunctionError(f"{name} requires exactly one argument.")
        return f"{name.lower()}({_as_date(arguments[0])})"
    if name in {"PUT", "INPUT"}:
        if len(arguments) != 2:
            raise DateFunctionError(f"{name} requires a value and a format.")
        if name == "PUT":
            if has_format(arguments[1]):
                try:
                    return render_user_format(arguments[0], arguments[1])
                except FormatError as error:
                    raise DateFunctionError(str(error)) from error
            try:
                return render_format(arguments[0], arguments[1])
            except DateFunctionError:
                if has_cntlin():
                    try:
                        return render_user_format(arguments[0], arguments[1])
                    except FormatError as error:
                        raise DateFunctionError(str(error)) from error
                raise
        if has_informat(arguments[1]):
            try:
                return render_user_informat(arguments[0], arguments[1])
            except FormatError as error:
                raise DateFunctionError(str(error)) from error
        try:
            return render_informat(arguments[0], arguments[1])
        except DateFunctionError:
            if has_cntlin():
                try:
                    return render_user_informat(arguments[0], arguments[1])
                except FormatError as error:
                    raise DateFunctionError(str(error)) from error
            raise
    raise AssertionError(f"Unknown date function: {name}")
