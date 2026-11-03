"""Bounded SAS macro expansion for supported compile-time constructs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import (
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_UP,
    Decimal,
    InvalidOperation,
)
from typing import NoReturn


@dataclass(frozen=True)
class MacroStatement:
    text: str
    line: int


@dataclass(frozen=True)
class _Parameter:
    name: str
    default: str | None


@dataclass(frozen=True)
class _Macro:
    name: str
    parameters: tuple[_Parameter, ...]
    body: tuple[MacroStatement, ...]
    line: int


class MacroExpansionError(ValueError):
    def __init__(self, message: str, statement: MacroStatement) -> None:
        self.message = message
        self.line = statement.line
        self.statement = statement.text
        super().__init__(message)


_MACRO_HEADER = re.compile(
    r"%macro\s+([A-Za-z_]\w*)\s*(?:\((.*)\))?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_MACRO_END = re.compile(r"%mend(?:\s+[A-Za-z_]\w*)?\s*$", re.IGNORECASE)
_MACRO_LET = re.compile(r"%let\s+([A-Za-z_]\w*)\s*=\s*(.*)\Z", re.I | re.S)
_MACRO_IF = re.compile(r"%if\s+(.+?)\s+%then(?:\s+(.*))?\Z", re.I | re.S)
_MACRO_LOOP = re.compile(
    r"%do\s+([A-Za-z_]\w*)\s*=\s*([^\s]+)\s+%to\s+([^\s]+)"
    r"(?:\s+%by\s+([^\s]+))?\s*$",
    re.I,
)
_MACRO_CALL = re.compile(r"%([A-Za-z_]\w*)\s*\(")
_MACRO_WHILE = re.compile(r"%do\s+%(while|until)\s*\((.*)\)\s*$", re.I | re.S)
_MACRO_FUNCTIONS = {"eval", "sysfunc", "upcase", "lowcase", "str", "nrstr"}
_MACRO_SCOPE = re.compile(
    r"%(global|local)\s+([A-Za-z_]\w*(?:\s+[A-Za-z_]\w*)*)\Z", re.I
)
_MAX_DEPTH = 32
_MAX_EXPANDED_STATEMENTS = 10_000
_MAX_LOOP_ITERATIONS = 1_000
_MAX_INDIRECTION = 10


class _ConditionParser:
    _TOKEN = re.compile(
        r"\s*(>=|<=|!=|~=|\^=|==|=|>|<|\(|\)|"
        r"\b(?:eq|ne|gt|ge|lt|le|and|or|not)\b|"
        r"\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*'|[^\s()]+)",
        re.IGNORECASE,
    )
    _COMPARISONS = {
        "=": lambda left, right: left == right,
        "==": lambda left, right: left == right,
        "eq": lambda left, right: left == right,
        "!=": lambda left, right: left != right,
        "~=": lambda left, right: left != right,
        "^=": lambda left, right: left != right,
        "ne": lambda left, right: left != right,
        ">": lambda left, right: left > right,
        "gt": lambda left, right: left > right,
        ">=": lambda left, right: left >= right,
        "ge": lambda left, right: left >= right,
        "<": lambda left, right: left < right,
        "lt": lambda left, right: left < right,
        "<=": lambda left, right: left <= right,
        "le": lambda left, right: left <= right,
    }

    def __init__(self, expression: str, statement: MacroStatement) -> None:
        self.statement = statement
        self.tokens = []
        position = 0
        while position < len(expression):
            match = self._TOKEN.match(expression, position)
            if match is None:
                self._error("Unsupported token in %IF expression.")
            self.tokens.append(match.group(1))
            position = match.end()
        self.index = 0

    def evaluate(self) -> bool:
        if not self.tokens:
            self._error("%IF requires a condition.")
        result = self._parse_or()
        if self.index != len(self.tokens):
            self._error("Unsupported or incomplete %IF expression.")
        return result

    def _parse_or(self) -> bool:
        value = self._parse_and()
        while self._accept("or"):
            right = self._parse_and()
            value = value or right
        return value

    def _parse_and(self) -> bool:
        value = self._parse_not()
        while self._accept("and"):
            right = self._parse_not()
            value = value and right
        return value

    def _parse_not(self) -> bool:
        if self._accept("not"):
            return not self._parse_not()
        if self._accept("("):
            value = self._parse_or()
            if not self._accept(")"):
                self._error("Unclosed parenthesis in %IF expression.")
            return value
        return self._parse_comparison()

    def _parse_comparison(self) -> bool:
        left = self._value(self._next())
        if self.index >= len(self.tokens):
            return self._truthy(left)
        operator = self.tokens[self.index].casefold()
        if operator not in self._COMPARISONS:
            self._error("Expected a comparison operator in %IF expression.")
        self.index += 1
        right = self._value(self._next())
        try:
            left_number = Decimal(left)
            right_number = Decimal(right)
        except InvalidOperation:
            left_value: Decimal | str = left.casefold()
            right_value: Decimal | str = right.casefold()
        else:
            left_value = left_number
            right_value = right_number
        return self._COMPARISONS[operator](left_value, right_value)

    def _value(self, token: str) -> str:
        if len(token) >= 2 and token[0] == token[-1] and token[0] in {"'", '"'}:
            quote = token[0]
            return token[1:-1].replace(quote * 2, quote)
        return token

    @staticmethod
    def _truthy(value: str) -> bool:
        try:
            return Decimal(value) != 0
        except InvalidOperation:
            return bool(value)

    def _accept(self, expected: str) -> bool:
        if (
            self.index < len(self.tokens)
            and self.tokens[self.index].casefold() == expected
        ):
            self.index += 1
            return True
        return False

    def _next(self) -> str:
        if self.index >= len(self.tokens):
            self._error("Incomplete %IF expression.")
        token = self.tokens[self.index]
        self.index += 1
        return token

    def _error(self, message: str) -> NoReturn:
        raise MacroExpansionError(message, self.statement)


class _EvalParser:
    """Integer-only %EVAL expression evaluator."""

    _TOKEN = re.compile(
        r"\s*(>=|<=|<>|!=|~=|\^=|==|=|>|<|\+|-|\*|/|\(|\)|"
        r"\b(?:eq|ne|gt|ge|lt|le|and|or|not)\b|&|\||\d+(?:\.\d+)?|[^\s()]+)",
        re.IGNORECASE,
    )
    _COMPARE = {
        "=": lambda a, b: a == b,
        "==": lambda a, b: a == b,
        "eq": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "~=": lambda a, b: a != b,
        "^=": lambda a, b: a != b,
        "<>": lambda a, b: a != b,
        "ne": lambda a, b: a != b,
        ">": lambda a, b: a > b,
        "gt": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "ge": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "lt": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        "le": lambda a, b: a <= b,
    }

    def __init__(self, expression: str, statement: MacroStatement) -> None:
        self.statement = statement
        self.tokens: list[str] = []
        position = 0
        expression = expression.strip()
        while position < len(expression):
            match = self._TOKEN.match(expression, position)
            if match is None:
                self._error("Unsupported token in %EVAL expression.")
            self.tokens.append(match.group(1))
            position = match.end()
        self.index = 0

    def evaluate(self) -> int:
        if not self.tokens:
            self._error("%EVAL requires an expression.")
        value = self._or()
        if self.index != len(self.tokens):
            self._error("Unsupported or incomplete %EVAL expression.")
        return value

    def _peek(self) -> str | None:
        return self.tokens[self.index].casefold() if self.index < len(self.tokens) else None

    def _or(self) -> int:
        value = self._and()
        while self._peek() in {"or", "|"}:
            self.index += 1
            right = self._and()
            value = int(bool(value) or bool(right))
        return value

    def _and(self) -> int:
        value = self._not()
        while self._peek() in {"and", "&"}:
            self.index += 1
            right = self._not()
            value = int(bool(value) and bool(right))
        return value

    def _not(self) -> int:
        if self._peek() == "not":
            self.index += 1
            return int(not self._not())
        return self._comparison()

    def _comparison(self) -> int:
        left = self._sum()
        operator = self._peek()
        if operator in self._COMPARE:
            self.index += 1
            right = self._sum()
            return int(self._COMPARE[operator](left, right))
        return left

    def _sum(self) -> int:
        value = self._product()
        while self._peek() in {"+", "-"}:
            operator = self.tokens[self.index]
            self.index += 1
            right = self._product()
            value = value + right if operator == "+" else value - right
        return value

    def _product(self) -> int:
        value = self._unary()
        while self._peek() in {"*", "/"}:
            operator = self.tokens[self.index]
            self.index += 1
            right = self._unary()
            if operator == "*":
                value *= right
            else:
                if right == 0:
                    self._error("Division by zero in %EVAL expression.")
                value = abs(value) // abs(right) * (1 if (value >= 0) == (right > 0) else -1)
        return value

    def _unary(self) -> int:
        if self._peek() in {"+", "-"}:
            sign = self.tokens[self.index]
            self.index += 1
            value = self._unary()
            return -value if sign == "-" else value
        if self._peek() == "(":
            self.index += 1
            value = self._or()
            if self._peek() != ")":
                self._error("Unclosed parenthesis in %EVAL expression.")
            self.index += 1
            return value
        if self.index >= len(self.tokens):
            self._error("Incomplete %EVAL expression.")
        token = self.tokens[self.index]
        if not token.isdigit():
            self._error(
                f"%EVAL supports integer arithmetic only; found {token!r}."
            )
        self.index += 1
        return int(token)

    def _error(self, message: str) -> NoReturn:
        raise MacroExpansionError(message, self.statement)


def _format_number(value: Decimal) -> str:
    if value == value.to_integral_value():
        return str(int(value))
    return format(value.normalize(), "f")


def _sysfunc(call: str, statement: MacroStatement) -> str:
    match = re.fullmatch(r"\s*([A-Za-z_]\w*)\s*\((.*)\)\s*", call, re.S)
    if match is None:
        raise MacroExpansionError(
            "%SYSFUNC requires a function call such as %SYSFUNC(upcase(text)).",
            statement,
        )
    name = match.group(1).casefold()
    arguments = [item.strip() for item in _split_top_level(match.group(2), statement)]

    def number(text: str) -> Decimal:
        try:
            return Decimal(text)
        except InvalidOperation as error:
            raise MacroExpansionError(
                f"%SYSFUNC({name}) requires numeric arguments; found {text!r}.",
                statement,
            ) from error

    text_functions = {
        "upcase": lambda value: value.upper(),
        "lowcase": lambda value: value.lower(),
        "strip": lambda value: value.strip(),
        "trim": lambda value: value.rstrip(),
        "left": lambda value: value.lstrip(),
    }
    if name in text_functions and len(arguments) == 1:
        return text_functions[name](arguments[0])
    if name == "length" and len(arguments) == 1:
        return str(len(arguments[0]))
    numeric_unary = {
        "abs": abs,
        "int": lambda value: value.to_integral_value(rounding=ROUND_DOWN),
        "ceil": lambda value: value.to_integral_value(rounding=ROUND_CEILING),
        "floor": lambda value: value.to_integral_value(rounding=ROUND_FLOOR),
    }
    if name in numeric_unary and len(arguments) == 1:
        return _format_number(numeric_unary[name](number(arguments[0])))
    if name == "round" and len(arguments) in {1, 2}:
        value = number(arguments[0])
        unit = number(arguments[1]) if len(arguments) == 2 else Decimal(1)
        if unit <= 0:
            raise MacroExpansionError("%SYSFUNC(round) requires a positive unit.", statement)
        return _format_number(
            (value / unit).to_integral_value(rounding=ROUND_HALF_UP) * unit
        )
    if name in {"min", "max"} and arguments:
        values = [number(item) for item in arguments]
        return _format_number(min(values) if name == "min" else max(values))
    if name == "mod" and len(arguments) == 2:
        divisor = number(arguments[1])
        if divisor == 0:
            raise MacroExpansionError("%SYSFUNC(mod) divisor cannot be zero.", statement)
        return _format_number(number(arguments[0]) % divisor)
    raise MacroExpansionError(
        f"%SYSFUNC({name}) is unsupported or has the wrong number of arguments; "
        "supported: upcase, lowcase, strip, trim, left, length, abs, int, ceil, "
        "floor, round, min, max, mod.",
        statement,
    )


def _split_top_level(arguments: str, statement: MacroStatement) -> list[str]:
    values: list[str] = []
    current: list[str] = []
    quote: str | None = None
    nesting = 0
    for char in arguments:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
        elif char in {"'", '"'}:
            quote = char
            current.append(char)
        elif char == "(":
            nesting += 1
            current.append(char)
        elif char == ")":
            nesting -= 1
            current.append(char)
        elif char == "," and nesting == 0:
            values.append("".join(current))
            current = []
        else:
            current.append(char)
    if quote or nesting:
        raise MacroExpansionError("Unterminated %SYSFUNC argument.", statement)
    values.append("".join(current))
    return values


class _MacroProcessor:
    def __init__(self, statements: list[MacroStatement]) -> None:
        self.macros: dict[str, _Macro] = {}
        self.statement_count = 0
        self.global_names: set[str] = set()
        self.local_names: list[set[str]] = [set()]
        self.statements = self._extract_definitions(statements)

    def expand(self) -> list[MacroStatement]:
        return self._process(self.statements, {}, (), 0)

    def _extract_definitions(
        self, statements: list[MacroStatement]
    ) -> list[MacroStatement]:
        output: list[MacroStatement] = []
        index = 0
        while index < len(statements):
            statement = statements[index]
            if not statement.text.casefold().startswith("%macro"):
                output.append(statement)
                index += 1
                continue
            match = _MACRO_HEADER.fullmatch(statement.text.strip())
            if match is None:
                raise MacroExpansionError("Malformed %MACRO definition.", statement)
            name, raw_parameters = match.groups()
            key = name.casefold()
            if key in self.macros:
                raise MacroExpansionError(f"Macro {name!r} is defined more than once.", statement)
            parameters = self._parse_parameters(raw_parameters or "", statement)
            body: list[MacroStatement] = []
            index += 1
            while index < len(statements) and not _MACRO_END.fullmatch(
                statements[index].text.strip()
            ):
                if statements[index].text.casefold().startswith("%macro"):
                    raise MacroExpansionError("Nested %MACRO definitions are unsupported.", statements[index])
                body.append(statements[index])
                index += 1
            if index >= len(statements):
                raise MacroExpansionError(f"Macro {name!r} is missing %MEND.", statement)
            self.macros[key] = _Macro(
                name,
                parameters,
                tuple(body),
                statement.line,
            )
            index += 1
        return output

    def _parse_parameters(
        self, raw_parameters: str, statement: MacroStatement
    ) -> tuple[_Parameter, ...]:
        if not raw_parameters.strip():
            return ()
        parameters: list[_Parameter] = []
        seen: set[str] = set()
        for raw in self._split_arguments(raw_parameters, statement):
            name, separator, default = raw.partition("=")
            name = name.strip()
            if not re.fullmatch(r"[A-Za-z_]\w*", name):
                raise MacroExpansionError("Malformed %MACRO parameter.", statement)
            key = name.casefold()
            if key in seen:
                raise MacroExpansionError(f"Duplicate macro parameter {name!r}.", statement)
            seen.add(key)
            parameters.append(
                _Parameter(name, default.strip() if separator else None)
            )
        return tuple(parameters)

    def _process(
        self,
        statements: list[MacroStatement] | tuple[MacroStatement, ...],
        variables: dict[str, str],
        call_stack: tuple[str, ...],
        depth: int,
    ) -> list[MacroStatement]:
        if depth > _MAX_DEPTH:
            location = statements[0] if statements else MacroStatement("", 1)
            raise MacroExpansionError("Macro expansion exceeded the nesting limit.", location)
        output: list[MacroStatement] = []
        index = 0
        while index < len(statements):
            statement = statements[index]
            text = statement.text.strip()
            lower = text.casefold()

            if lower.startswith(("%global", "%local")):
                scope = _MACRO_SCOPE.fullmatch(text)
                if scope is None:
                    raise MacroExpansionError(
                        "Expected %GLOBAL or %LOCAL followed by variable names.", statement
                    )
                for name in scope.group(2).split():
                    key = name.casefold()
                    if scope.group(1).casefold() == "global":
                        self.global_names.add(key)
                        variables.setdefault(key, "")
                    else:
                        self.local_names[-1].add(key)
                        variables[key] = ""
                index += 1
                continue

            if lower.startswith("%let"):
                match = _MACRO_LET.fullmatch(text)
                if match is None:
                    raise MacroExpansionError(
                        "Expected %LET name=value with a simple value.", statement
                    )
                name, value = match.groups()
                if self._has_unsupported_macro_text(value):
                    raise MacroExpansionError(
                        "Macro calls and macro-language statements in %LET "
                        "values are unsupported; only %EVAL and %SYSFUNC are allowed.",
                        statement,
                    )
                variables[name.casefold()] = self._substitute(value, variables, statement)
                index += 1
                continue

            if lower.startswith("%if"):
                match = _MACRO_IF.fullmatch(text)
                if match is None:
                    raise MacroExpansionError("Malformed %IF/%THEN statement.", statement)
                expression, inline_action = match.groups()
                condition = _ConditionParser(
                    self._substitute(expression, variables, statement), statement
                ).evaluate()
                if inline_action and inline_action.casefold() == "%do":
                    true_body, after_true = self._read_block(statements, index + 1, statement)
                    false_body, index = self._read_else(statements, after_true)
                elif inline_action:
                    true_body = (MacroStatement(inline_action, statement.line),)
                    index += 1
                    false_body, index = self._read_else(statements, index)
                else:
                    if index + 1 >= len(statements):
                        raise MacroExpansionError("%IF is missing its %THEN action.", statement)
                    true_body = (statements[index + 1],)
                    false_body = ()
                    index += 2
                    false_body_statement = self._statement_at(statements, index)
                    if self._starts_else(false_body_statement):
                        assert false_body_statement is not None
                        false_body, index = self._read_else_action(
                            statements, index, false_body_statement
                        )
                selected = true_body if condition else false_body
                output.extend(self._process(selected, variables, call_stack, depth + 1))
                continue

            if lower == "%do" or _MACRO_LOOP.fullmatch(text) or _MACRO_WHILE.fullmatch(text):
                body, end_index = self._read_block(statements, index + 1, statement)
                loop_match = _MACRO_LOOP.fullmatch(text)
                while_match = _MACRO_WHILE.fullmatch(text)
                if while_match is not None:
                    kind, raw_condition = while_match.groups()
                    iterations = 0
                    while True:
                        if kind.casefold() == "while" and not _ConditionParser(
                            self._substitute(raw_condition, variables, statement),
                            statement,
                        ).evaluate():
                            break
                        iterations += 1
                        if iterations > _MAX_LOOP_ITERATIONS:
                            raise MacroExpansionError(
                                f"%DO %{kind.upper()} exceeds the "
                                f"{_MAX_LOOP_ITERATIONS}-iteration limit.",
                                statement,
                            )
                        output.extend(self._process(body, variables, call_stack, depth + 1))
                        if kind.casefold() == "until" and _ConditionParser(
                            self._substitute(raw_condition, variables, statement),
                            statement,
                        ).evaluate():
                            break
                elif loop_match is None:
                    output.extend(self._process(body, variables, call_stack, depth + 1))
                else:
                    variable, start, stop, step = loop_match.groups()
                    try:
                        first = int(self._substitute(start, variables, statement))
                        last = int(self._substitute(stop, variables, statement))
                        increment = int(
                            self._substitute(step, variables, statement)
                            if step is not None
                            else "1"
                        )
                    except MacroExpansionError:
                        raise
                    except ValueError as error:
                        raise MacroExpansionError(
                            "%DO loop bounds and step must expand to integer literals.",
                            statement,
                        ) from error
                    if increment == 0:
                        raise MacroExpansionError("%DO loop step cannot be zero.", statement)
                    values = range(first, last + (1 if increment > 0 else -1), increment)
                    if len(values) > _MAX_LOOP_ITERATIONS:
                        raise MacroExpansionError(
                            f"%DO loop exceeds the {_MAX_LOOP_ITERATIONS}-iteration limit.",
                            statement,
                        )
                    for value in values:
                        variables[variable.casefold()] = str(value)
                        output.extend(
                            self._process(body, variables, call_stack, depth + 1)
                        )
                index = end_index
                continue

            if lower.startswith("%else") or lower == "%end":
                raise MacroExpansionError("Unexpected %ELSE or %END statement.", statement)

            call = self._standalone_call(text, statement)
            if call is not None:
                name, arguments = call
                output.extend(
                    self._invoke(name, arguments, variables, call_stack, depth, statement)
                )
                index += 1
                continue

            expanded = self._expand_inline(text, variables, call_stack, depth, statement)
            output.extend(expanded)
            self.statement_count += len(expanded)
            if self.statement_count > _MAX_EXPANDED_STATEMENTS:
                raise MacroExpansionError(
                    f"Macro expansion exceeded the {_MAX_EXPANDED_STATEMENTS}-statement limit.",
                    statement,
                )
            index += 1
        return output

    def _read_block(
        self,
        statements: list[MacroStatement] | tuple[MacroStatement, ...],
        start: int,
        opening: MacroStatement,
    ) -> tuple[tuple[MacroStatement, ...], int]:
        body: list[MacroStatement] = []
        nesting = 1
        index = start
        while index < len(statements):
            text = statements[index].text.strip()
            if self._opens_block(text):
                nesting += 1
            elif text.casefold() == "%end":
                nesting -= 1
                if nesting == 0:
                    return tuple(body), index + 1
            body.append(statements[index])
            index += 1
        raise MacroExpansionError("%DO block is missing %END.", opening)

    @staticmethod
    def _opens_block(text: str) -> bool:
        lower = text.casefold()
        if lower == "%do" or _MACRO_LOOP.fullmatch(text) or _MACRO_WHILE.fullmatch(text):
            return True
        if lower.startswith("%else") and lower[len("%else") :].strip() == "%do":
            return True
        match = _MACRO_IF.fullmatch(text)
        return bool(match and match.group(2) and match.group(2).casefold() == "%do")

    def _read_else(
        self,
        statements: list[MacroStatement] | tuple[MacroStatement, ...],
        index: int,
    ) -> tuple[tuple[MacroStatement, ...], int]:
        statement = self._statement_at(statements, index)
        if statement is None or not self._starts_else(statement):
            return (), index
        action = statement.text.strip()[len("%else") :].strip()
        if action.casefold() == "%do":
            return self._read_block(statements, index + 1, statement)
        if action:
            return (MacroStatement(action, statement.line),), index + 1
        if index + 1 >= len(statements):
            raise MacroExpansionError("%ELSE is missing its action.", statement)
        return (statements[index + 1],), index + 2

    def _read_else_action(
        self,
        statements: list[MacroStatement] | tuple[MacroStatement, ...],
        index: int,
        statement: MacroStatement,
    ) -> tuple[tuple[MacroStatement, ...], int]:
        action = statement.text.strip()[len("%else") :].strip()
        if action:
            return (MacroStatement(action, statement.line),), index + 1
        if index + 1 >= len(statements):
            raise MacroExpansionError("%ELSE is missing its action.", statement)
        return (statements[index + 1],), index + 2

    @staticmethod
    def _starts_else(statement: MacroStatement | None) -> bool:
        return statement is not None and statement.text.strip().casefold().startswith("%else")

    @staticmethod
    def _statement_at(
        statements: list[MacroStatement] | tuple[MacroStatement, ...], index: int
    ) -> MacroStatement | None:
        return statements[index] if index < len(statements) else None

    def _standalone_call(
        self, text: str, statement: MacroStatement
    ) -> tuple[str, tuple[str, ...]] | None:
        match = re.fullmatch(r"%([A-Za-z_]\w*)\s*(?:\((.*)\))?", text, re.S)
        if match is None:
            return None
        name = match.group(1).casefold()
        if name not in self.macros:
            return None
        raw_arguments = match.group(2) or ""
        return name, tuple(self._split_arguments(raw_arguments, statement))

    def _invoke(
        self,
        name: str,
        arguments: tuple[str, ...],
        variables: dict[str, str],
        call_stack: tuple[str, ...],
        depth: int,
        statement: MacroStatement,
    ) -> list[MacroStatement]:
        if name not in self.macros:
            raise MacroExpansionError(f"Undefined macro %{name}.", statement)
        if name in call_stack:
            raise MacroExpansionError(f"Recursive macro call %{name} is unsupported.", statement)
        macro = self.macros[name]
        local_variables = dict(variables)
        positional: list[str] = []
        named: dict[str, str] = {}
        for argument in arguments:
            key, separator, value = argument.partition("=")
            if separator and re.fullmatch(r"\s*[A-Za-z_]\w*\s*", key):
                normalized = key.strip().casefold()
                if normalized in named:
                    raise MacroExpansionError(
                        f"Macro parameter {key.strip()!r} is specified more than once.",
                        statement,
                    )
                named[normalized] = value.strip()
            else:
                if named:
                    raise MacroExpansionError(
                        "Positional macro parameters must precede named parameters.",
                        statement,
                    )
                positional.append(argument.strip())
        if len(positional) > len(macro.parameters):
            raise MacroExpansionError(f"Too many arguments for macro %{name}.", statement)
        parameter_names = {item.name.casefold() for item in macro.parameters}
        unknown = set(named).difference(parameter_names)
        if unknown:
            raise MacroExpansionError(
                f"Unknown parameter {sorted(unknown)[0]!r} for macro %{name}.",
                statement,
            )
        for index, parameter in enumerate(macro.parameters):
            if index < len(positional):
                value = positional[index]
            elif parameter.name.casefold() in named:
                value = named[parameter.name.casefold()]
            elif parameter.default is not None:
                value = self._substitute(parameter.default, local_variables, statement)
            else:
                raise MacroExpansionError(
                    f"Missing required parameter {parameter.name!r} for macro %{name}.",
                    statement,
                )
            if self._has_unsupported_macro_text(value):
                raise MacroExpansionError("Macro calls inside arguments are unsupported.", statement)
            local_variables[parameter.name.casefold()] = self._substitute(
                value, local_variables, statement
            )
        self.local_names.append(set())
        try:
            result = self._process(
                macro.body,
                local_variables,
                (*call_stack, name),
                depth + 1,
            )
        finally:
            declared_local = self.local_names.pop()
        for key in self.global_names - declared_local:
            if key in local_variables:
                variables[key] = local_variables[key]
        return result

    def _expand_inline(
        self,
        text: str,
        variables: dict[str, str],
        call_stack: tuple[str, ...],
        depth: int,
        statement: MacroStatement,
    ) -> list[MacroStatement]:
        expanded = self._substitute(text, variables, statement)
        matches = list(_MACRO_CALL.finditer(expanded))
        for match in reversed(matches):
            name = match.group(1).casefold()
            if name not in self.macros:
                raise MacroExpansionError(f"Undefined macro %{match.group(1)}.", statement)
            opening = expanded.find("(", match.start())
            closing = self._matching_paren(expanded, opening, statement)
            arguments = tuple(
                self._split_arguments(expanded[opening + 1 : closing], statement)
            )
            result = self._invoke(name, arguments, variables, call_stack, depth, statement)
            if len(result) != 1:
                raise MacroExpansionError(
                    "A macro used inside a SAS statement must expand to exactly one statement.",
                    statement,
                )
            expanded = expanded[: match.start()] + result[0].text + expanded[closing + 1 :]
        return [MacroStatement(expanded, statement.line)]

    def _substitute(
        self, text: str, variables: dict[str, str], statement: MacroStatement
    ) -> str:
        for match in re.finditer(r"%([A-Za-z_]\w*)", text):
            name = match.group(1).casefold()
            following = text[match.end() :].lstrip()
            if (
                name not in self.macros and name not in _MACRO_FUNCTIONS
            ) or not following.startswith("("):
                raise MacroExpansionError(
                    "Unsupported macro function or statement in generated SAS code.",
                    statement,
                )
        return self._evaluate_functions(
            self._resolve_variables(text, variables, statement), statement
        )

    def _resolve_variables(
        self, text: str, variables: dict[str, str], statement: MacroStatement
    ) -> str:
        pattern = re.compile(r"&&|&([A-Za-z_]\w*)\.?")
        for _ in range(_MAX_INDIRECTION):
            rescan = False

            def replace(match: re.Match[str]) -> str:
                nonlocal rescan
                if match.group(0) == "&&":
                    rescan = True
                    return "&"
                name = match.group(1).casefold()
                if name not in variables:
                    raise MacroExpansionError(
                        f"Undefined macro variable &{match.group(1)}.", statement
                    )
                return variables[name]

            text = pattern.sub(replace, text)
            if not rescan:
                return text
        raise MacroExpansionError(
            "Indirect macro references exceeded the resolution limit.", statement
        )

    def _evaluate_functions(self, text: str, statement: MacroStatement) -> str:
        pattern = re.compile(r"%(eval|sysfunc|upcase|lowcase|str|nrstr)\s*\(", re.IGNORECASE)
        while True:
            matches = list(pattern.finditer(text))
            if not matches:
                return text
            match = matches[-1]
            opening = text.find("(", match.start())
            closing = self._matching_paren(text, opening, statement)
            inner = text[opening + 1 : closing]
            function = match.group(1).casefold()
            if function == "eval":
                value = str(_EvalParser(inner, statement).evaluate())
            elif function == "sysfunc":
                value = _sysfunc(inner, statement)
            elif function == "upcase":
                value = inner.strip().upper()
            elif function == "lowcase":
                value = inner.strip().lower()
            else:
                value = inner
            text = text[: match.start()] + value + text[closing + 1 :]

    @staticmethod
    def _has_unsupported_macro_text(text: str) -> bool:
        return any(
            name.casefold() not in _MACRO_FUNCTIONS
            for name in re.findall(r"%([A-Za-z_]\w*)", text)
        )

    def _split_arguments(
        self, arguments: str, statement: MacroStatement
    ) -> list[str]:
        if not arguments.strip():
            return []
        values: list[str] = []
        current: list[str] = []
        quote: str | None = None
        nesting = 0
        index = 0
        while index < len(arguments):
            char = arguments[index]
            if quote:
                current.append(char)
                if char == quote:
                    if index + 1 < len(arguments) and arguments[index + 1] == quote:
                        current.append(arguments[index + 1])
                        index += 1
                    else:
                        quote = None
            elif char in {"'", '"'}:
                quote = char
                current.append(char)
            elif char == "(":
                nesting += 1
                current.append(char)
            elif char == ")":
                nesting -= 1
                if nesting < 0:
                    raise MacroExpansionError("Unbalanced macro argument parentheses.", statement)
                current.append(char)
            elif char == "," and nesting == 0:
                values.append("".join(current).strip())
                current = []
            else:
                current.append(char)
            index += 1
        if quote or nesting:
            raise MacroExpansionError("Unterminated macro argument.", statement)
        values.append("".join(current).strip())
        return values

    def _matching_paren(
        self, text: str, opening: int, statement: MacroStatement
    ) -> int:
        quote: str | None = None
        nesting = 0
        index = opening
        while index < len(text):
            char = text[index]
            if quote:
                if char == quote:
                    if index + 1 < len(text) and text[index + 1] == quote:
                        index += 1
                    else:
                        quote = None
            elif char in {"'", '"'}:
                quote = char
            elif char == "(":
                nesting += 1
            elif char == ")":
                nesting -= 1
                if nesting == 0:
                    return index
            index += 1
        raise MacroExpansionError("Unterminated macro invocation.", statement)


def expand_macros(statements: list[MacroStatement]) -> list[MacroStatement]:
    return _MacroProcessor(statements).expand()
