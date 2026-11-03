"""Bounded SAS macro expansion for supported compile-time constructs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
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
_MAX_DEPTH = 32
_MAX_EXPANDED_STATEMENTS = 10_000
_MAX_LOOP_ITERATIONS = 1_000


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


class _MacroProcessor:
    def __init__(self, statements: list[MacroStatement]) -> None:
        self.macros: dict[str, _Macro] = {}
        self.statement_count = 0
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

            if lower.startswith("%let"):
                match = _MACRO_LET.fullmatch(text)
                if match is None:
                    raise MacroExpansionError(
                        "Expected %LET name=value with a simple value.", statement
                    )
                name, value = match.groups()
                if re.search(r"%[A-Za-z_]", value):
                    raise MacroExpansionError(
                        "Macro calls and macro-language expressions in %LET "
                        "values are unsupported.",
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

            if lower == "%do" or _MACRO_LOOP.fullmatch(text):
                body, end_index = self._read_block(statements, index + 1, statement)
                loop_match = _MACRO_LOOP.fullmatch(text)
                if loop_match is None:
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
                        local_variables = dict(variables)
                        local_variables[variable.casefold()] = str(value)
                        output.extend(
                            self._process(body, local_variables, call_stack, depth + 1)
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
        if lower == "%do" or _MACRO_LOOP.fullmatch(text):
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
            if re.search(r"%[A-Za-z_]", value):
                raise MacroExpansionError("Macro calls inside arguments are unsupported.", statement)
            local_variables[parameter.name.casefold()] = self._substitute(
                value, local_variables, statement
            )
        return self._process(
            macro.body,
            local_variables,
            (*call_stack, name),
            depth + 1,
        )

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
        if "&&" in text:
            raise MacroExpansionError(
                "Indirect or repeated ampersand macro references are unsupported.",
                statement,
            )
        for match in re.finditer(r"%([A-Za-z_]\w*)", text):
            name = match.group(1).casefold()
            following = text[match.end() :].lstrip()
            if name not in self.macros or not following.startswith("("):
                raise MacroExpansionError(
                    "Unsupported macro function or statement in generated SAS code.",
                    statement,
                )

        def replace(match: re.Match[str]) -> str:
            name = match.group(1).casefold()
            if name not in variables:
                raise MacroExpansionError(
                    f"Undefined macro variable &{match.group(1)}.", statement
                )
            return variables[name]

        return re.sub(r"&([A-Za-z_]\w*)\.?", replace, text)

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
