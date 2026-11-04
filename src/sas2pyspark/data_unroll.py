"""Resolve DATA-step arrays and iterative DO loops at conversion time.

* ``ARRAY a{*} ...``, multi-dimensional arrays, ``$`` character arrays and implicit
  element lists are normalised to the one-dimensional form the converter supports.
* ``DIM``/``HBOUND``/``LBOUND`` of a declared array become integer literals.
* Constant array indexes (including simple integer arithmetic) become literals;
  two-dimensional indexes are flattened in row-major order.
* Iterative ``DO i = a TO b [BY c]`` loops without ``OUTPUT`` are unrolled when the
  bounds are constants or ``DIM(array)``. ``OUTPUT`` loops keep the dedicated
  converter path. ``DO WHILE``/``DO UNTIL`` depend on run-time values and are rejected.
"""

from __future__ import annotations

import re

from .macro_processor import MacroExpansionError, MacroStatement

_DATA = re.compile(r"data\s+((?:\w+\.)?\w+)", re.I)
_QUOTED = re.compile(r"('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\")")
_ARRAY = re.compile(
    r"array\s+([A-Za-z_]\w*)\s*[\[{]\s*([^\]}]*?)\s*[\]}]\s*(.*)", re.I | re.S
)
_LOOP = re.compile(
    r"do\s+([A-Za-z_]\w*)\s*=\s*(.+?)\s+to\s+(.+?)(?:\s+by\s+(.+))?", re.I | re.S
)
_MAX_STATEMENTS = 5000


def rewrite_arrays_and_loops(statements: list[MacroStatement]) -> list[MacroStatement]:
    result: list[MacroStatement] = []
    index = 0
    while index < len(statements):
        statement = statements[index]
        if _DATA.fullmatch(statement.text.strip()) is None:
            result.append(statement)
            index += 1
            continue
        end = index + 1
        while end < len(statements) and statements[end].text.strip().casefold() not in {
            "run",
            "quit",
        }:
            end += 1
        result.append(statement)
        result.extend(_Step().process(list(statements[index + 1 : end])))
        if end < len(statements):
            result.append(statements[end])
        index = end + 1
    return result


class _Step:
    def __init__(self) -> None:
        self.arrays: dict[str, tuple[int, ...]] = {}
        self.emitted = 0

    def process(self, statements: list[MacroStatement]) -> list[MacroStatement]:
        output: list[MacroStatement] = []
        index = 0
        while index < len(statements):
            item = statements[index]
            text = item.text.strip()
            if re.match(r"array\s", text, re.I):
                output.append(self._declare(item))
                index += 1
                continue
            folded = MacroStatement(self._fold(text, item), item.line)
            text = folded.text.strip()
            if re.match(r"do\s+(?:while|until)\b", text, re.I):
                raise MacroExpansionError(
                    "DO WHILE and DO UNTIL loops depend on run-time values and cannot "
                    "be converted statically; rewrite them as an indexed DO loop.",
                    item,
                )
            header = _LOOP.fullmatch(text)
            if header is None:
                output.append(folded)
                index += 1
                continue
            end = _loop_end(statements, index)
            if end is None:
                output.append(folded)
                index += 1
                continue
            body = statements[index + 1 : end]
            if any(_has_output(part.text) for part in body):
                output.extend(self._folded(part) for part in statements[index : end + 1])
                index = end + 1
                continue
            output.extend(self._unroll(header, body, folded))
            index = end + 1
        return output

    def _folded(self, item: MacroStatement) -> MacroStatement:
        return MacroStatement(self._fold(item.text.strip(), item), item.line)

    def _declare(self, item: MacroStatement) -> MacroStatement:
        match = _ARRAY.fullmatch(item.text.strip())
        if match is None or "(" in match.group(3) or "_temporary_" in match.group(3).casefold():
            return item
        name, spec, rest = match.group(1), match.group(2), match.group(3)
        rest = re.sub(r"^\$\s*\d*\s*", "", rest.strip())
        dims: list[int] = []
        wildcard = False
        for part in (piece.strip() for piece in spec.split(",")):
            if part == "*":
                wildcard = True
            elif part.isdigit() and int(part) >= 1:
                dims.append(int(part))
            else:
                return item
        elements = _expand_elements(rest, item) if rest else []
        if wildcard:
            if len(spec.split(",")) != 1 or not elements:
                raise MacroExpansionError(
                    "ARRAY {*} requires an explicit element list and one dimension.", item
                )
            dims = [len(elements)]
        total = 1
        for size in dims:
            total *= size
        if not elements:
            elements = [f"{name}{number}" for number in range(1, total + 1)]
        if len(elements) != total:
            raise MacroExpansionError(
                f"ARRAY {name!r} declares size {total} but lists {len(elements)} elements.",
                item,
            )
        self.arrays[name.casefold()] = tuple(dims)
        return MacroStatement(f"array {name}[{total}] {' '.join(elements)}", item.line)

    def _fold(self, text: str, item: MacroStatement) -> str:
        if not self.arrays:
            return text
        parts = _QUOTED.split(text)
        for position in range(0, len(parts), 2):
            segment = parts[position]
            for name, dims in self.arrays.items():
                segment = re.sub(
                    rf"\b(?:dim|hbound)\s*\(\s*{re.escape(name)}\s*\)",
                    str(dims[0]),
                    segment,
                    flags=re.I,
                )
                segment = re.sub(
                    rf"\blbound\s*\(\s*{re.escape(name)}\s*\)", "1", segment, flags=re.I
                )
                segment = re.sub(
                    rf"\b({re.escape(name)})\s*[\[{{]([^\]}}]*)[\]}}]",
                    lambda match, dims=dims: _reference(match, dims, item),
                    segment,
                    flags=re.I,
                )
            parts[position] = segment
        return "".join(parts)

    def _unroll(
        self, header: re.Match[str], body: list[MacroStatement], opener: MacroStatement
    ) -> list[MacroStatement]:
        variable = header.group(1)
        values = [parse_constant_integer(header.group(group)) for group in (2, 3)]
        step = parse_constant_integer(header.group(4)) if header.group(4) else 1
        if None in values or step is None:
            raise MacroExpansionError(
                "Iterative DO loops without OUTPUT are unrolled at conversion time, "
                "so the bounds must be integer constants or DIM(array).",
                opener,
            )
        start, stop = values
        assert start is not None and stop is not None
        if step == 0:
            raise MacroExpansionError("DO loop BY step cannot be zero.", opener)
        counter = range(start, stop + (1 if step > 0 else -1), step)
        self.emitted += len(counter) * max(len(body), 1)
        if self.emitted > _MAX_STATEMENTS:
            raise MacroExpansionError(
                f"Unrolling this DO loop would exceed {_MAX_STATEMENTS} statements.",
                opener,
            )
        for part in body:
            if re.match(rf"(?:.*\bthen\s+)?{re.escape(variable)}\s*(?:=(?!=)|\+)", part.text.strip(), re.I | re.S):
                raise MacroExpansionError(
                    "DO loop body cannot reassign its index variable.", part
                )
        output: list[MacroStatement] = []
        for value in counter:
            literal = str(value) if value >= 0 else f"({value})"
            copy = [
                MacroStatement(_substitute(part.text, variable, literal), part.line)
                for part in body
            ]
            output.extend(self.process(copy))
        final = (counter[-1] + step) if len(counter) else start
        output.append(MacroStatement(f"{variable} = {final}", opener.line))
        return output


def _reference(match: re.Match[str], dims: tuple[int, ...], item: MacroStatement) -> str:
    indexes = [parse_constant_integer(part) for part in match.group(2).split(",")]
    if len(indexes) != len(dims):
        raise MacroExpansionError(
            f"Array {match.group(1)!r} needs {len(dims)} subscript(s).", item
        )
    if any(value is None for value in indexes):
        return f"{match.group(1)}[{match.group(2).strip()}]" if len(dims) == 1 else match.group(0)
    flat = 0
    for value, size in zip(indexes, dims):
        assert value is not None
        if not 1 <= value <= size:
            raise MacroExpansionError(
                f"Array index {value} is outside 1..{size} for array {match.group(1)!r}.",
                item,
            )
        flat = flat * size + (value - 1)
    return f"{match.group(1)}[{flat + 1}]"


def parse_constant_integer(text: str) -> int | None:
    value = text.strip()
    if not re.fullmatch(r"[\d\s+\-*()]+", value) or "**" in value:
        return None
    try:
        result = eval(value, {"__builtins__": {}}, {})  # noqa: S307 - validated digits and + - * ( ) only
    except (SyntaxError, TypeError, ValueError):
        return None
    return result if isinstance(result, int) else None


def _substitute(text: str, variable: str, literal: str) -> str:
    parts = _QUOTED.split(text)
    for position in range(0, len(parts), 2):
        parts[position] = re.sub(
            rf"(?<![\w.]){re.escape(variable)}(?![\w.(])", literal, parts[position], flags=re.I
        )
    return "".join(parts)


def _expand_elements(text: str, item: MacroStatement) -> list[str]:
    elements: list[str] = []
    for token in text.replace(",", " ").split():
        ranged = re.fullmatch(r"([A-Za-z_]\w*?)(\d+)-([A-Za-z_]\w*?)(\d+)", token)
        if ranged:
            if ranged.group(1).casefold() != ranged.group(3).casefold():
                raise MacroExpansionError("ARRAY name ranges must share a prefix.", item)
            first, last = int(ranged.group(2)), int(ranged.group(4))
            if first > last:
                raise MacroExpansionError("ARRAY name ranges must be ascending.", item)
            elements.extend(f"{ranged.group(1)}{number}" for number in range(first, last + 1))
        elif re.fullmatch(r"[A-Za-z_]\w*", token):
            elements.append(token)
        else:
            raise MacroExpansionError(f"Unsupported ARRAY element: {token!r}.", item)
    return elements


def _opens_block(text: str) -> bool:
    return bool(
        re.match(r"(?:do|select)\b", text, re.I)
        or re.search(r"\bthen\s+do$", text, re.I)
        or re.fullmatch(r"else\s+do", text, re.I)
    )


def _loop_end(statements: list[MacroStatement], start: int) -> int | None:
    depth = 0
    for position in range(start, len(statements)):
        text = statements[position].text.strip()
        if text.casefold() == "end":
            depth -= 1
            if depth == 0:
                return position
        elif _opens_block(text):
            depth += 1
    return None


def _has_output(text: str) -> bool:
    return bool(re.search(r"(?:^|\bthen\s+|\belse\s+)output\b", text.strip(), re.I))
