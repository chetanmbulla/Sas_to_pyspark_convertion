"""Conservative SAS dependency analysis and deterministic migration planning."""

from __future__ import annotations

import hashlib
import heapq
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .discovery import _decode_source, _sas_statements
from .project import Dataset, MigrationProject, Program


PLAN_SCHEMA_VERSION = 1
_DATASET = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?$")
_DATASET_TOKEN = re.compile(
    r"&[A-Za-z_]\w*\.?|[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?"
)
_SQL_DATASET = re.compile(
    r"\b(?:from|join)\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?|&[A-Za-z_]\w*\.?)\b",
    re.IGNORECASE,
)
_SQL_CREATE = re.compile(
    r"\bcreate\s+(?:table|view)\s+"
    r"([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?|&[A-Za-z_]\w*\.?)\b",
    re.IGNORECASE,
)
_INCLUDE_LITERAL = re.compile(
    r"^\s*%include\s+(['\"])(.*?)\1", re.IGNORECASE | re.DOTALL
)
_MACRO_DEFINITION = re.compile(
    r"^\s*%macro\s+([A-Za-z_]\w*)\b", re.IGNORECASE
)
_MACRO_CALL = re.compile(r"%([A-Za-z_]\w*)\s*(?=[(;]|$)", re.IGNORECASE)
_MACRO_BUILTINS = {
    "abort",
    "bquote",
    "by",
    "copy",
    "display",
    "do",
    "else",
    "end",
    "eval",
    "global",
    "goto",
    "if",
    "include",
    "input",
    "kleft",
    "ktrim",
    "let",
    "local",
    "macro",
    "mend",
    "nrstr",
    "put",
    "qscan",
    "qsysfunc",
    "quote",
    "scan",
    "str",
    "superq",
    "syscall",
    "sysevalf",
    "sysfunc",
    "sysget",
    "syslput",
    "sysmacexec",
    "sysmexecdepth",
    "sysmexecname",
    "sysprod",
    "syspbuff",
    "sysrput",
    "sysrputSync",
    "sysrc",
    "sysrput",
    "syslput",
    "sysmstoreclear",
    "sysmexecdepth",
    "sysmexecname",
    "sysindex",
    "sysfunc",
    "sysevalf",
    "syscall",
    "syserr",
    "syserrortext",
    "syscc",
    "sysdate",
    "sysdate9",
    "systime",
    "sysver",
    "sysvlong",
    "sysvlong4",
    "sysvlong5",
    "sysvlong6",
    "then",
    "to",
    "while",
    "until",
    "upcase",
    "lowcase",
    "left",
    "length",
    "substr",
    "qsubstr",
    "qupcase",
    "qleft",
    "qtrim",
    "trim",
    "unquote",
    "verify",
    "window",
    "write",
    "index",
    "scan",
    "substrn",
    "cmpres",
    "superq",
    "put",
    "sysevalf",
    "eval",
    "quote",
    "nrquote",
    "nrbquote",
    "nstr",
    "nrstr",
}
_SUPPORTED_PROCS = {
    "EXPORT",
    "FREQ",
    "IMPORT",
    "MEANS",
    "SQL",
    "SORT",
    "SUMMARY",
    "TRANSPOSE",
}
class DependencyAnalysisError(ValueError):
    """Raised when a migration plan cannot be analyzed from its project inputs."""


@dataclass(frozen=True)
class PlanDependency:
    source_id: str
    target_id: str
    kind: str

    def to_dict(self) -> dict[str, str]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "kind": self.kind,
        }


@dataclass(frozen=True)
class UnresolvedReference:
    program_id: str
    program_path: str
    kind: str
    reference: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "program_id": self.program_id,
            "program_path": self.program_path,
            "kind": self.kind,
            "reference": self.reference,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class DatasetFlow:
    dataset_id: str
    name: str
    produced_by: tuple[str, ...]
    consumed_by: tuple[str, ...]
    classification: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "name": self.name,
            "produced_by": list(self.produced_by),
            "consumed_by": list(self.consumed_by),
            "classification": self.classification,
        }


@dataclass(frozen=True)
class ProgramPlan:
    program_id: str
    path: str
    source_sha256: str | None
    sequence: int | None
    status: str
    complexity_score: int
    complexity: str
    statement_count: int
    procedures: tuple[str, ...]
    unsupported_procedures: tuple[str, ...]
    macro_features: tuple[str, ...]
    prerequisites: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "program_id": self.program_id,
            "path": self.path,
            "source_sha256": self.source_sha256,
            "sequence": self.sequence,
            "status": self.status,
            "complexity_score": self.complexity_score,
            "complexity": self.complexity,
            "statement_count": self.statement_count,
            "procedures": list(self.procedures),
            "unsupported_procedures": list(self.unsupported_procedures),
            "macro_features": list(self.macro_features),
            "prerequisites": list(self.prerequisites),
        }


@dataclass(frozen=True)
class MigrationPlan:
    project_id: str
    project_name: str
    repository_root: str
    generated_at: str
    programs: tuple[ProgramPlan, ...]
    datasets: tuple[DatasetFlow, ...]
    dependencies: tuple[PlanDependency, ...]
    unresolved_references: tuple[UnresolvedReference, ...]
    cycles: tuple[tuple[str, ...], ...]
    schema_version: int = PLAN_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "repository_root": self.repository_root,
            "generated_at": self.generated_at,
            "programs": [item.to_dict() for item in self.programs],
            "datasets": [item.to_dict() for item in self.datasets],
            "dependencies": [item.to_dict() for item in self.dependencies],
            "unresolved_references": [
                item.to_dict() for item in self.unresolved_references
            ],
            "cycles": [list(cycle) for cycle in self.cycles],
            "summary": {
                "program_count": len(self.programs),
                "dataset_count": len(self.datasets),
                "dependency_count": len(self.dependencies),
                "unresolved_reference_count": len(self.unresolved_references),
                "cycle_count": len(self.cycles),
                "blocked_program_count": sum(
                    item.status in {"blocked_cycle", "blocked_dependency"}
                    for item in self.programs
                ),
            },
        }

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


@dataclass
class _ProgramFacts:
    program: Program
    source_sha256: str | None
    statement_count: int
    producers: set[str]
    consumers: set[str]
    includes: set[str]
    macro_calls: set[str]
    macro_definitions: set[str]
    unresolved: list[UnresolvedReference]
    procedures: set[str]
    macro_features: set[str]


def _dataset_key(value: str) -> str:
    normalized = value.strip().strip("'\"").casefold()
    if "." not in normalized:
        normalized = f"work.{normalized}"
    return normalized


def _parse_dataset_tokens(value: str) -> tuple[list[str], list[str]]:
    datasets: list[str] = []
    dynamic: list[str] = []
    remainder = value.strip()
    while remainder:
        remainder = remainder.lstrip()
        if not remainder or remainder.startswith(("(", "/")):
            break
        token_match = _DATASET_TOKEN.match(remainder)
        if token_match is None:
            break
        token = token_match.group()
        following = remainder[token_match.end():].lstrip()
        if following.startswith("="):
            break
        if following.startswith("-"):
            range_match = _DATASET_TOKEN.match(following[1:].lstrip())
            if range_match is not None:
                dynamic.append(f"{token}-{range_match.group()}")
                remainder = following[1:].lstrip()[range_match.end():]
                continue
        if token.casefold() in {"_null_", "_all_"}:
            remainder = remainder[token_match.end():]
            continue
        if token.startswith("&"):
            dynamic.append(token)
        elif _DATASET.fullmatch(token):
            datasets.append(_dataset_key(token))
        remainder = remainder[token_match.end():]
        if remainder.startswith("="):
            break
    return datasets, dynamic


def _option_values(statement: str, option: str) -> list[str]:
    result: list[str] = []
    for match in re.finditer(rf"\b{re.escape(option)}\s*=\s*", statement, re.I):
        remainder = statement[match.end():].lstrip()
        if remainder.startswith("&"):
            token = _DATASET_TOKEN.match(remainder)
            result.append(token.group() if token else remainder.split()[0])
            continue
        token_match = re.match(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?", remainder)
        if token_match:
            result.append(_dataset_key(token_match.group()))
    return result


def _without_quoted_strings(statement: str) -> str:
    output = list(statement)
    index = 0
    quote: str | None = None
    while index < len(statement):
        char = statement[index]
        if quote is not None:
            output[index] = " "
            if char == quote:
                if index + 1 < len(statement) and statement[index + 1] == quote:
                    output[index + 1] = " "
                    index += 2
                    continue
                quote = None
        elif char in {"'", '"'}:
            quote = char
            output[index] = " "
        index += 1
    return "".join(output)


def _analyze_program(
    program: Program,
    repository: Path,
) -> _ProgramFacts:
    path = (repository / Path(program.path)).resolve(strict=False)
    try:
        path.relative_to(repository)
    except ValueError:
        raise DependencyAnalysisError(
            f"Program path escapes repository root: {program.path!r}."
        ) from None
    try:
        content = path.read_bytes()
    except OSError as error:
        unresolved = UnresolvedReference(
            program.id,
            program.path,
            "program_source",
            program.path,
            f"Cannot read source program: {error}",
        )
        return _ProgramFacts(
            program, None, 0, set(), set(), set(), set(), set(), [unresolved], set(), set()
        )
    source_sha256 = hashlib.sha256(content).hexdigest()
    source, _, decode_error = _decode_source(content)
    if source is None:
        unresolved = UnresolvedReference(
            program.id,
            program.path,
            "program_source",
            program.path,
            decode_error or "Cannot decode program source.",
        )
        return _ProgramFacts(
            program,
            source_sha256,
            0,
            set(),
            set(),
            set(),
            set(),
            set(),
            [unresolved],
            set(),
            set(),
        )

    statements = _sas_statements(source)
    producers: set[str] = set()
    consumers: set[str] = set()
    includes: set[str] = set()
    macro_calls: set[str] = set()
    macro_definitions: set[str] = set()
    unresolved: list[UnresolvedReference] = []
    procedures: set[str] = set()
    macro_features: set[str] = set()
    in_proc = False

    def add_dataset_values(values: list[str], direction: set[str]) -> None:
        for value in values:
            if value.startswith("&"):
                unresolved.append(
                    UnresolvedReference(
                        program.id,
                        program.path,
                        "dataset",
                        value,
                        "Dataset name is macro-generated and cannot be resolved statically.",
                    )
                )
            else:
                direction.add(value)

    for statement in statements:
        stripped = statement.strip()
        lower = stripped.casefold()
        code = _without_quoted_strings(stripped)
        macro_definition = _MACRO_DEFINITION.match(stripped)
        if macro_definition:
            macro_definitions.add(macro_definition.group(1).casefold())
            macro_features.add("macro_definition")
        for call in _MACRO_CALL.finditer(code):
            macro_name = call.group(1).casefold()
            if macro_name not in _MACRO_BUILTINS:
                macro_calls.add(macro_name)
                macro_features.add("macro_call")
        if re.search(r"&{1,2}[A-Za-z_]\w*", code):
            macro_features.add("macro_variable_reference")
        if lower.startswith("%include"):
            macro_features.add("include")
            include_match = _INCLUDE_LITERAL.match(stripped)
            if include_match:
                includes.add(include_match.group(2))
            else:
                unresolved.append(
                    UnresolvedReference(
                        program.id,
                        program.path,
                        "include",
                        stripped,
                        "Include path is dynamic or not a quoted literal.",
                    )
                )

        data_match = re.match(r"^\s*data\s+(.+)$", stripped, re.I | re.S)
        if data_match:
            values, dynamic = _parse_dataset_tokens(data_match.group(1))
            add_dataset_values(values, producers)
            for value in dynamic:
                reason = (
                    "DATA-step output name is macro-generated."
                    if value.startswith("&")
                    else "DATA-step output range cannot be resolved statically."
                )
                unresolved.append(
                    UnresolvedReference(
                        program.id,
                        program.path,
                        "dataset_output",
                        value,
                        reason,
                    )
                )
            continue

        input_match = re.match(r"^\s*(set|merge)\s+(.+)$", stripped, re.I | re.S)
        if input_match:
            values, dynamic = _parse_dataset_tokens(input_match.group(2))
            add_dataset_values(values, consumers)
            for value in dynamic:
                reason = (
                    "Input dataset name is macro-generated."
                    if value.startswith("&")
                    else "Input dataset range cannot be resolved statically."
                )
                unresolved.append(
                    UnresolvedReference(
                        program.id,
                        program.path,
                        "dataset_input",
                        value,
                        reason,
                    )
                )
            continue

        proc_match = re.match(r"^\s*proc\s+([A-Za-z_]\w*)\b", stripped, re.I)
        if proc_match:
            in_proc = True
            procedure = proc_match.group(1).upper()
            procedures.add(procedure)
            add_dataset_values(_option_values(code, "data"), consumers)
            if procedure == "IMPORT":
                add_dataset_values(_option_values(code, "out"), producers)
            elif procedure == "EXPORT":
                add_dataset_values(_option_values(code, "data"), consumers)
            elif procedure != "SQL":
                add_dataset_values(_option_values(code, "out"), producers)
        elif lower in {"run", "quit"}:
            in_proc = False

        if in_proc and not lower.startswith("proc "):
            add_dataset_values(_option_values(code, "out"), producers)

        sql_create = _SQL_CREATE.search(code)
        if sql_create:
            value = sql_create.group(1)
            if value.startswith("&"):
                unresolved.append(
                    UnresolvedReference(
                        program.id,
                        program.path,
                        "dataset_output",
                        value,
                        "SQL output dataset name is macro-generated.",
                    )
                )
            else:
                producers.add(_dataset_key(value))
        for sql_match in _SQL_DATASET.finditer(code):
            value = sql_match.group(1)
            if value.startswith("&"):
                unresolved.append(
                    UnresolvedReference(
                        program.id,
                        program.path,
                        "dataset_input",
                        value,
                        "SQL input dataset name is macro-generated.",
                    )
                )
            else:
                consumers.add(_dataset_key(value))
    for macro_name in macro_definitions:
        macro_calls.discard(macro_name)
    return _ProgramFacts(
        program,
        source_sha256,
        len(statements),
        producers,
        consumers,
        includes,
        macro_calls,
        macro_definitions,
        unresolved,
        procedures,
        macro_features,
    )


def _complexity(facts: _ProgramFacts) -> tuple[int, str, tuple[str, ...]]:
    unsupported = tuple(sorted(facts.procedures - _SUPPORTED_PROCS))
    score = 0
    score += min(facts.statement_count // 25, 10)
    score += 2 * len(unsupported)
    score += 2 * len(facts.macro_features)
    score += len(facts.unresolved)
    score += len(facts.includes)
    score += len(facts.consumers - facts.producers) // 5
    level = "low" if score <= 2 else "medium" if score <= 6 else "high"
    return score, level, unsupported


def _topological_order(
    program_ids: set[str],
    prerequisites: dict[str, set[str]],
    path_by_id: dict[str, str],
) -> tuple[list[str], set[str], tuple[tuple[str, ...], ...]]:
    dependents: dict[str, set[str]] = defaultdict(set)
    remaining = {node: set(prerequisites.get(node, ())) for node in program_ids}
    for node, required in remaining.items():
        for prerequisite in required:
            if prerequisite in program_ids:
                dependents[prerequisite].add(node)
    ready = [
        (path_by_id[node].casefold(), node)
        for node, required in remaining.items()
        if not required.intersection(program_ids)
    ]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        _, node = heapq.heappop(ready)
        ordered.append(node)
        for dependent in sorted(
            dependents[node], key=lambda item: path_by_id[item].casefold()
        ):
            remaining[dependent].discard(node)
            if not remaining[dependent].intersection(program_ids):
                heapq.heappush(
                    ready, (path_by_id[dependent].casefold(), dependent)
                )
    blocked = program_ids.difference(ordered)
    cycles = _find_cycles(blocked, prerequisites, path_by_id)
    return ordered, blocked, cycles


def _find_cycles(
    candidates: set[str],
    prerequisites: dict[str, set[str]],
    path_by_id: dict[str, str],
) -> tuple[tuple[str, ...], ...]:
    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    cycles: list[tuple[str, ...]] = []

    def connect(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        for neighbor in sorted(
            prerequisites.get(node, set()).intersection(candidates),
            key=lambda item: path_by_id[item].casefold(),
        ):
            if neighbor not in indices:
                connect(neighbor)
                lowlinks[node] = min(lowlinks[node], lowlinks[neighbor])
            elif neighbor in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[neighbor])
        if lowlinks[node] == indices[node]:
            component: list[str] = []
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.append(member)
                if member == node:
                    break
            if len(component) > 1 or node in prerequisites.get(node, set()):
                cycles.append(
                    tuple(sorted(component, key=lambda item: path_by_id[item].casefold()))
                )

    for node in sorted(candidates, key=lambda item: path_by_id[item].casefold()):
        if node not in indices:
            connect(node)
    cycles.sort(key=lambda component: path_by_id[component[0]].casefold())
    return tuple(cycles)


def build_migration_plan(
    project: MigrationProject, repository_root: str | Path
) -> MigrationPlan:
    project.validate()
    try:
        root = Path(repository_root).resolve(strict=True)
    except OSError as error:
        raise DependencyAnalysisError(
            f"Cannot access repository root {str(repository_root)!r}: {error}"
        ) from error
    if not root.is_dir():
        raise DependencyAnalysisError(
            f"Repository root is not a directory: {str(root)!r}."
        )

    facts = [_analyze_program(program, root) for program in project.programs]
    path_by_id = {item.program.id: item.program.path for item in facts}
    dataset_names = sorted(
        set().union(*(item.producers | item.consumers for item in facts))
    )
    datasets = {name: Dataset.create(name) for name in dataset_names}
    program_by_dataset_producer: dict[str, set[str]] = defaultdict(set)
    program_by_dataset_consumer: dict[str, set[str]] = defaultdict(set)
    dependencies: set[tuple[str, str, str]] = set()
    prerequisites: dict[str, set[str]] = defaultdict(set)
    unresolved = [item for fact in facts for item in fact.unresolved]

    for fact in facts:
        for dataset_name in fact.producers:
            dataset_id = datasets[dataset_name].id
            program_by_dataset_producer[dataset_name].add(fact.program.id)
            dependencies.add((fact.program.id, dataset_id, "produces"))
        for dataset_name in fact.consumers:
            dataset_id = datasets[dataset_name].id
            program_by_dataset_consumer[dataset_name].add(fact.program.id)
            dependencies.add((fact.program.id, dataset_id, "consumes"))

    macro_providers: dict[str, set[str]] = defaultdict(set)
    for fact in facts:
        for name in fact.macro_definitions:
            macro_providers[name].add(fact.program.id)
    for fact in facts:
        for name in fact.macro_calls:
            providers = macro_providers.get(name, set())
            if len(providers) == 1:
                provider = next(iter(providers))
                if provider != fact.program.id:
                    prerequisites[fact.program.id].add(provider)
                    dependencies.add((fact.program.id, provider, "calls_macro"))
            elif len(providers) > 1:
                unresolved.append(
                    UnresolvedReference(
                        fact.program.id,
                        fact.program.path,
                        "macro",
                        name,
                        "Multiple macro definitions were found; call target is ambiguous.",
                    )
                )
            else:
                unresolved.append(
                    UnresolvedReference(
                        fact.program.id,
                        fact.program.path,
                        "macro",
                        name,
                        "No macro definition was found in the scanned repository.",
                    )
                )

    include_lookup: dict[str, set[str]] = defaultdict(set)
    for fact in facts:
        include_lookup[fact.program.path.casefold().replace("\\", "/")].add(
            fact.program.id
        )
        include_lookup[Path(fact.program.path).name.casefold()].add(fact.program.id)
    for fact in facts:
        for include in fact.includes:
            include_path = Path(include)
            candidates = (
                (Path(fact.program.path).parent / include_path).as_posix().casefold(),
                include_path.as_posix().casefold(),
                include_path.name.casefold(),
            )
            matches = set().union(*(include_lookup.get(value, set()) for value in candidates))
            if len(matches) == 1:
                prerequisite = next(iter(matches))
                if prerequisite != fact.program.id:
                    prerequisites[fact.program.id].add(prerequisite)
                    dependencies.add((fact.program.id, prerequisite, "includes"))
            elif not matches:
                unresolved.append(
                    UnresolvedReference(
                        fact.program.id,
                        fact.program.path,
                        "include",
                        include,
                        "Included program was not found among scanned project programs.",
                    )
                )
            else:
                unresolved.append(
                    UnresolvedReference(
                        fact.program.id,
                        fact.program.path,
                        "include",
                        include,
                        "Included filename matches multiple scanned programs.",
                    )
                )

    dataset_flows: list[DatasetFlow] = []
    for name in dataset_names:
        producers = tuple(
            sorted(program_by_dataset_producer[name], key=lambda item: path_by_id[item].casefold())
        )
        consumers = tuple(
            sorted(program_by_dataset_consumer[name], key=lambda item: path_by_id[item].casefold())
        )
        if len(producers) > 1:
            classification = "ambiguous_producer"
            for consumer in consumers:
                unresolved.append(
                    UnresolvedReference(
                        consumer,
                        path_by_id[consumer],
                        "dataset",
                        name,
                        "Dataset has multiple producers; execution order cannot be inferred.",
                    )
                )
        elif producers:
            classification = "internal"
            for producer in producers:
                for consumer in consumers:
                    if producer != consumer:
                        prerequisites[consumer].add(producer)
                        dependencies.add((consumer, producer, "dataset_dependency"))
        elif consumers:
            classification = "external_input"
            for consumer in consumers:
                unresolved.append(
                    UnresolvedReference(
                        consumer,
                        path_by_id[consumer],
                        "dataset",
                        name,
                        "No producer was found; classify this as an external input or resolve it.",
                    )
                )
        else:
            classification = "unreferenced"
        dataset_flows.append(
            DatasetFlow(
                datasets[name].id,
                name,
                producers,
                consumers,
                classification,
            )
        )

    ordered, blocked, cycles = _topological_order(
        {item.program.id for item in facts}, prerequisites, path_by_id
    )
    cycle_members = set().union(*cycles) if cycles else set()
    facts_by_id = {item.program.id: item for item in facts}
    planned_programs: list[ProgramPlan] = []
    sequenced = {program_id: sequence for sequence, program_id in enumerate(ordered, start=1)}
    for program_id in sorted(
        path_by_id, key=lambda item: (sequenced.get(item, len(sequenced) + 1), path_by_id[item].casefold())
    ):
        fact = facts_by_id[program_id]
        score, level, unsupported = _complexity(fact)
        program_prerequisites = tuple(
            sorted(prerequisites.get(program_id, set()), key=lambda item: path_by_id[item].casefold())
        )
        planned_programs.append(
            ProgramPlan(
                program_id=program_id,
                path=fact.program.path,
                source_sha256=fact.source_sha256,
                sequence=sequenced.get(program_id),
                status=(
                    "blocked_cycle"
                    if program_id in cycle_members
                    else "blocked_dependency"
                    if program_id in blocked
                    else "planned"
                ),
                complexity_score=score,
                complexity=level,
                statement_count=fact.statement_count,
                procedures=tuple(sorted(fact.procedures)),
                unsupported_procedures=unsupported,
                macro_features=tuple(sorted(fact.macro_features)),
                prerequisites=program_prerequisites,
            )
        )

    unresolved.sort(
        key=lambda item: (
            item.program_path.casefold(),
            item.kind,
            item.reference.casefold(),
            item.reason,
        )
    )
    plan_dependencies = tuple(
        PlanDependency(*item)
        for item in sorted(dependencies, key=lambda item: (item[0], item[1], item[2]))
    )
    return MigrationPlan(
        project_id=project.project_id,
        project_name=project.name,
        repository_root=str(root),
        generated_at=datetime.now(timezone.utc).isoformat(),
        programs=tuple(planned_programs),
        datasets=tuple(dataset_flows),
        dependencies=plan_dependencies,
        unresolved_references=tuple(unresolved),
        cycles=cycles,
    )
