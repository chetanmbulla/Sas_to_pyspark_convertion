"""Typed intermediate records used while lowering SAS DATA steps."""

from __future__ import annotations

from dataclasses import dataclass

from .data_order import OrderKey


@dataclass(frozen=True)
class DataAction:
    kind: str
    expression: str
    column: str | None = None
    columns: tuple[str, ...] = ()
    array_index: str | None = None
    array_index_columns: tuple[str, ...] = ()
    array_loop_variable: str | None = None
    condition: str | None = None
    otherwise: str | None = None
    loop_start: int | str | None = None
    loop_stop: int | str | None = None
    loop_step: int | None = None
    loop_bound_columns: tuple[str, ...] = ()
    loop_assignments: tuple[tuple[str, str], ...] = ()
    loop_delete_conditions: tuple[str, ...] = ()
    loop_operations: tuple[tuple[str, int | str], ...] = ()
    merge_sources: tuple[str, ...] = ()
    merge_keys: tuple[str, ...] = ()
    merge_directions: tuple[bool, ...] = ()
    merge_in_vars: tuple[str, ...] = ()
    merge_order_keys: tuple[tuple[OrderKey, ...], ...] = ()
    branches: tuple[tuple[str, str], ...] = ()
    retain_initial: str | None = None
    retain_partition: tuple[str, ...] = ()
    retain_reset: str | None = None


@dataclass(frozen=True)
class DataStepIR:
    target: str
    source_line: int
    actions: tuple[DataAction, ...]
    order_by_keys: tuple[OrderKey, ...] | None
    has_retained_state: bool
    by_flags: frozenset[str]
    has_row_number: bool

    @property
    def requires_deterministic_order(self) -> bool:
        return (
            self.has_row_number
            or self.has_retained_state
            or bool(self.by_flags)
            or any(
                "lag(" in action.expression.casefold()
                or " over (" in action.expression.casefold()
                for action in self.actions
            )
        )
