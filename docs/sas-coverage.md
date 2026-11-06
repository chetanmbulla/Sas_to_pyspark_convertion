# SAS Coverage and Compatibility Policy

## Target

The converter targets a deliberately bounded subset of Base SAS syntax shared
by SAS 9.4M8 and current SAS Viya. It is not a general SAS compiler. SAS/STAT,
CAS, DS2, SAS/ACCESS-specific behavior, and other separately licensed products
are outside this target unless explicitly added to a future profile.

The `sas-version` selection currently records the asserted source release and
checks marker/override consistency. It does not gate every feature by release
or emulate release-specific runtime behavior.

## Coverage matrix

“Supported” means only the documented forms are translated; it does not mean
all SAS options or runtime semantics are equivalent. “Approximate” means the
converter deliberately emits behavior that can differ from SAS. Features not
listed as supported or approximate should be treated as unsupported.

| Area | Status | Current boundary |
|---|---|---|
| Lexing and diagnostics | Partial | Comments, strings, tokens, semicolon-terminated statements, and source spans; not a complete SAS grammar. |
| DATA steps | Partial | Documented `SET`/`MERGE`, assignments, filters, selected blocks, arrays, loops, `OUTPUT`, `RETAIN`, `BY`, and dataset options only. Unsupported forms fail conversion. |
| Macros | Partial | Bounded macro expansion and selected macro statements/functions; not the complete macro facility. |
| Procedures | Partial | Selected forms of `SORT`, `FREQ`, `MEANS`/`SUMMARY`, `TRANSPOSE`, `IMPORT`, `EXPORT`, `SQL`, `FORMAT`, and report procedures. Most PROCs are unsupported. |
| Functions and formats | Partial | Only functions, date operations, formats, and informats listed in the [supported syntax reference](../README.md#supported-syntax). |
| Ordering and state | Strict for supported order-sensitive DATA steps | `LAG`/`DIF`, `_N_`, `RETAIN`, and `FIRST.`/`LAST.` require an explicit unique DATA-step `ORDER BY`. MERGE with duplicate BY groups requires per-input `ORDER_BY=(...)`; otherwise generated code raises at runtime. |
| Sort tie behavior | Approximate | `PROC SORT NODUPKEY` may retain a different tied row than SAS. |
| Missing values and types | Approximate | SAS missing values, fixed-width character behavior, implicit conversions, and Spark schema/type coercion are not universally equivalent. |
| Execution and comparison | Separate from conversion | Generated code needs Spark; syntax validity alone does not establish runtime or business-result parity. Reconciliation requires reference outputs. |

Use the [README supported-syntax section](../README.md#supported-syntax) as
the detailed inventory of implemented statements, options, limitations, and
known semantic differences. A conversion success means the input matched the
implemented subset; it is not a SAS-equivalence certification.

## Feature acceptance gates

Before calling a new SAS construct supported:

1. Define its accepted grammar, release scope, and unsupported variants.
2. Preserve source locations through parsing, expansion, and lowering; errors
   should identify the source construct rather than silently pass through.
3. Add positive and negative parser/converter tests, including boundary cases.
4. Add a curated semantic fixture with input schema/data and expected output.
   When a SAS runtime is available, prefer a SAS-generated reference result;
   record the reference provenance with the fixture.
5. Execute generated code on supported Spark versions and compare schemas,
   missing values, duplicate rows, and values—not just generated source text.
6. Document approximations and warnings, and require an explicit deterministic
   key for any behavior that depends on observation order.

## Incremental semantic-IR migration

The source parser currently produces tokens and statements with spans, and the
DATA-step converter now records typed actions plus shared order-safety metadata.
Most parsing and PROC semantics still use construct-specific logic. Replace
those boundaries incrementally rather than attempting a flag-day rewrite:

1. Extend typed DATA-step actions with source spans and typed expression nodes.
2. Build complete DATA-step and PROC nodes from parsed statements after macro
   expansion.
3. Move one well-tested construct at a time to IR-based validation and lowering.
4. Keep old and new paths behaviorally comparable during migration, then remove
   string-rewrite paths only after their feature fixtures pass.

Do not treat an unused IR model as an improvement; each node should be consumed
by validation, lowering, or diagnostics.
