# sas2pyspark

`sas2pyspark` converts a **supported subset** of SAS programs into PySpark
source. It includes a Python library, command-line converter, and optional
Streamlit interface for reviewing and testing generated code.

It is a migration aid, not a complete SAS compiler. Generated code must be
reviewed and validated against representative data before production use.

## Documentation

- [Quick start](#quick-start)
- [Install](#install)
- [Use the command line](#use-the-command-line)
- [Use the Streamlit GUI](#use-the-streamlit-gui)
- [Use the Python API](#use-the-python-api)
- [Conversion process](docs/conversion_process.md)
- [Examples](#examples)
- [Progressive SAS and CSV examples](#progressive-gui-test-programs)
- [Supported syntax and limitations](#supported-syntax)
- [Migration project workflows](#migration-project-foundation)
- [Generated-code conventions and caveats](#generated-code-conventions-and-caveats)
- [Run tests](#run-tests)
- [Windows Spark worker troubleshooting](#windows-spark-worker-troubleshooting)

## Quick start

The fastest way to try the GUI on Windows is to open PowerShell in the project
folder and run:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[gui]"
.\.venv\Scripts\python.exe -m streamlit run app\streamlit_app.py
```

Open the local URL printed by Streamlit (usually
`http://localhost:8501`). Paste or upload SAS source, choose **Convert to
PySpark**, review or edit the generated code, then upload its input CSV file(s)
and choose **Run PySpark test**.

## Project layout

- `src/sas2pyspark/` — installable Python package and CLI.
- `app/` — Streamlit GUI entry point.
- `tests/` — unit and Spark integration tests.
- `docs/` — workflow and implementation documentation.
- `examples/` — sample SAS programs and input datasets.
- `pyproject.toml` — package metadata and build configuration.

## Install

Python 3.10 or newer is required. Run installation commands from the project
root, where `pyproject.toml` is located.

### Core converter and command line

The core package has no third-party runtime dependencies. Install it in
editable mode to use the `sas2pyspark` command while working from this source
checkout:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
```

### Optional components

Install only the extras needed for your workflow:

| Extra | Install command | Provides |
|---|---|---|
| `gui` | `.\.venv\Scripts\python.exe -m pip install -e ".[gui]"` | Streamlit GUI and PySpark local execution |
| `test` | `.\.venv\Scripts\python.exe -m pip install -e ".[test]"` | PySpark integration tests |
| `reconcile` | `.\.venv\Scripts\python.exe -m pip install -e ".[reconcile]"` | PySpark plus pandas and pyreadstat for output reconciliation |

The GUI and Spark integration tests need a compatible Java runtime as well as
PySpark. PySpark 4 requires Java 17 or newer. Check the compatibility
requirements for the PySpark version installed in your environment.

## Use the command line

Convert a SAS file into a Python file:

```powershell
.\.venv\Scripts\sas2pyspark.exe input.sas --output output.py
```

The compatibility target is the Base SAS language surface shared by current
SAS Viya and legacy SAS 9.4M8 programs. The Viya profile follows the current
language baseline; it is not pinned to a specific Viya deployment release and
does not imply support for SAS/STAT, CAS, DS2, or other separately licensed
products. Declare the source release in the SAS file when known:

```sas
/* sas2pyspark: sas-version=9.4M8 */
```

Or select a profile explicitly with `--sas-version 9.4M8` or
`--sas-version Viya`. The default `auto` mode reads the marker; if absent, it
selects Viya and emits a warning. The Python API accepts the same keyword
(`convert_sas(source, sas_version="9.4M8")`), and the GUI and
`project convert` command expose the same selection. If a marker and explicit
override disagree, conversion fails. At this stage, the profile records
release selection and validates marker consistency; it does not yet enforce a
complete per-release syntax/semantic feature matrix. Every conversion warns
about this limitation. Selecting 9.4M8 does not emulate SAS runtime
differences.

The command also works as `sas2pyspark input.sas -o output.py` when the
environment's scripts directory is on `PATH`. To print generated code to the
terminal instead of writing a file:

```powershell
.\.venv\Scripts\sas2pyspark.exe input.sas
```

Conversion can emit warnings even when it succeeds. Read those warnings, review
the generated code, and test it using representative data. A successful
conversion means the source matched the supported syntax; it does not prove
that the generated program runs or produces equivalent SAS results.

For a versioned, multi-program workflow, see
[Migration project workflows](#migration-project-foundation).

## Use the Streamlit GUI

Install the `gui` extra, then start the app from the project root:

```powershell
.\.venv\Scripts\python.exe -m streamlit run app\streamlit_app.py
```

In the app:

1. Paste SAS code or upload a `.sas` file and choose **Load uploaded SAS file**.
2. Choose **Convert to PySpark**. Review any conversion warnings.
3. Review or edit the generated Python code. **Check Python syntax** checks
   Python parsing, not Spark behavior.
4. Upload the test data files under **Test datasets**. Use CSV, Parquet, JSON,
   or JSONL files. Each file's base name becomes its SAS input dataset name:
   `people.csv` supplies `df_people` for `SET work.people`.
5. Choose the local Spark master and test settings, then choose
   **Run PySpark test**. Review its status, elapsed time, logs, and CSV output
   previews.

The uploaded datasets are temporary and removed after the test. The app does
not validate business equivalence against SAS automatically. Performance
notices are heuristics, and timings on small samples are not production
benchmarks.

**Security:** Testing runs Python code with the permissions of the account
running Streamlit; the worker process and timeout are not a security sandbox.
Only run code you trust, keep the app local, and use an externally managed
container or VM if stronger isolation is needed.

## Use the Python API

Convert source text directly from Python:

```python
from sas2pyspark import ConversionError, convert_sas

source = """
data work.adults;
  set work.people;
  where age ge 18;
  keep name age;
run;
"""

try:
    result = convert_sas(source)
except ConversionError as error:
    print(f"Conversion failed: {error}")
else:
    print(result.code)
    for warning in result.warnings:
        print(f"Warning: {warning}")
```

`%SYSGET` is disabled by default because it reads the converter host's
environment. To opt in explicitly, use
`SasToPySparkConverter(allow_sysget=True).convert(source)` or
`convert_sas(source, allow_sysget=True)`. It reads the named environment
variable during conversion; missing variables raise an error. Do not use this
option when environment values may contain secrets or untrusted code.

`result.code` is generated PySpark source; it is not executed by the converter.
To run it, provide a Spark session named `spark` and an input DataFrame named
`df_people`, or use the GUI or `sas2pyspark project run` workflow to load files
and execute it.

## Supported syntax

- DATA steps with one or more `SET` inputs and the supported dataset options, filters, assignments, nested `IF`/`DO`/`SELECT`, numeric accumulator and character/conditional `RETAIN` subsets, static arrays and bounded loop unrolling, `BY` flags, `OUTPUT` routing, inline `DATALINES`, `_N_`, `LAG`/`DIF`, and the `ORDER BY` deterministic-order extension (see the detailed limitations below).
- Sequential `%LET name=value` definitions and `&name` substitution.
- User-defined `%MACRO`/`%MEND` macros with positional and keyword parameters, defaults, nested calls (including calls inside arguments), bounded recursion, and inline calls that expand to one statement. `%SYSGET` is opt-in through the Python API; dynamic `%INCLUDE` paths may use preceding `%LET` values.
- Compile-time `%IF`/`%THEN`/`%ELSE` branches with simple comparisons and `AND`/`OR`/`NOT`, plus bounded integer `%DO` loops.
- Indexed integer `DO ... TO ... [BY ...]` loops when the loop ends with one `OUTPUT`, with simple assignments in the loop body.
- `MERGE` of two or more datasets followed by `BY`, including duplicate BY keys (one-to-many and many-to-many), `IN=`, and optional per-input `ORDER_BY=(...)` dataset options for deterministic duplicate-key pairing.
- `PROC SORT` with `DATA=`, optional `OUT=`, `BY` keys with `ASCENDING` or
  `DESCENDING` direction, `NODUPKEY`, `NODUP`/`NODUPRECS`, `DUPOUT=` (with
  `NODUPKEY` or `NODUP`), and a `WHERE` statement. `PROC APPEND BASE= DATA= [FORCE]`
  maps to `unionByName`; `PROC DATASETS` supports `DELETE`, `CHANGE old=new` and
  `APPEND` (terminated by `RUN`/`QUIT`). `PROC FORMAT; VALUE name ... ;` defines
  numeric and `$char` formats with single values, lists, ranges (`low`, `high`, `<`
  exclusions, `.` for missing) and `OTHER`; they apply through `PUT(x, name.)` and
  become `CASE` expressions. Unmatched values show as their own text (`.` for missing
  numerics). `INVALUE` supports exact input mappings and `OTHER`; numeric and `$`
  character outputs are supported. `PICTURE` supports simple `0`/`9` masks with an
  optional decimal and `PREFIX=`, `MULT=` and `ROUND`. `PROC FORMAT CNTLIN=` supports
  standard `FMTNAME`, `TYPE`, `START`, `END`, `LABEL` and `HLO` columns; its rows are
  collected to the driver and should remain small. `LIBNAME lib 'path';` / `LIBNAME lib CLEAR;`
  are accepted as no-ops with a warning (library prefixes are dropped).
  `%INCLUDE 'file';` with a quoted path is inlined before parsing; paths resolve
  relative to the including file, must stay inside the source directory (or
  repository root in batch mode), and circular includes are rejected. A quoted
  path may contain macro variables set by preceding `%LET` statements. Unresolved
  paths, paths outside the allowed root, and dynamic includes inside macro
  definitions are rejected. Line numbers in errors from included code refer to
  the expanded source.
- `PROC FREQ` one-way and two-way `TABLES`, with counts, percentages,
  optional `OUT=`, and the `MISSING` option.
- `PROC MEANS` and `PROC SUMMARY` with `VAR`, optional `CLASS`, selected
  `N`, `NMISS`, `SUM`, `MEAN`, `STD`, `STDERR`, `VAR`, `CV`, `MIN`, and `MAX`
  statistics, and limited `OUTPUT` statistic-name overrides.
- `PROC TRANSPOSE` with `DATA=`, optional `OUT=`, `VAR`, optional `BY`, and a required unique `ORDER=` column.
- CSV-only `PROC IMPORT` with `DATAFILE=`, `OUT=`, `DBMS=CSV`, `GETNAMES=`, and one-character `DELIMITER=`.
- CSV-only `PROC EXPORT` with `DATA=`, `OUTFILE=`, `DBMS=CSV`, `PUTNAMES=`, and one-character `DELIMITER=`.
- `PROC SQL` with `CREATE TABLE ... AS SELECT ...` and standalone `SELECT`/`WITH` queries. `PROC SQL NOPRINT` is accepted. `LEFT`/`RIGHT`/`FULL JOIN` and `CASE` pass through to Spark SQL, and `CALCULATED alias` is rewritten to the aliased expression. `SELECT ... INTO :macro` sets a run-time macro variable (see the macro section).
- Common SAS comparison/logical words (`EQ`, `NE`, `GT`, `GE`, `LT`, `LE`, `AND`, `OR`, `NOT`), `MISSING(column)`, `UPCASE`, `LOWCASE`, `TODAY()`, `CAT`, `CATS`, `CATX`, `STRIP`, `COALESCE`, `COALESCEC`, `SUBSTR`, `COMPRESS`, and `ROUND`.
- SAS block comments and statement comments.

The source-aware parser preserves statement and token locations and recognizes
basic SAS tokens and macro references. It currently provides a lexical and
statement-parsing foundation only; it is not a complete SAS grammar, and
DATA-step/PROC semantics remain limited to the constructs listed above.

The converter fails with `ConversionError` on syntax outside this subset instead of silently generating a misleading translation. Errors identify the SAS source line and offending statement when available. A `SET` statement with multiple simple datasets concatenates them with Spark `unionByName(allowMissingColumns=True)`; this fills columns absent from an input with `NULL`, but Spark row order is unspecified and type coercion may differ from SAS. Dataset options on `SET` are supported as described under "`LAG`/`DIF` and `SET` dataset options" below. `MERGE ... BY` follows SAS match-merge semantics: within each BY group, output has as many rows as the largest input group, and an input with fewer rows repeats its last row (one-to-many and many-to-many). It uses window functions plus left joins, preserves later-dataset overwrite behavior, and orders output by BY keys. Row order inside a duplicated key group follows Spark's incidental order (not guaranteed), so many-to-many pairings are only reproducible when inputs carry a deterministic order. `IN=` flags (`merge a(in=ina) b(in=inb);`) become boolean columns usable in `IF ina and inb;` / `IF ina = 1;` and are dropped from the output. Null BY keys match each other; normalize blank/padded character keys consistently in Spark inputs to match SAS fixed-width character behavior. Other dataset options and `WHERE` on MERGE are unsupported. Conditional branches must be simple assignments to the same column. `IF cond THEN col = expr;` without `ELSE` keeps the existing value of `col` (or `NULL` when the column does not exist yet), and `ELSE IF` chains are supported; `IF cond THEN DO; ... END;` blocks (with `ELSE IF ... THEN DO;` / `ELSE DO;`) are rewritten into per-statement conditionals, and `IF cond THEN DELETE;` becomes a filter that keeps rows whose condition is false or missing. Blocks may contain simple assignments, `DELETE`, nested `IF`/`ELSE` and nested `SELECT` (see "Nested blocks" below); `OUTPUT`, iterative `DO` loops and conditions that reference a column assigned inside the same block are rejected. Multi-dataset DATA steps and `OUTPUT` routing are supported when the routing statements (`IF c THEN OUTPUT ds;`, `ELSE IF ... OUTPUT`, `ELSE OUTPUT`, `OUTPUT [ds ...];`, bare `OUTPUT;` for all datasets) are the final statements; each target dataset becomes its own DATA step with a subsetting filter (rows whose condition is missing fall through to `ELSE`), so shared upstream logic is recomputed per target. A dataset written by more than one routing group (which would create duplicate rows), a dataset never written, `OUTPUT` followed by more statements, and `OUTPUT` inside `DO` blocks are rejected. Supported output-generating DO loops require integer literal or compile-time integer-expression bounds/step, a nonzero step, simple assignments, and exactly one final `OUTPUT`; stateful self-updating assignments, nested loops, and other OUTPUT patterns are rejected. `CAT` concatenates values without trimming and maps Spark `NULL` inputs to empty strings; `CATS` trims each input before concatenating. `CATX` trims values and skips missing/blank inputs. `STRIP` trims both ends and maps `NULL` to an empty string. `COALESCE` uses Spark null semantics for numeric values; `COALESCEC` treats `NULL` and whitespace-only strings as missing, returns the first non-missing value unchanged, and returns an empty string when all inputs are missing. These rules model character missing values as Spark `NULL`, empty, or whitespace-only strings. `COMPRESS` supports the optional character set but not SAS modifiers. `ROUND` supports a positive numeric literal as its rounding unit. `SUBSTR` uses Spark's one-based substring indexing; start positions and lengths must be positive for SAS-compatible results. Date values use SAS numeric day counts from 1960-01-01, and datetime values use numeric seconds from that date. Date literals such as `'01JAN2024'd` and datetime literals such as `'01JAN2024:12:34:56'dt` convert to those counts; invalid calendar dates/times are rejected. `MDY` (invalid dates give missing), `QTR`, `WEEKDAY` (Sunday=1), `DATDIF(...,'ACT/ACT')`, `DATEPART`, `TIMEPART`, `DHMS`, `DATE()`, `TODAY()`, `DATETIME()` and `TIME()` are supported. `INTCK` and `INTNX` support `DAY`, `WEEK` (Sunday start), `MONTH`, `QTR` and `YEAR` with a quoted literal interval, discrete boundary counting, and `INTNX` alignments `BEGINNING`/`END`/`SAME`. `YEAR`, `MONTH` and `DAY` accept SAS date counts. Date `PUT`/`INPUT` support `DATE9.`, `YYMMDD10.`, `YYMMDDN8.`, `MMDDYY10.`, `DDMMYY10.`; datetime `PUT`/`INPUT` support `DATETIME20.` and `E8601DT19.`; numeric `Zw.` and `w.d` formats/informats are also supported. User-defined `VALUE` formats work with `PUT`; simple exact-mapping `INVALUE` definitions work with `INPUT`; `PICTURE` supports simple digit masks with optional prefix/multiplier/rounding; `CNTLIN=` supports the standard control columns but collects rows to the driver and is only suitable for small tables. A `FORMAT var fmt.;` statement is display-only: values are unchanged and a warning is emitted, so use `PUT` where formatted text is needed. Date and datetime inputs remain numeric SAS values, not Spark `DATE`/`TIMESTAMP` columns. Advanced macro processing, advanced array behavior, other SAS-specific functions, and most non-SQL procedures are not implemented.

The `ORDER_BY=(...)` MERGE option described below is the supported exception to
the preceding statement that other MERGE dataset options are unsupported.

Macro support is a bounded compile-time expansion, not a complete SAS macro
processor. It supports sequential `%LET name=value` declarations, `&name`
substitution (including inside quoted strings), user-defined `%MACRO`/`%MEND`
definitions, positional and keyword parameters, default parameter values,
nested macro calls, and inline calls that expand to exactly one statement.
Macro variable names and macro names are case-insensitive; a period may
terminate a variable reference without being included in its value.
`%IF`/`%THEN`/`%ELSE` supports simple scalar comparisons and parenthesized
`AND`/`OR`/`NOT` expressions. `%DO` supports unconditional `%DO;` blocks and
integer-literal `%DO index=start %TO stop [%BY step];` loops; loop bounds can
use macro variables that expand to integers. `%DO %WHILE (condition);` and
`%DO %UNTIL (condition);` loops are supported with the same scalar conditions as
`%IF`. `%LET` values, `%IF` conditions and macro arguments may use `%EVAL(...)`
(integer arithmetic, comparisons, `AND`/`OR`/`NOT`) and `%SYSFUNC(function(...))`
for `UPCASE`, `LOWCASE`, `STRIP`, `TRIM`, `LEFT`, `LENGTH`, `ABS`, `INT`,
`CEIL`, `FLOOR`, `ROUND`, `MIN`, `MAX` and `MOD` (no format argument; clock and
other non-deterministic functions are rejected). Indirect references such as
`&&part&i` are resolved by repeated scanning, up to 10 passes. Variables changed
by `%LET` inside `%IF`/`%DO` blocks are visible to later statements in the same
macro (or program). Expansion is limited to 32 nested
levels, 1,000 loop iterations per loop, and 10,000 emitted statements.

`%UPCASE(text)`, `%LOWCASE(text)`, `%STR`, `%NRSTR`, `%QUOTE`, `%NRQUOTE`,
`%BQUOTE` and `%NRBQUOTE` are supported (the quoting functions simply return
their text; characters that would end a statement, such as `;`, are not
protected). `%SUBSTR(text, pos[, len])`, `%SCAN(text, n[, delimiters])`
(negative `n` counts from the end), `%INDEX(source, excerpt)` and
`%LENGTH(text)` operate on text resolved at conversion time. `%PUT` is
accepted and ignored. `%GLOBAL a b;` makes
variables assigned inside a macro visible to the caller afterwards, and
`%LOCAL a;` keeps them private to the macro. `%SYSEVALF(expr[, conversion])`
evaluates decimal arithmetic (conversions `BOOLEAN`, `CEIL`, `FLOOR`, `INTEGER`),
and `%SYMEXIST(name)` returns `1` or `0` for the variables defined at that
point. `%RETURN;` ends the current macro early. `%GOTO label;` jumps forward to
a `%label:` in the same macro (including out of `%IF`/`%DO` blocks); backward
jumps and missing labels are errors. `%ABORT` is rejected with an error when it
is reached, because it would stop the SAS session.

**Run-time macro variables.** `PROC SQL` `SELECT ... INTO :a, :b [TRIMMED]`
(first row of the result) and `CALL SYMPUT`/`SYMPUTX('name', expression)` in a
DATA step (including `data _null_;`, optionally as `IF cond THEN CALL SYMPUTX(...)`)
set values that only exist when the program runs. They are generated as Spark
SQL session variables (`DECLARE OR REPLACE VARIABLE sas_macro_<name>`), which
**requires Spark 4.0 or later**, and later `&name` references become that
variable inside expressions (`WHERE`, `IF`, assignments, `PROC SQL`). For
`CALL SYMPUT` the value comes from the last row that executes the call, in the
frame's current row order. The value keeps its Spark type (SAS would convert it
to text), and an empty result gives `NULL`. Run-time variables cannot be used in
`%LET`, `%IF`, `%DO`, `%EVAL`, macro arguments, quoted strings (`"&name"`), or as
dataset/column names (`SET &name`); these raise a conversion error. `SELECT ...
INTO ... SEPARATED BY`, a non-literal `CALL SYMPUT` name, and `CALL SYMPUT`
inside `DO`/`SELECT`/`ELSE` blocks are unsupported.

`%SYSGET(name)` can read an environment variable only when the converter is
created with `allow_sysget=True`; it is evaluated at conversion time, is disabled
by default, and errors if the name is not set. Macro recursion is supported up to
the 32-level nesting limit; an unbounded recursive call is rejected at that
limit. Nested macro calls inside arguments are supported when each call expands
to one statement. Dynamic `%INCLUDE` paths may use macro variables assigned by
preceding `%LET` statements, but not macro parameters or `%SYSFUNC` expressions.
Without `%GLOBAL`, macro variables assigned during a macro call
are scoped to that call and do not modify the caller's variables. Unsupported macro syntax
raises a conversion error rather than being passed through to generated Python.

The simple `SELECT;` form (a `WHEN` condition and one assignment per branch, all
targeting the same column, with `OTHERWISE` required) converts to a single `F.when`
chain. The `SELECT (expr)` value-match form, `WHEN (a, b)` lists, branches that are
`DO ... END` blocks with several statements, and `SELECT` nested in `IF`/`DO` blocks
are rewritten into guarded per-statement conditionals, like `IF ... THEN DO` blocks.

**Nested blocks.** `IF ... THEN DO` blocks may contain assignments, `DELETE`, nested
`IF`/`ELSE` and nested `SELECT`; each inner statement is guarded by the combined
condition of every enclosing branch. Conditions that read a column assigned inside the
same chain, iterative `DO` loops inside blocks, and `OUTPUT` inside blocks are rejected.

**Character and conditional `RETAIN`.** A retained variable that is not the
`v = v + expr` accumulator (for example `retain last ''; if x > 1 then last = name;`)
keeps its previous value on rows where no assignment runs, with the `RETAIN` initial
value (or missing) before the first assignment. It is computed with a window function
over one Spark partition. Without the DATA-step `ORDER BY` extension it uses
`monotonically_increasing_id()` and is best-effort on row order. The variable
cannot appear in its own assignment, the step cannot use `BY` or `MERGE`, and
the variable must be assigned before it is read. A retained variable that is
never assigned is rejected.

**`LENGTH` and `ATTRIB`.** `LENGTH v $ n` and `ATTRIB v LENGTH=$n` truncate assigned
character variables with `substring(v, 1, n)` after the step's assignments. Numeric
lengths, `LABEL=`, `FORMAT=`, `INFORMAT=` and `TRANSCODE=` are accepted and ignored.
A variable that is never assigned in the step is not created.

**Numeric accumulator `RETAIN`.** This supported form handles one or more numeric
variables initialized with numeric literals (`retain a b 0 c 10;`, repeated
`RETAIN` statements allowed), each updated exactly once using
`variable = variable + expression`; simple assignments may be mixed in. With a
`BY` statement, every accumulator variable
needs its own preceding reset of the form
`if first.key then variable = numeric-literal;`; each running total is
partitioned by the BY keys through `key`. The increment must evaluate to a
nonmissing numeric value. Because Spark DataFrames do not preserve input row
order, generated code uses `monotonically_increasing_id()` as a best-effort
order and returns a warning unless the DATA-step `ORDER BY` extension provides
unique keys. With explicit keys the cumulative order is deterministic for the
same input values; this still does not prove equivalence to SAS physical
observation order. Other numeric conditional updates/resets and
retained-state patterns are unsupported. Character/conditional retention is
limited to the narrower form described above (single SET, no BY/MERGE, no
self-reference).

**Report procedures.** `PROC PRINT` (`DATA=`, `OBS=`, `NOOBS`, `VAR`, `WHERE`) becomes `DataFrame.show()` (capped at 1000 rows when `OBS=` is absent; not SAS listing formatting). `PROC CONTENTS` supports `DATA=`, `OUT=`, `NOPRINT`, `SHORT` and `VARNUM`: it displays the Spark schema, or just column names with `SHORT`; `OUT=` creates a metadata DataFrame/view with `NAME`, `SPARK_TYPE`, `NULLABLE` and `POSITION` (Spark schema order), not the SAS `OUT=` schema. `PROC COPY IN= OUT=` requires a `SELECT` list and, because library prefixes are dropped, only aliases the views. `PROC COMPARE BASE= COMPARE=` prints row/column difference counts, not SAS's per-variable report. `TITLE`, `FOOTNOTE` and `ODS` are accepted and ignored with a warning. Dataset options and other statements on these procs are rejected; unsupported `PROC PRINT` flags are rejected rather than silently ignored.

**Deterministic DATA-step order extension.** Add `ORDER BY sequence, event_time DESC;` inside a SET DATA step to define a stable iteration key for order-sensitive windows. It applies to `LAG`/`DIF`, `_N_`, numeric and character `RETAIN`, and `FIRST.`/`LAST.` flags. The converter checks at execution that every key exists exactly once and that the key combination is unique; otherwise it raises an error instead of silently choosing a row order. This is a sas2pyspark extension, not a SAS DATA-step statement. It does not sort the resulting DataFrame or alter `SET` dataset-option `FIRSTOBS`/`OBS` selection. Without it, existing best-effort `monotonically_increasing_id()` behavior remains. Calls to `LAG`/`DIF` inside conditionals still do not reproduce SAS's lag-queue behavior.

**Deterministic MERGE input order extension.** Specify `ORDER_BY=(key1, key2 DESC)` on every input in the `MERGE` statement, for example `merge left(order_by=(left_seq)) right(in=found order_by=(right_seq descending));`. Each input is ordered independently within its BY groups. At execution, the converter verifies the order columns exist and that BY-key plus order-key combinations are unique in each input; missing or duplicate values fail explicitly. This is a sas2pyspark dataset-option extension, not standard SAS syntax. Without per-input `ORDER_BY`, duplicate-key pairing remains best-effort and a warning is emitted. `WHERE=` and other MERGE dataset options remain unsupported.

**`SET` dataset options.** `SET a(where=(...) firstobs=n obs=m keep=|drop= rename=(old=new))` is applied in this order: `WHERE`, `FIRSTOBS`/`OBS` (counted over the filtered rows; `OBS` is the last observation number), `KEEP`/`DROP`, `RENAME`. Other options are rejected. Row order is best effort unless the dataset is sorted by the source itself; the DATA-step `ORDER BY` extension does not change `FIRSTOBS`/`OBS` input selection.

**Inline data and `_N_`.** `data name; [infile datalines dlm='c' [dsd] [missover|truncover];]
input a b $ c; datalines; ... ; run;` (also `CARDS`) creates a DataFrame from the
rows with `spark.createDataFrame`. Only list input is supported: names, with `$` for
character variables; numbers become `double`, `.` or empty values become `NULL`, rows
with fewer values than variables are padded with missing values, and rows with more
are an error. Informats, column or pointer input (`@`, `:`, `1-10`), `INFILE` for
external files, quoted values with spaces in whitespace-delimited data, and extra
statements after `DATALINES` are unsupported. `data _null_;` with `DATALINES` produces
nothing. The automatic variable `_N_` is available in `SET`/`MERGE` steps as a row
number derived from `monotonically_increasing_id()`, so it is only as reliable as
Spark's row order without the DATA-step `ORDER BY` extension. `ORDER BY` does not
apply to MERGE.

DATA-step `ARRAY` declarations support `{n}`, `{*}` and multi-dimensional sizes such
as `{2,3}` (stored row-major), character arrays (`$` with an optional length), implicit
element names (`array s{3} $ 5;` creates `s1`-`s3`), and explicit element lists with
ascending numeric ranges such as `score1-score3`. Constant indexes, including simple
integer arithmetic such as `q{i+1}`, are translated to the corresponding Spark columns,
and `DIM`, `HBOUND` and `LBOUND` of a declared array become integer literals.
Iterative `DO i = a TO b [BY c]` loops without `OUTPUT` are unrolled when the bounds
are integer constants or `DIM(array)`, so `do i = 1 to dim(q); q{i} = ...; end;` works;
the index variable is left at its final SAS value. Unrolling is capped at 5000
statements. Variable bounds, `DO WHILE`/`DO UNTIL`, run-time array indexes,
`_TEMPORARY_` arrays and initial-value lists are rejected because they depend on
run-time values.

For a `SET` DATA step, one `BY` statement with simple column names enables
`FIRST.key` and `LAST.key` flags in supported expressions. Spark input order is
unspecified; by default the generated code assigns a best-effort order using
`monotonically_increasing_id()` and emits a warning. Add the sas2pyspark
`ORDER BY` extension to provide a unique stable key for deterministic window
results. Thus without the extension, flags identify one row per relevant BY
group for a given execution, but which row is first or last can differ from SAS
or change across executions. Descending BY keys and
`FIRST.`/`LAST.` processing with `RETAIN` beyond the single numeric reset
pattern described above is not supported. DATA-step BY statements accept
`ASCENDING` and `DESCENDING` modifiers, but Spark input order is still
best-effort; the modifiers do not make observation order equivalent to SAS.

`PROC MEANS` and `PROC SUMMARY` accept `DATA=` plus one or more statistic
options (`N`, `NMISS`, `SUM`, `MEAN`, `STD`, `STDERR`, `VAR`, `CV`, `MIN`,
`MAX`, `MEDIAN`, `RANGE`; with none given, SAS's defaults `N MEAN STD MIN MAX`
are used), one `VAR` statement, an optional `CLASS` statement, and an optional
`OUTPUT OUT=dataset` statement. Output aggregate columns are named
`<statistic>_<variable>` in lowercase (for example, `mean_salary`). An output
statistic can be renamed by listing one output name per `VAR` column, such as
`OUTPUT OUT=stats MEAN=avg_salary avg_units;`. The named statistic must also
appear on the PROC statement. `N` counts nonmissing values and `NMISS` counts
nulls per variable. `STD` and `STDERR` use sample standard deviation;
`STDERR` divides it by the square root of the nonmissing count. `VAR` is sample
variance, and `CV` is sample standard deviation divided by the mean times 100;
`CV` is null when the mean is zero or undefined. `VAR` columns must be numeric.
Missing (`NULL`) class values are excluded by default; the `MISSING` PROC option
includes them as a class level. Analysis-column missing values are handled as
nulls by Spark aggregates. Without `OUT=`, results are available as
`df_means_1`, `df_means_2`, etc. Other procedure options, multiple `VAR` or
`CLASS` statements, and other `OUTPUT` options are unsupported. `PROC UNIVARIATE data=ds; var ...;` is approximated by a
`PROC MEANS` with `N NMISS MEAN STD MIN MEDIAN MAX` (results in `df_means_N`) and a
warning; quantile tables, normality tests, histograms and other options are not
generated. Quartiles and percentiles (`Q1`, `Q3`, `P90`) are unsupported because SAS's
default percentile definition differs from Spark's. `PROC PRINT` supports only the
documented `DATA=`, `OBS=`, `NOOBS`, `VAR`, and `WHERE` subset described above.

`PROC FREQ` is limited to one `TABLES` specification per procedure, with one or
two simple column names and the optional `OUT=` option. Missing (`NULL`) values
in the table columns are excluded by default, matching the default SAS
behavior; the `MISSING` PROC option includes them as table levels. Multiple
tables, formats, weights, table-level options, and other `PROC FREQ` options
are unsupported. The output contains `COUNT` and `PERCENT`; two-way tables also
contain `ROW_PERCENT` and `COL_PERCENT`. With `OUT=`, the result is available as
the corresponding `df_<table>` DataFrame and Spark temp view. Without `OUT=`, it
is available as `df_freq_1`, `df_freq_2`, etc. Cumulative percentages and SAS
report formatting are not generated.

`PROC TRANSPOSE` requires an `ORDER=column` option (a sas2pyspark extension,
not a standard SAS PROC TRANSPOSE option) because Spark DataFrames do not
preserve SAS observation order. The order column must be unique globally, or
unique within each `BY` group. It emits one row per `VAR` column, stores the
source column name in `_NAME_`, and creates `COL1`, `COL2`, etc. in ascending
order within each BY group. BY columns are retained. All VAR columns must have
the same Spark data type, and at most 10,000 output COLn columns are allowed.
`ID`, `COPY`, `PREFIX`, `NAME=`, and other transpose options are unsupported.

`PROC IMPORT` supports CSV files only. It accepts `DATAFILE=`, `OUT=`,
`DBMS=CSV`, optional `GETNAMES=YES|NO` (default `YES`), optional one-character
`DELIMITER=` (default comma), and the `REPLACE` flag. The generated Spark reader
uses schema inference and `FAILFAST` parsing, and registers the imported data
as a temporary view. Inferred Spark types can differ from SAS types; validate
schemas on representative files. Other DBMS types, import options (including
`GUESSINGROWS=`), and SAS-specific import behavior are unsupported.

`PROC EXPORT` supports CSV only. It accepts `DATA=`, `OUTFILE=`, `DBMS=CSV`,
optional `PUTNAMES=YES|NO` (default `YES`), optional one-character
`DELIMITER=` (default comma), and the `REPLACE` flag. Without `REPLACE`, Spark
uses error-if-exists mode; with it, Spark overwrites the destination path.
Spark writes CSV output as a directory containing part files, not as one
single CSV file. Other DBMS types, export options, and SAS-specific file
behavior are unsupported.

## Examples

The [sales analysis example](examples/sales_analysis.sas) covers arrays,
`SELECT/WHEN`, sorting, output-producing `DO` loops, `PROC MEANS`, `PROC FREQ`,
`PROC TRANSPOSE`, `PROC SQL`, and `PROC EXPORT`. To run it in the GUI, upload
that SAS file and [sales.csv](examples/sales.csv); the file name `sales.csv`
provides the `work.sales` input dataset.

To test user-defined macro expansion, upload
[macro_customer_report.sas](examples/macro_customer_report.sas) and
[customers_macro.csv](examples/customers_macro.csv). The example covers
positional and keyword parameters, a default parameter, `%LET`, `%IF/%ELSE`,
and an integer `%DO` loop. The CSV base name `customers_macro` matches the
program's `SET work.customers_macro` input.

### Progressive GUI test programs

The `examples/` folder also contains five progressively more complex SAS
programs. For each level, upload the listed SAS file, click **Load uploaded SAS
file**, then **Convert to PySpark**. Upload every corresponding CSV under
**Test datasets** and choose **Run PySpark test**.

| Level | SAS program | CSV input file(s) | Main constructs |
|---|---|---|---|
| 1 — Basic | `examples/level1_basic.sas` | `examples/customers_l1.csv` | `SET`, `WHERE`, expressions, `KEEP`, `PROC SORT`, `PROC SQL`, export |
| 2 — Conditions | `examples/level2_conditions.sas` | `examples/customer_spend_l2.csv` | `%LET`, missing-value filter, `IF/ELSE`, `SELECT/WHEN`, `PROC FREQ`, `PROC MEANS` |
| 3 — Arrays and loops | `examples/level3_arrays_loops.sas` | `examples/assessments_l3.csv` | Arrays, indexed array references, output-generating `DO`, aggregates and SQL |
| 4 — Merge and reshape | `examples/level4_merge_transpose.sas` | `examples/accounts_l4.csv`, `examples/orders_l4.csv` | Multi-input `MERGE ... BY`, derived values, sort, `PROC TRANSPOSE`, frequency |
| 5 — Integrated | `examples/level5_integrated.sas` | `examples/profiles_l5.csv`, `examples/subscriptions_l5.csv`, `examples/payments_l5.csv` | Macros, three-input merge, arrays, classifications, sort, frequency, aggregates and SQL |
| 6 — Blocks and macro functions | `examples/level6_blocks_macros.sas` | `examples/applicants_l6.csv` | `IF` without `ELSE`, `ELSE IF`, `IF THEN DO`, `DELETE`, `%EVAL`, `%SYSFUNC`, `%DO %WHILE`, `&&` references |
| 7 — Dates and formats | `examples/level7_dates_formats.sas` | `examples/orders_l7.csv` | Date literals, `MDY`, `INTCK`, `INTNX`, `PUT`, `INPUT`, `FORMAT` |

For merge examples, upload all listed files together so their filename base
names match the SAS dataset names. The level 3 and level 4 examples use
constructs with ordering or type requirements documented in the supported
syntax section above; check any converter warnings and Spark errors shown by
the GUI.

## Migration project foundation

Create and validate a versioned project manifest before tracking migration work:

```powershell
sas2pyspark project init migration.project.json --name "Claims migration"
sas2pyspark project validate migration.project.json
sas2pyspark project discover migration.project.json --root .
sas2pyspark project plan migration.project.json --root .
sas2pyspark project convert migration.project.json --root .
```

Use `--sas-version auto` to resolve each program from its in-source marker or
default to the Viya profile. Pass `--sas-version 9.4M8` or `--sas-version Viya`
to set one source profile for the batch.

The JSON manifest uses schema version `1` and starts with empty `programs`,
`datasets`, `dependencies`, `conversions`, and `reviews` collections. The
`MigrationProject` Python API can add records to these collections and load,
validate, and save the manifest. Program IDs are deterministic from normalized
project-relative paths; dataset IDs from case-insensitive logical names;
dependency IDs from their endpoints and kind; conversion IDs from program,
source SHA-256, and converter version; and review IDs from program. A project
keeps one current summary review status per program. Detailed issue decisions
and their audit trail are kept in a separate review report. Reconciliation,
approvals, and readiness evidence are kept in separate versioned reports.

`project discover` scans without executing SAS code. It adds discovered `.sas`
files to the project manifest and writes a separate versioned JSON inventory
report (by default beside the manifest, with an `.inventory.json` suffix). The
report records project-relative paths, byte sizes, SHA-256 hashes, detected
source encodings, statement and DATA-step counts, PROC counts, and macro
statement counts. It also inventories companion files with common data/config
extensions, recording their paths, sizes, and hashes; these are candidates,
not yet verified dependencies. Files that cannot be read or decoded are listed
as issues rather than silently omitted. Common generated/vendor directories
are skipped by default; add more names or relative-path globs with repeated
`--exclude-dir` options. Specify a custom asset extension set with repeated
`--asset-extension` options. Discovered paths are relative to `--root`, which
should be the repository root used as the migration project’s path base.

`project plan` analyzes the project’s registered SAS programs without executing
them and writes a versioned JSON dependency graph and migration plan (by
default beside the manifest, with a `.plan.json` suffix). It detects literal
DATA-step outputs and SET/MERGE inputs, common PROC `DATA=`/`OUT=` datasets,
simple SQL `CREATE TABLE`/`FROM`/`JOIN` references, quoted `%INCLUDE` paths,
and macro calls that match one scanned macro definition. Dataset names without
a library are normalized to `WORK.name`. The plan records program-to-dataset
flows, inferred program prerequisites, deterministic topological order,
cycles, unproduced/external inputs, dynamic references, unsupported PROC
names, source hashes, and a heuristic complexity score. Cyclic programs and
programs blocked by their cycles receive no execution sequence. Complexity is a
prioritization signal, not a conversion-effort guarantee.

Analysis dependencies are refreshed in the project manifest using
`analysis:`-prefixed dependency kinds; project datasets are added as needed.
Existing non-analysis dependencies are retained. The plan report is still the
authoritative analysis output and includes unresolved references. External
input datasets are intentionally reported for classification rather than
assumed to exist. The command writes the report and updates the manifest even
when references or cycles remain, then returns status `1` to indicate that the
plan needs attention.

This is static, conservative analysis—not a full SAS parser. Dynamic macro
resolution, `LIBNAME` path resolution, dataset options that generate names,
complex macro expansion, dictionary tables, and nonliteral include paths
cannot be reliably inferred; inspect unresolved references and the graph
before using its order as an execution plan.

`project convert` calls the existing SAS converter separately for each
registered program. Successful outputs are written under `--output-dir`
(default `pyspark`) while preserving each source's relative path and replacing
`.sas` with `.py`. A separate versioned JSON report (default
`<manifest>.conversion.json`) maps every program ID and source path to its
source SHA-256, output path, status, warnings, and any conversion/read/write
error. Successful and failed conversions with a readable source hash are
recorded in the project manifest; unreadable or missing files remain visible in
the report without inventing a source hash. A failed or unsupported source does
not prevent other programs from being converted. The command returns status
`1` if any program fails and `2` for invalid workflow options or project-level
errors. Existing output files are not replaced unless `--force` is supplied.
The output directory must be a safe project-relative path under `--root`.

`project review` creates or refreshes a versioned report from conversion
warnings and errors. Warning issues are labeled `warning`; unsupported
conversion errors are labeled `unsupported_statement`, and other errors are
labeled `conversion_error`. Each issue identifies its program, source hash,
converter version, and generated output. The report is saved beside the
manifest with a `.review.json` suffix by default. Refreshing it retains prior
decisions and audit history.

Record a human decision using the issue ID from the report:

```powershell
sas2pyspark project review migration.project.json --root .
sas2pyspark project review-decision migration.project.json --root . `
  --issue "issue:..." --decision deferred --reviewer "reviewer@example.com" `
  --notes "Waiting for business confirmation."
sas2pyspark project review-decision migration.project.json --root . `
  --issue "issue:..." --decision corrected --reviewer "reviewer@example.com" `
  --corrected-code .\manual-fix.py
```

Decisions are `accepted`, `corrected`, `deferred`, or `rejected`. Each action
appends a timestamped event with the reviewer and optional notes. A corrected
Python file must parse successfully and is copied to a new uniquely named file
under `reviewed/`; neither the original generated output nor earlier reviewed
corrections are overwritten. This workflow does not generate AI suggestions.
Any future AI suggestions must be explicitly enabled and remain subject to
human review and approval.

`project approval` appends a timestamped governance decision to a separate
`.approvals.json` report. The default required gates are `business`, `data`,
`security`, and `platform`; configure a smaller policy by repeating
`--required-gate`. Each event records the reviewer, gate, decision, comments,
and evidence references. `unsupported_statement` and `conversion_error` review
issues are treated as critical; use `--critical-issue` to add other issue IDs
to the critical set. A gate decision is not a migration-wide approval: the
computed approval remains pending or blocked until every required gate approves
and every critical issue is accepted or corrected.

Record each required gate independently; the approval file retains every
decision in its event history:

```powershell
sas2pyspark project approval migration.project.json --root . `
  --gate business --decision approved --reviewer "business-owner" `
  --evidence "ticket:CLAIMS-42"
sas2pyspark project approval migration.project.json --root . `
  --gate data --decision approved --reviewer "data-owner" `
  --evidence .\evidence\data-validation.pdf
```

Decisions are `approved`, `changes_requested`, or `rejected`. Later decisions
append to the log and become the current gate status without erasing history.
The gate and critical-issue policy is fixed when the report is created; use a
new `--report` path to start a report with a different policy. Use
`--review-report` to link a non-default review report.

`project run` executes only explicitly selected programs with a successful
conversion matching the current SAS source hash. Each converted program runs
in a separate Python worker process. Runs are written to a unique
`runs/<run-id>/` directory containing `run.json`, per-program stdout/stderr
logs, Spark configuration, and a separate `outputs/` working directory. The
report records commands, environment metadata (but not environment override
values), input file hashes, exit status, timing, logs, and discovered outputs.
Failed or timed-out programs are reported while the remaining selections
continue. Each run also stores an immutable copy of the exact converted Python
file it executed, with its SHA-256 hash.

Provide data inputs as `DATASET=PATH`; paths must be relative to `--root` and
point to CSV, Parquet, or JSON files. The worker registers each file as a
Spark DataFrame variable such as `df_people` and a temporary view named
`people`. Relative outputs produced by generated code are written inside that
program's run-specific `outputs/` directory.

```powershell
sas2pyspark project run migration.project.json --root . `
  --program sas\claims.sas `
  --input WORK.people=data\people.csv `
  --master "local[2]" `
  --spark-conf spark.sql.shuffle.partitions=4 `
  --timeout 1800
```

The default master is `local[*]`; use `--spark-conf KEY=VALUE` repeatedly for
additional settings, `--env KEY=VALUE` for worker environment overrides, and
`--run-dir` to change the repository-relative run directory. Secret values
passed with `--env` are not copied into the report, though they are available
to the executed program; program output may still disclose them in logs. The
worker also inherits the parent process environment. Run reports are retained
as reproducibility evidence and include source, converted-code, and input-file
hashes.

Sensitive Spark settings (for example credentials or secret keys) must use an
environment reference such as `${S3_SECRET}` as the setting value; the
reference, not its resolved value, is retained in run configuration and
reports. Set the referenced variable in the parent environment. Avoid passing
secrets as `--env` command-line values because shell history or process
inspection may expose them.

**Security boundary:** a local subprocess provides process separation and a
timeout, not a security sandbox. Converted Python executes with the current
user's filesystem and network permissions and inherits the parent environment.
Only run code you trust; use an externally managed container or VM when a
stronger isolation boundary is required.

`project reconcile` compares configured SAS reference datasets with Spark
outputs and writes a versioned JSON report. Install the optional comparison
dependencies, including the native SAS reader, with:

```powershell
py -m pip install -e ".[reconcile]"
```

Provide matching `DATASET=PATH` pairs for `--reference` and `--spark-output`.
Supported formats are CSV, Parquet, JSON, native SAS7BDAT, and SAS XPORT
(`.xpt`). Spark output directories (including the directory produced by a
Spark CSV export) are accepted. Paths are relative to `--root`.

```powershell
sas2pyspark project reconcile migration.project.json --root . `
  --reference WORK.RESULTS=reference\results.sas7bdat `
  --spark-output WORK.RESULTS=runs\<run-id>\program-001\outputs\results.csv `
  --keys WORK.RESULTS=customer_id,period `
  --rtol 1e-6 --atol 1e-8
```

With keys, rows are paired by case-insensitive key columns and comparison
ignores row ordering; duplicate keys make value comparison inconclusive. With
no keys, rows are compared as an exact unordered multiset, preserving duplicate
row counts. When no keys are configured and a nonzero numeric tolerance is
requested, the result is inconclusive because tolerant row pairing is
ambiguous. Schema names are matched case-insensitively and numeric Spark types
are treated as one family; schema details, row counts, per-column null counts,
key uniqueness, missing keys, differing values, and bounded examples appear
in the report. Floating-point comparisons use
`abs(actual - reference) <= atol + rtol * abs(reference)`.

Reports distinguish `pass`, `fail`, and `inconclusive`; the CLI exits `1` for
any failed or inconclusive dataset. Reports include comparison settings and
input hashes, and are saved under a unique `reconciliation/` path by default.
Native SAS files are decoded by pyreadstat into the driver process, so ensure
the machine has sufficient memory for those reference datasets. Dataset
reading or schema errors are reported as inconclusive rather than treated as
successful comparisons.

`project readiness` links the inventory, dependency, conversion, review,
execution, reconciliation, and approval reports into a versioned evidence
manifest. It records each report's path, SHA-256, summary, and validation
status, and hashes the canonical evidence list. Inventory, plan, conversion,
review, and approval reports default to the matching suffix beside the project
manifest. Link execution and reconciliation reports explicitly; repeat either
option when including multiple reports:

```powershell
sas2pyspark project readiness migration.project.json --root . `
  --execution-report runs\<run-id>\run.json `
  --reconciliation-report reconciliation\<reconciliation-id>.json
```

Override any linked report with `--inventory`, `--dependency`, `--conversion`,
`--review-report`, or `--approval-report`. Missing or malformed evidence is
reported as a blocker rather than treated as success. Readiness is `ready` only
when evidence is present and consistent, dependency analysis has no unresolved
references or cycles, every registered program converted and executed
successfully with source hashes matching the repository inventory, all linked
reconciliation comparisons pass, critical review findings are resolved, and
all required governance gates approve. The report
is written to a unique file under `readiness/` by default; the command returns
exit status `1` when blockers remain. Re-running readiness against the same
report files produces the same evidence-manifest hash; changing a linked file
changes its recorded digest.

Or pipe source through standard input:

```powershell
Get-Content input.sas -Raw | sas2pyspark -
```

## Generated-code conventions and caveats

- Input datasets are expected to be DataFrames named `df_<table>`, such as `df_people`.
- Output datasets become DataFrames named `df_<table>` and are registered as Spark temporary views under `<table>`.
- For two-level SAS references (`library.table`), the generated view name is the table component. Ensure table names are unique across libraries or adapt the naming convention.
- `PROC SQL` queries are passed to `spark.sql`; SQL is not generally rewritten beyond SAS comparison words, `CALCULATED` aliases and removal of library qualifiers. Unsupported SAS SQL constructs may therefore fail when executed by Spark.
- `PROC SORT` supports `ASCENDING` and `DESCENDING` BY-key modifiers.
`PROC SORT NODUPKEY` uses a window to keep one row per BY key. Which tied row
is retained can differ from SAS; the converter emits a warning.
- Generated programs assume that `spark` and referenced input DataFrames exist. Directly executing generated files requires you to provide them; `project run` creates a local Spark session and binds `--input` datasets. Generated programs import `pyspark.sql.functions as F`, so PySpark must be installed in the selected Python environment.

## Performance check

A local, synthetic Spark check exercised the generated filter/assignment/aggregation
pipeline (filter `age >= 18`, derive `adult_flag` and `age_band`, then aggregate
counts and flag sums). With Python 3.11.9, PySpark 4.3.0, Java 17.0.20.1,
`local[2]`, and two shuffle partitions, cached `Spark.range` inputs were warmed
and measured three times:

| Input rows | Output rows | Median elapsed |
|---:|---:|---:|
| 100,000 | 79,992 | 0.429 s |
| 1,000,000 | 799,992 | 0.310 s |

Conversion took about 7.84 ms and generated 384 characters. This is a
reproducibility snapshot, not a benchmark suite or a production throughput
claim: the run is local, synthetic, cached, and sensitive to Spark startup,
warm-up, scheduling, and host load. The lower median at the larger input size
shows the measurements are noisy; benchmark representative uncached data and
your target cluster before making capacity or scaling decisions.

## Run tests

The unit tests need only Python. To also run the generated-code integration test
against a local Spark session, install the optional test dependency:

```powershell
py -m pip install -e ".[test]"
py -m unittest discover -s tests -v
```

The Spark integration test executes a sample DATA step, PROC SORT, and PROC SQL
conversion against small in-memory DataFrames and checks the resulting rows and
aggregates. It skips with a clear message if PySpark or a compatible Java
runtime is unavailable. PySpark 4 requires Java 17 or newer.

### Windows Spark worker troubleshooting

The verified local Windows test setup for this project is Python 3.11,
PySpark 4.3.0, and Java 17. A Python 3.13/PySpark 4.0.1 combination in one
Windows environment failed to start Spark Python workers (`WinError 10038` /
`EOFException`); the converter itself was not the cause. If you see that
failure, try the known-working interpreter pairing and remove machine-specific
Hadoop classpath overrides for this local test session:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
$python = (py -3.11 -c "import sys; print(sys.executable)").Trim()
$env:PYSPARK_PYTHON = $python
$env:PYSPARK_DRIVER_PYTHON = $python
Remove-Item Env:HADOOP_USER_CLASSPATH_FIRST -ErrorAction SilentlyContinue
Remove-Item Env:HADOOP_CONF_DIR -ErrorAction SilentlyContinue
py -3.11 -m unittest discover -s tests -v
```

Check the installed PySpark version with
`py -3.11 -c "import pyspark; print(pyspark.__version__)"`. These environment
changes apply only to the current PowerShell session.
