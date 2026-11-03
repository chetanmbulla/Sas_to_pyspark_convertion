# Conversion Process

This guide explains how `sas2pyspark` turns supported SAS source into generated
PySpark, and how the generated program is prepared and checked when you run a
test. Conversion and execution are separate operations: converting source
creates Python code, while the GUI or project runner starts Spark and executes
that code.

## End-to-end flow

```text
SAS source
   |
   v
Lex source, retain spans, and split statements
   |
   v
Expand supported sequential %LET variables
   |
   v
Parse DATA steps and supported PROC blocks
   |
   v
Translate expressions and render PySpark
   |
   +----> generated Python and conversion warnings
               |
               v
       Syntax check and code review
               |
               v
       Load input files into Spark DataFrames
               |
               v
       Execute in a separate worker process
               |
               v
       Status, logs, output files, and previews
```

## 1. Read and prepare the SAS source

The converter receives SAS text from the CLI, Python API, or GUI. Before
parsing:

1. Select the SAS source profile. A block comment of the form
   `/* sas2pyspark: sas-version=9.4M8 */` or
   `/* sas2pyspark: sas-version=Viya */` identifies the source profile.
   `--sas-version` or the equivalent API/GUI option can provide an override;
   conflicting markers and overrides fail explicitly. Without either, the
   converter selects the Viya profile and warns that the source release is
   unknown.
2. The source-aware lexer masks SAS block comments (`/* ... */`) and statement
   comments (`* ... ;`) while preserving source offsets and quoted text.
3. It tokenizes identifiers, numbers, strings, operators, punctuation, and
   macro references/directives, then splits statements at semicolons outside
   quoted strings. Statement and token spans retain original line and column
   positions for diagnostics.
4. Every statement must be terminated with a semicolon, and quoted strings and
   comments must be closed.

The version profile currently records source-version selection and checks
marker/override consistency. It does not yet gate every syntax feature by
release or reproduce version-specific SAS runtime behavior. Legacy syntax is
accepted only where it matches the converter's implemented subset.

An unterminated comment or quote, or a statement without a terminating
semicolon, fails conversion rather than being silently ignored.

This is a lexical and statement-parsing foundation, not a complete SAS grammar:
DATA-step and PROC semantics are still parsed by the existing bounded
construct-specific converters.

## 2. Expand supported macro variables

`%LET name=value;` definitions are evaluated sequentially. User-defined
`%MACRO`/`%MEND` definitions can declare positional or keyword parameters with
defaults; a call expands the macro body with those arguments. Macro calls can
be nested, and an inline call inside a SAS statement must expand to exactly one
statement. Macro variables assigned during a macro call are scoped to that
call. References such as `&name` are substituted with their value, including
inside quoted strings. Names are case-insensitive, and a period can terminate
a variable reference without becoming part of its value.

For example:

```sas
%let minimum_age = 18;
where age ge &minimum_age;
```

is parsed as if the filter were written `where age ge 18;`.

The processor also evaluates simple compile-time `%IF`/`%THEN`/`%ELSE`
conditions, including scalar comparisons and parenthesized `AND`/`OR`/`NOT`
expressions. It supports unconditional `%DO; ... %END;` blocks and integer
`%DO index=start %TO stop [%BY step]; ... %END;` loops. Loop bounds may use
macro variables if they expand to integer literals. Expansion is limited to
32 nested levels, 1,000 iterations per loop, and 10,000 emitted statements to
prevent accidental expansion blowups.

This is not a full SAS macro processor. Macro arithmetic and macro functions
(such as `%EVAL`, `%SYSFUNC`, `%UPCASE`, and quoting functions), indirect
references, `%GLOBAL`/`%LOCAL`, `%GOTO`, `%RETURN`, and dynamic loop bounds are
unsupported. Undefined variables and unsupported macro syntax raise a
conversion error.

## 3. Identify and parse each statement block

The converter walks the prepared statements in source order. It recognizes
supported `DATA` steps and supported procedures, including `PROC SORT`,
`PROC FREQ`, `PROC MEANS`/`PROC SUMMARY`, `PROC TRANSPOSE`, `PROC IMPORT`,
`PROC EXPORT`, and `PROC SQL`.

Within a `DATA` step, the converter gathers its output dataset, input dataset or
datasets, filters, `BY` keys (including per-key `ASCENDING`/`DESCENDING`
modifiers), array declarations, assignments, conditionals, and supported
`KEEP`/`DROP` rules. Conditional assignments and `SELECT/WHEN`
branches are checked against the supported simple-assignment patterns. A
supported `DO` loop must use integer literal bounds and step, simple
assignments, and one final `OUTPUT`.

For a `MERGE`, the converter requires multiple inputs and a `BY` statement.
The generated program checks that each input has unique BY keys before joining.

Each `PROC` parser validates its required options and statements. For example,
`PROC IMPORT` and `PROC EXPORT` support CSV rather than arbitrary SAS DBMS
types, and `PROC TRANSPOSE` requires an `ORDER=` column.

## 4. Translate expressions

Supported SAS expressions are normalized for Spark SQL. Examples include:

| SAS expression | Generated Spark SQL expression |
|---|---|
| `age ge 18` | `age >= 18` |
| `salary ne 0` | `salary != 0` |
| `not missing(salary)` | `NOT salary IS NULL` |
| `upcase(first_name)` | `upper(first_name)` |
| `today()` | `current_date()` |

Supported functions such as `CATX`, `ROUND`, and `SUBSTR` have dedicated
translation rules. Quoted values are kept distinct from unquoted identifiers
while operators and functions are rewritten.

This translation is limited to the supported SAS subset. In particular,
`PROC SQL` is passed to Spark SQL with limited normalization; the converter is
not a general SAS SQL dialect translator.

## 5. Generate PySpark code and warnings

Generated code starts with a marker comment and imports
`pyspark.sql.functions as F`. It then emits DataFrame transformations and
procedure-specific Spark operations in the order of the SAS statements.

Common generated patterns include:

| SAS operation | Typical PySpark result |
|---|---|
| `SET work.people` | Read from an input DataFrame named `df_people` |
| `WHERE condition` | Filter rows with a Spark SQL expression |
| Assignment | Add or replace a DataFrame column |
| `KEEP`/`DROP` | Select or remove DataFrame columns |
| `PROC SORT` | Order rows by the requested columns |
| `PROC SQL` `CREATE TABLE` | Run `spark.sql(...)`, assign `df_<table>`, and register a temporary view |
| `PROC EXPORT` | Write a Spark CSV output directory |

Output SAS datasets are represented as DataFrames named `df_<table>` and are
registered as temporary Spark views. A successful conversion returns
`ConversionResult.code` and any non-fatal notes in `ConversionResult.warnings`.

Warnings matter: some SAS behavior has no exact match in unordered Spark
DataFrames. For example, `RETAIN` and `FIRST.`/`LAST.` behavior may rely on
best-effort row ordering, and `PROC SORT NODUPKEY` may keep a different tied
row than SAS.

## 6. Handle unsupported input

When a top-level statement or a construct inside a supported block is outside
the implemented subset, conversion raises `ConversionError`. When available,
the error includes the SAS source line and statement context. Conversion
errors are not replaced with guessed or pass-through translations.

Check [Supported syntax](../README.md#supported-syntax) and its limitations before
reworking a failed statement. Simplify the SAS source into supported forms or
manually review and translate the unsupported behavior; do not assume that
similar-looking SAS syntax has identical Spark semantics.

## 7. Check and run the generated program

### GUI test flow

The GUI's **Check Python syntax** action verifies that the generated text parses
as Python. It does not initialize Spark or prove that columns, types, or
business logic are correct.

For **Run PySpark test**, the GUI:

1. Checks the edited code's Python syntax.
2. Associates each uploaded CSV, Parquet, JSON, or JSONL file with a dataset
   named after the file's base name. For example, `people.csv` becomes
   `df_people`.
3. Writes the inputs and generated script into a temporary test project.
4. Creates a local Spark session in a separate worker process and loads the
   inputs. CSV input uses a header, inferred schema, and fail-fast parsing.
5. Runs the converted program with `spark` and the input DataFrames in its
   execution namespace. The Spark session is stopped when the program exits.
6. Displays success, failure, or timeout status, elapsed time, logs, and
   previews of CSV files written by the program.

The GUI deletes its temporary project after the test. A successful run only
shows that the program completed for those inputs; it does not prove equivalence
to SAS or production-scale performance.

### CLI/project runner

For tracked projects, `sas2pyspark project run` similarly loads explicitly
mapped inputs, executes selected current conversions in separate worker
processes, and saves run reports, logs, and outputs beneath the repository's
`runs/` directory. See the [migration project workflow](../README.md#migration-project-foundation)
for its configuration and security details.

## 8. Validate semantics and results

Before using generated code beyond a small test:

1. Review every conversion warning and the generated Python.
2. Test representative data, including missing values, boundary conditions,
   duplicate keys, and empty inputs where relevant.
3. Check expected columns, inferred Spark data types, row counts, null
   handling, grouping, and output values.
4. Compare results with trusted SAS outputs when available. The project
   reconciliation workflow can compare reference datasets with Spark outputs.
5. Test larger data volumes separately before making performance decisions.

Spark is lazy: some expression and schema errors appear only when an action
such as a write or collect is executed. A syntax check alone cannot catch
those runtime errors.

## Worked example

SAS source:

```sas
data work.adults;
    set work.people;
    where age ge 18;
    full_name = upcase(first_name);
    keep customer_id full_name age;
run;
```

The generated code follows the same sequence: reference `df_people`, filter
rows where `age >= 18`, add `full_name` using `upper(first_name)`, select the
three kept columns, and register the result as `df_adults` and the Spark view
`adults`. The exact code formatting may vary with the conversion rules and
future framework versions.

## Related documentation

- [Installation and usage](../README.md#install)
- [Supported SAS syntax and limitations](../README.md#supported-syntax)
- [Progressive SAS and CSV test examples](../README.md#progressive-gui-test-programs)
- [Project execution and reconciliation](../README.md#migration-project-foundation)
