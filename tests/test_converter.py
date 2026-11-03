import unittest

from sas2pyspark import ConversionError, SasToPySparkConverter


class ConverterTests(unittest.TestCase):
    def setUp(self):
        self.converter = SasToPySparkConverter()

    def test_data_step_assignments_filters_and_keep(self):
        result = self.converter.convert(
            """
            data work.adults;
              set work.people;
              where age ge 18;
              if active = 1;
              full_name = upcase(first);
              keep full_name age;
            run;
            """
        )
        self.assertIn("df_adults = df_people", result.code)
        self.assertIn("F.expr('age >= 18')", result.code)
        self.assertIn("F.expr('active = 1')", result.code)
        self.assertIn("F.expr('upper(first)')", result.code)
        self.assertIn(".select(*['full_name', 'age'])", result.code)
        self.assertIn("createOrReplaceTempView('adults')", result.code)
        self.assertTrue(
            any(
                "source version was not declared" in warning
                for warning in result.warnings
            )
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_set_concatenates_multiple_datasets_with_missing_columns(self):
        result = self.converter.convert(
            """
            data work.all_records;
              set work.first_records second_records;
            run;
            """
        )

        self.assertIn("df_all_records = df_first_records", result.code)
        self.assertIn(
            "df_all_records = df_all_records.unionByName("
            "df_second_records, allowMissingColumns=True)",
            result.code,
        )
        self.assertTrue(
            any("Multiple SET inputs" in warning for warning in result.warnings)
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_set_rejects_duplicate_dataset_references(self):
        with self.assertRaises(ConversionError) as context:
            self.converter.convert("data out; set source work.source; run;")

        self.assertIn("SET input datasets must have distinct names", str(context.exception))

    def test_sas_version_marker_selects_legacy_profile(self):
        result = self.converter.convert(
            "/* sas2pyspark: sas-version=9.4M8 */ "
            "data out; set source; run;"
        )
        self.assertEqual(result.source_version, "9.4M8")
        self.assertEqual(result.version_source, "marker")
        self.assertTrue(
            any("selected by source marker" in warning for warning in result.warnings)
        )

    def test_sas_version_marker_text_inside_a_string_is_ignored(self):
        result = self.converter.convert(
            "data out; set source; note='/* sas2pyspark: sas-version=9.4M8 */'; run;"
        )
        self.assertEqual(result.source_version, "Viya")
        self.assertEqual(result.version_source, "default")

    def test_sas_version_override_and_default_are_reported(self):
        overridden = self.converter.convert(
            "data out; set source; run;", sas_version="9.4m8"
        )
        self.assertEqual(overridden.source_version, "9.4M8")
        self.assertEqual(overridden.version_source, "override")

        defaulted = self.converter.convert("data out; set source; run;")
        self.assertEqual(defaulted.source_version, "Viya")
        self.assertEqual(defaulted.version_source, "default")
        self.assertTrue(
            any(
                "source version was not declared" in warning
                for warning in defaulted.warnings
            )
        )

    def test_sas_version_marker_conflicts_and_unknown_profiles_are_rejected(self):
        invalid_sources = (
            (
                "/* sas2pyspark: sas-version=9.4M8 */ "
                "data out; set source; run;",
                {"sas_version": "Viya"},
                "conflicts with source marker",
            ),
            (
                "/* sas2pyspark: sas-version=unknown */ "
                "data out; set source; run;",
                {},
                "Unsupported SAS version in sas2pyspark source marker",
            ),
            (
                "/* sas2pyspark: sas-version=9.4M8 */ "
                "/* sas2pyspark: sas-version=Viya */ "
                "data out; set source; run;",
                {},
                "conflicting sas2pyspark version markers",
            ),
            (
                "/* sas2pyspark: sas-version= */ data out; set source; run;",
                {},
                "Malformed sas2pyspark SAS version marker",
            ),
            (
                "data out; set source; run;",
                {"sas_version": "9.3"},
                "Unsupported SAS source version",
            ),
        )
        for source, options, expected_message in invalid_sources:
            with self.subTest(source=source, options=options):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source, **options)
                self.assertIn(expected_message, str(context.exception))

    def test_simple_macro_variables_expand_sequentially(self):
        result = self.converter.convert(
            """
            %let source=work.people;
            %let minimum_age=18;
            %let output_name=adults;
            data &output_name.;
              set &source.;
              where age ge &minimum_age;
              label = '&output_name';
            run;
            """
        )
        self.assertIn("df_adults = df_people", result.code)
        self.assertIn("F.expr('age >= 18')", result.code)
        self.assertIn("F.expr(\"'adults'\")", result.code)
        self.assertNotIn("%let", result.code.lower())
        self.assertNotIn("&output_name", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_macro_processing_rejects_unsupported_or_undefined_variables(self):
        unsupported_sources = (
            (
                "%let source=people; data out; set &missing; run;",
                "Undefined macro variable",
            ),
            (
                "%if 1 %then %do; data out; set input; run;",
                "%DO block is missing %END",
            ),
            (
                "%let source=people; data out; set &&missing&source; run;",
                "Undefined macro variable",
            ),
            (
                "%let source=%sysget(HOME); data out; set &source; run;",
                "Macro calls and macro-language statements",
            ),
            (
                "%let n=%eval(1.5+1); data out; set input; run;",
                "integer arithmetic only",
            ),
            (
                "%let n=%sysfunc(today()); data out; set input; run;",
                "unsupported",
            ),
            (
                "%let i=0; %do %while (&i lt 1); %end; data out; set input; run;",
                "iteration limit",
            ),
            (
                "%let =people; data out; set input; run;",
                "%LET name=value",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_macro_definitions_calls_and_named_parameters_expand(self):
        result = self.converter.convert(
            """
            %macro create_adults(source, target=adults, minimum_age=18);
              data work.&target;
                set work.&source;
                where age ge &minimum_age;
              run;
            %mend;
            %create_adults(people, target=adults_21, minimum_age=21);
            """
        )

        self.assertIn("df_adults_21 = df_people", result.code)
        self.assertIn("F.expr('age >= 21')", result.code)
        self.assertNotIn("%macro", result.code.lower())
        self.assertNotIn("%create_adults", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_nested_macro_calls_and_inline_macro_expansion(self):
        result = self.converter.convert(
            """
            %macro input_name(name);
              &name;
            %mend;
            %macro make_table(source, target);
              data work.&target;
                set work.%input_name(&source);
              run;
            %mend;
            %make_table(people, adults);
            """
        )

        self.assertIn("df_adults = df_people", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_macro_if_else_blocks_and_integer_do_loops(self):
        result = self.converter.convert(
            """
            %macro make_tables(source, stop=3);
              %if &stop ge 2 and (&stop le 3) %then %do;
                %if &stop eq 2 %then %do;
                  %do index=1 %to &stop;
                    data work.output&index;
                      set work.&source;
                      sequence = &index;
                    run;
                  %end;
                %end;
              %end;
              %else %do;
                data work.fallback;
                  set work.&source;
                run;
              %end;
            %mend;
            %make_tables(people, stop=2);
            """
        )
        fallback = self.converter.convert(
            """
            %macro make_tables(source, stop=3);
              %if &stop ge 2 and (&stop le 3) %then %do;
                %do index=1 %to &stop;
                  data work.output&index;
                    set work.&source;
                    sequence = &index;
                  run;
                %end;
              %end;
              %else %do;
                data work.fallback;
                  set work.&source;
                run;
              %end;
            %mend;
            %make_tables(people, stop=1);
            """
        )

        self.assertIn("df_output1 = df_people", result.code)
        self.assertIn("df_output2 = df_people", result.code)
        self.assertNotIn("df_fallback", result.code)
        self.assertIn("F.expr('2')", result.code)
        self.assertIn("df_fallback = df_people", fallback.code)
        self.assertNotIn("df_output1", fallback.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_macro_eval_sysfunc_while_until_and_indirect_references(self):
        result = self.converter.convert(
            """
            %let base=work.people;
            %let count=%eval(2 * (3 + 1) - 5);
            %let name=%sysfunc(upcase(adults));
            %let part1=alpha;
            %let part2=beta;
            %let i=1;
            %let total=0;
            %do %while (&i le 3);
              %let total=%eval(&total + &i);
              %let i=%eval(&i + 1);
            %end;
            %let j=1;
            %do %until (&j ge 2);
              %let j=%eval(&j + 1);
            %end;
            %do k=1 %to 2;
              data work.&&part&k;
                set &base;
                tag = &count + &total + &j + %sysfunc(max(1, 7)) + %sysfunc(round(3.6));
              run;
            %end;
            """
        )
        self.assertIn("df_alpha = df_people", result.code)
        self.assertIn("df_beta = df_people", result.code)
        self.assertIn("F.expr('3 + 6 + 2 + 7 + 4')", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_macro_if_simple_actions_and_else_branch(self):
        result = self.converter.convert(
            """
            %let source=people;
            %if 0 %then %let target=unused;
            %else %let target=selected;
            data work.&target;
              set work.&source;
            run;
            """
        )

        self.assertNotIn("df_unused", result.code)
        self.assertIn("df_selected", result.code)

    def test_macro_expansion_rejects_recursion_and_unbounded_loops(self):
        unsupported_sources = (
            (
                "%macro recursive(); %recursive(); %mend; %recursive();",
                "Recursive macro call",
            ),
            (
                "%macro loop(); %do i=1 %to 1001; data work.out; set input; run; %end; %mend; %loop();",
                "iteration limit",
            ),
            (
                "%macro loop(); %do i=1 %to &stop; data work.out; run; %end; %mend; %loop();",
                "Undefined macro variable",
            ),
            (
                "%macro make(name); data work.&name; run; %mend; %make();",
                "Missing required parameter",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_retain_generates_numeric_running_total_and_warning(self):
        result = self.converter.convert(
            """
            data running;
              retain total 10;
              set amounts;
              total = total + amount;
            run;
            """
        )
        self.assertIn("F.monotonically_increasing_id()", result.code)
        self.assertIn("F.sum(F.col('__sas2pyspark_retain_delta')).over(", result.code)
        self.assertIn("F.lit(10.0)", result.code)
        self.assertEqual(
            sum(
                not warning.startswith("SAS version profile:")
                for warning in result.warnings
            ),
            1,
        )
        self.assertTrue(
            any(
                "Spark does not guarantee SAS observation order" in warning
                for warning in result.warnings
            )
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_retain_generates_by_group_reset_running_total(self):
        result = self.converter.convert(
            """
            data running;
              retain total 10;
              set amounts;
              by region department;
              if first.department then total = 0;
              total = total + amount;
            run;
            """
        )
        self.assertIn(
            "partitionBy(F.col('region'), F.col('department')).orderBy(",
            result.code,
        )
        self.assertIn("F.lit(0.0) + ", result.code)
        self.assertIn(
            "F.sum(F.col('__sas2pyspark_retain_delta')).over(",
            result.code,
        )
        self.assertEqual(
            sum(
                not warning.startswith("SAS version profile:")
                for warning in result.warnings
            ),
            2,
        )
        self.assertTrue(any("SAS observation order" in warning for warning in result.warnings))
        self.assertTrue(any("row order is unspecified" in warning for warning in result.warnings))
        compile(result.code, "<generated-pyspark>", "exec")

    def test_retain_multiple_declarations_and_rejections(self):
        result = self.converter.convert(
            "data o; retain a b 0; retain c 5; set s; a = a + x; b = b + 1; c = c + x; run;"
        )
        self.assertEqual(result.code.count("F.sum("), 3)
        compile(result.code, "<generated-pyspark>", "exec")
        for source, message in (
            ("data o; retain a 0 a 1; set s; a = a + 1; run;", "more than once"),
            ("data o; retain a b 0; set s; a = a + 1; run;", "accumulator assignment: b"),
            ("data o; retain 0; set s; run;", "must follow a variable"),
            ("data o; retain a 0 b; set s; a = a + 1; run;", "numeric literal initial"),
        ):
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(message, str(context.exception))

    def test_retain_rejects_unmodeled_state(self):
        unsupported_sources = (
            (
                "data out; retain total; set source; total=total+amount; run;",
                "numeric literal initial value",
            ),
            (
                "data out; retain total 0; set source; total=total-amount; run;",
                "variable = variable + expression",
            ),
            (
                "data out; retain total 0; set source; total=total+amount; if active; run;",
                "accumulator and simple assignment",
            ),
            (
                "data out; retain total 0; set source; run;",
                "one supported accumulator assignment",
            ),
            (
                "data out; retain total 0; set source; by group; total=total+amount; run;",
                "requires an IF FIRST.",
            ),
            (
                "data out; retain total 0; set source; by group; if first.other then total=0; total=total+amount; run;",
                "is not a variable in the BY statement",
            ),
            (
                "data out; retain total 0; set source; by group; if first.group then total=reset; total=total+amount; run;",
                "numeric literal reset value",
            ),
            (
                "data out; retain total 0; set source; by group; total=total+amount; if first.group then total=0; run;",
                "must precede the accumulator assignment",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_array_references_expand_to_explicit_columns(self):
        result = self.converter.convert(
            """
            data calculated;
              set scores;
              array marks[3] mark1-mark3;
              total = marks[1] + marks[3];
              marks[2] = marks[1] + 1;
              if marks[2] gt 0;
            run;
            """
        )
        self.assertIn("F.expr('mark1 + mark3')", result.code)
        self.assertIn("withColumn('mark2', F.expr('mark1 + 1'))", result.code)
        self.assertIn(".filter(F.expr('mark2 > 0'))", result.code)
        self.assertNotIn("marks[", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_arrays_reject_dynamic_indexes_and_invalid_declarations(self):
        unsupported_sources = (
            (
                "data out; set source; array x[2] x1 x2; value=x[index]; run;",
                "positive integer literal index",
            ),
            (
                "data out; set source; array x[2] x1 x2; value=x[3]; run;",
                "outside 1..2",
            ),
            (
                "data out; set source; array x[3] x1 x2; run;",
                "lists 2 elements",
            ),
            (
                "data out; set source; value=x[1]; run;",
                "requires a preceding supported ARRAY declaration",
            ),
            (
                "data out; set source; array x[2] x1-x3; run;",
                "declares size 2 but lists 3 elements",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_by_first_and_last_generate_group_flags(self):
        result = self.converter.convert(
            """
            data flagged;
              set source;
              by region department;
              first_region = first.region;
              first_department = first.department;
              last_region = last.region;
              last_department = last.department;
            run;
            """
        )
        self.assertIn("partitionBy(F.col('region')).orderBy(", result.code)
        self.assertIn(
            "partitionBy(F.col('region'), F.col('department')).orderBy(",
            result.code,
        )
        self.assertIn("partitionBy(F.col('region'), F.col('department')).orderBy(", result.code)
        self.assertIn(
            "partitionBy(F.col('region'), F.col('department')).orderBy(",
            result.code,
        )
        self.assertIn("'__sas2pyspark_first_region'", result.code)
        self.assertIn("'__sas2pyspark_last_department'", result.code)
        self.assertTrue(
            any("row order is unspecified" in warning for warning in result.warnings)
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_by_first_and_last_require_a_matching_by_statement(self):
        unsupported_sources = (
            (
                "data out; set source; value=first.group; run;",
                "requires a preceding BY statement",
            ),
            (
                "data out; set source; by group; value=first.other; run;",
                "is not a variable in the BY statement",
            ),
            ("data out; set source; by descending; run;", "must be followed by one column"),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_by_first_and_last_accept_ascending_and_descending_keys(self):
        result = self.converter.convert(
            """
            data out;
              set source;
              by descending region ascending department;
              is_first = first.region;
              is_last = last.department;
            run;
            """
        )
        self.assertIn("partitionBy(F.col('region')).orderBy(", result.code)
        self.assertIn(
            "partitionBy(F.col('region'), F.col('department')).orderBy(",
            result.code,
        )
        self.assertTrue(
            any(
                "best-effort observation order" in warning
                for warning in result.warnings
            )
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_sort_and_nodupkey(self):
        result = self.converter.convert(
            """
            proc sort data=work.people out=work.sorted nodupkey;
              by descending score name;
            run;
            """
        )
        self.assertIn("from pyspark.sql import Window as __sas2pyspark_window", result.code)
        self.assertIn("partitionBy(F.col('score'), F.col('name'))", result.code)
        self.assertIn("F.col('score').desc()", result.code)
        self.assertIn("orderBy(F.col('score').desc(), F.col('name').asc())", result.code)
        self.assertTrue(
            any("tie behavior may differ" in warning for warning in result.warnings)
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_sort_dupout_nodup_and_where(self):
        result = self.converter.convert(
            """
            proc sort data=work.people out=work.unique dupout=work.dups nodupkey;
              where score ge 10;
              by name;
            run;
            proc sort data=work.people out=work.distinct noduprecs;
              by name;
            run;
            """
        )
        self.assertIn("filter(F.expr('(score >= 10)'))", result.code)
        self.assertIn("df_dups = df_unique__ranked.filter(", result.code)
        self.assertIn("dropDuplicates()", result.code)
        self.assertTrue(
            any("removes only adjacent" in warning for warning in result.warnings)
        )
        compile(result.code, "<generated-pyspark>", "exec")
        for source, message in (
            ("proc sort data=a dupout=b; by x; run;", "requires NODUPKEY or NODUP"),
            ("proc sort data=a nodup nodupkey dupout=b; by x; run;", "both NODUPKEY and NODUP"),
        ):
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(message, str(context.exception))

    def test_proc_append_and_datasets(self):
        result = self.converter.convert(
            """
            proc append base=work.big data=work.small force;
            run;
            proc datasets lib=work nolist;
              change tmp=final;
              delete scratch;
              append base=big data=small;
            quit;
            """
        )
        self.assertIn("df_big = df_big.unionByName(df_small, allowMissingColumns=True)", result.code)
        self.assertIn("df_final = df_tmp", result.code)
        self.assertIn("dropTempView('scratch')", result.code)
        self.assertIn("allowMissingColumns=False", result.code)
        self.assertTrue(any("APPEND FORCE" in w for w in result.warnings))
        compile(result.code, "<generated-pyspark>", "exec")
        with self.assertRaises(ConversionError):
            self.converter.convert("proc append data=a; run;")
        with self.assertRaises(ConversionError):
            self.converter.convert("proc datasets lib=work; modify a; run;")

    def test_libname_is_accepted_and_include_is_inlined(self):
        import tempfile
        from pathlib import Path

        result = self.converter.convert(
            "libname mylib 'C:/data';\nlibname old clear;\n"
            "data mylib.b; set mylib.a; run;"
        )
        self.assertIn("# LIBNAME mylib", result.code)
        self.assertIn("df_b = df_a", result.code)
        self.assertTrue(any("LIBNAME mylib" in w for w in result.warnings))
        with self.assertRaises(ConversionError):
            self.converter.convert("libname bad;")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "sub").mkdir()
            (root / "sub" / "inner.sas").write_text("data c; set a; run;\n", encoding="utf-8")
            (root / "part.sas").write_text("%include 'sub/inner.sas';\n", encoding="utf-8")
            (root / "loop.sas").write_text("%include 'loop.sas';\n", encoding="utf-8")
            result = self.converter.convert("%include 'part.sas';\n", include_dir=root)
            self.assertIn("df_c = df_a", result.code)
            with self.assertRaises(ConversionError) as ctx:
                self.converter.convert("%include '../x.sas';", include_dir=root / "sub", include_root=root / "sub")
            self.assertIn("escapes", str(ctx.exception))
            with self.assertRaises(ConversionError) as ctx:
                self.converter.convert("%include 'loop.sas';", include_dir=root)
            self.assertIn("Circular", str(ctx.exception))
            with self.assertRaises(ConversionError):
                self.converter.convert("%include 'missing.sas';", include_dir=root)
        with self.assertRaises(ConversionError):
            self.converter.convert("%include 'x.sas';")
        with self.assertRaises(ConversionError):
            self.converter.convert("%let f=x; %include &f;", include_dir=".")

    def test_proc_format_value_and_put(self):
        result = self.converter.convert(
            """
            proc format;
              value agegrp low-<18='Minor' 18-64='Adult' 65-high='Senior' .='Unknown';
              value $region 'N','S'='Ns' other='Other';
            run;
            data work.out;
              set work.people;
              grp = put(age, agegrp.);
              reg = put(code, $region.);
              format age agegrp.;
            run;
            """
        )
        self.assertIn("'Minor'", result.code)
        self.assertIn("(age) >= 65", result.code)
        self.assertIn("(code) = 'N' OR (code) = 'S'", result.code)
        self.assertIn("ELSE 'Other'", result.code)
        compile(result.code, "<generated-pyspark>", "exec")
        with self.assertRaises(ConversionError):
            self.converter.convert("proc format; picture p low-high='9'; run;")
        with self.assertRaises(ConversionError):
            self.converter.convert("proc format; value f 'a'=1; run;")
        with self.assertRaises(ConversionError):
            self.converter.convert("proc format library=x; run;")

    def test_multi_output_data_step_is_split_per_target(self):
        result = self.converter.convert(
            """
            data work.high work.mid work.low;
              set work.scores;
              band = score / 10;
              if score >= 80 then output high;
              else if score >= 50 then output mid;
              else output low;
            run;
            data work.copy_a work.copy_b;
              set work.scores;
              output;
            run;
            data work.only;
              set work.scores;
              if score > 1 then output;
            run;
            """
        )
        self.assertIn("df_high = df_scores", result.code)
        self.assertIn("df_low = df_scores", result.code)
        self.assertIn("coalesce(NOT (score >= 80), true)", result.code)
        self.assertIn("df_copy_b = df_scores", result.code)
        self.assertIn("createOrReplaceTempView('only')", result.code)
        compile(result.code, "<generated-pyspark>", "exec")
        for source, message in (
            ("data a b; set s; if x then output a; y = 1; run;", "last statements"),
            ("data a b; set s; if x then output a; run;", "never written"),
            ("data a b; set s; if x then output c; run;", "not listed"),
            (
                "data a b; set s; if x then output a b; if y then output a; run;",
                "more than one OUTPUT",
            ),
            ("data a; set s; else output a; run;", "no preceding IF"),
        ):
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(message, str(context.exception))

    def test_means_defaults_median_range_and_univariate(self):
        result = self.converter.convert(
            """
            proc means data=work.t;
              var x;
            run;
            proc summary data=work.t median range;
              class g;
              var x;
              output out=work.s median=med range=rng;
            run;
            proc univariate data=work.t;
              var x y;
            run;
            """
        )
        self.assertIn("defaulted to N, MEAN", result.code)
        self.assertIn("F.median(F.col('x')).alias('med')", result.code)
        self.assertIn("(F.max(F.col('x')) - F.min(F.col('x'))).alias('rng')", result.code)
        self.assertIn("F.median(F.col('y')).alias('median_y')", result.code)
        self.assertTrue(any("UNIVARIATE" in w for w in result.warnings))
        compile(result.code, "<generated-pyspark>", "exec")
        with self.assertRaises(ConversionError):
            self.converter.convert("proc univariate data=t normal; var x; run;")

    def test_proc_freq_generates_one_way_and_two_way_tables(self):
        result = self.converter.convert(
            """
            proc freq data=work.people;
              tables team / out=work.team_frequency;
            run;
            proc freq data=work.people;
              tables team * status;
            run;
            """
        )
        self.assertIn("df_team_frequency = __sas2pyspark_freq_counts", result.code)
        self.assertIn("createOrReplaceTempView('team_frequency')", result.code)
        self.assertIn("df_freq_2 = __sas2pyspark_freq_counts", result.code)
        self.assertIn("'ROW_PERCENT'", result.code)
        self.assertIn("'COL_PERCENT'", result.code)
        self.assertIn("F.col('team').isNotNull()", result.code)
        self.assertIn("F.col('status').isNotNull()", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_freq_missing_option_keeps_null_table_values(self):
        result = self.converter.convert(
            "proc freq data=work.people missing; tables team; run;"
        )
        self.assertIn(
            "__sas2pyspark_freq_counts = df_people.groupBy(F.col('team'))",
            result.code,
        )
        self.assertNotIn(".isNotNull()", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_freq_rejects_unsupported_forms(self):
        unsupported_sources = (
            ("proc freq; tables team; run;", "requires DATA="),
            ("proc freq data=people; run;", "requires one TABLES statement"),
            (
                "proc freq data=people; tables team status; run;",
                "one-way or two-way tables",
            ),
            (
                "proc freq data=people; tables team*status / missing; run;",
                "supports only the OUT= option",
            ),
            (
                "proc freq data=people; tables team; tables status; run;",
                "exactly one TABLES specification",
            ),
            (
                "proc freq data=people; tables team; nlevels; run;",
                "Unsupported PROC FREQ statement",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_proc_means_and_summary_generate_aggregations(self):
        result = self.converter.convert(
            """
            proc means data=work.sales n sum mean std min max;
              class region;
              var amount units;
              output out=work.summary;
            run;
            proc summary data=work.sales sum mean;
              var amount;
            run;
            """
        )
        self.assertIn("df_sales.filter(F.col('region').isNotNull())", result.code)
        self.assertIn(
            "F.count(F.col('amount')).alias('n_amount')", result.code
        )
        self.assertIn(
            "F.stddev_samp(F.col('units')).alias('std_units')", result.code
        )
        self.assertIn("createOrReplaceTempView('summary')", result.code)
        self.assertIn("df_means_2 = df_sales.agg(", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_means_supports_additional_statistics_and_output_names(self):
        result = self.converter.convert(
            """
            proc means data=work.sales n nmiss sum mean std stderr var cv min max;
              var amount units;
              output out=work.summary
                n=valid_amount valid_units
                nmiss=missing_amount missing_units
                mean=avg_amount avg_units;
            run;
            """
        )
        self.assertIn(
            "F.count(F.when(F.col('amount').isNull(), F.lit(1)))"
            ".alias('missing_amount')",
            result.code,
        )
        self.assertIn(
            "(F.stddev_samp(F.col('amount')) / "
            "F.sqrt(F.count(F.col('amount')))).alias('stderr_amount')",
            result.code,
        )
        self.assertIn("F.var_samp(F.col('amount')).alias('var_amount')", result.code)
        self.assertIn("F.when(F.avg(F.col('amount')) != F.lit(0)", result.code)
        self.assertIn("F.count(F.col('amount')).alias('valid_amount')", result.code)
        self.assertIn("F.avg(F.col('amount')).alias('avg_amount')", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_means_missing_option_keeps_null_class_groups(self):
        result = self.converter.convert(
            "proc means data=work.sales n missing; "
            "class region; var amount; run;"
        )
        self.assertIn("__sas2pyspark_means_input = df_sales", result.code)
        self.assertIn(
            "groupBy(F.col('region')).agg(F.count(F.col('amount'))",
            result.code,
        )
        self.assertNotIn(".isNotNull()", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_sql_calculated_and_into_macro(self):
        code = SasToPySparkConverter().convert(
            "proc sql; create table o as select x*2 as dbl, "
            "calculated dbl + 1 as d2 from a where calculated dbl > 3; quit;"
        ).code
        self.assertIn("(x*2) + 1 as d2", code)
        self.assertIn("(x*2) > 3", code)
        with self.assertRaises(ConversionError):
            SasToPySparkConverter().convert(
                "proc sql; create table o as select calculated nope from a; quit;"
            )
        with self.assertRaises(ConversionError) as caught:
            SasToPySparkConverter().convert(
                "proc sql noprint; select max(x) into :mx from a; quit;"
            )
        self.assertIn("INTO :macro", caught.exception.message)

    def test_macro_text_functions(self):
        source = """
%let name = sales_2024_q1;
%put name is &name;
%let part = %scan(&name, 2, _);
%let last = %scan(&name, -1, _);
%let head = %substr(&name, 1, 5);
%let tail = %substr(&name, 7);
%let pos = %index(&name, 2024);
%let len = %length(&name);
data out;
  set &head;
  a = &part;
  b = "&last";
  c = "&tail";
  d = &pos + &len;
  e = "%quote(x y)";
run;
"""
        code = SasToPySparkConverter().convert(source).code
        self.assertIn("df_sales", code)
        self.assertIn("F.expr('2024')", code)
        self.assertIn('"q1"', code)
        self.assertIn('"2024_q1"', code)
        self.assertIn("7 + 13", code)
        for bad in ("%substr(abc)", "%substr(abc,x)", "%scan(abc)", "%substr(abc,0)"):
            with self.assertRaises(ConversionError):
                SasToPySparkConverter().convert(f"%let v = {bad};")

    def test_macro_sysevalf_symexist_return_goto_abort(self):
        source = """
%let ratio = %sysevalf(10 / 4);
%let up = %sysevalf(10 / 4, ceil);
%let down = %sysevalf(10 / 4, floor);
%let flag = %sysevalf(0.5, boolean);
%let known = %symexist(ratio);
%let unknown = %symexist(nothere);
%macro pick(mode=a);
  %if &mode = a %then %return;
  %if &mode = skip %then %goto done;
  data skipped; set src; run;
  %done:
  data after; set src; run;
%mend;
%macro early();
  data first; set src; run;
  %return;
  data never; set src; run;
%mend;
data out;
  set src;
  a = &ratio; b = &up; c = &down; d = &flag; e = &known; f = &unknown;
run;
%early();
%pick(mode=skip);
%pick();
"""
        code = SasToPySparkConverter().convert(source).code
        for fragment in ("F.expr('2.5')", "F.expr('3')", "F.expr('2')", "F.expr('1')", "F.expr('0')"):
            self.assertIn(fragment, code)
        self.assertIn("df_first = df_src", code)
        self.assertNotIn("never", code)
        self.assertIn("df_after = df_src", code)
        self.assertNotIn("skipped", code)
        for bad in (
            "%macro m(); %goto missing; data a; set b; run; %mend; %m();",
            "%macro m(); %done: data a; set b; run; %goto done; %mend; %m();",
            "%return;",
            "%abort;",
            "%let v = %sysevalf(1, weird);",
            "%let v = %symexist(1x);",
        ):
            with self.assertRaises(ConversionError, msg=bad):
                SasToPySparkConverter().convert(bad)

    def test_macro_upcase_str_global_local(self):
        source = """
%macro setup();
  %global region;
  %local tmp;
  %let tmp = x;
  %let region = %upcase(west);
  %let hidden = %str(a b);
%mend;
%setup();
data out;
  set sales;
  where region = "&region";
  label2 = "%lowcase(ABC)";
run;
"""
        result = SasToPySparkConverter().convert(source)
        self.assertIn('region = "WEST"', result.code)
        self.assertIn('"abc"', result.code)
        with self.assertRaises(ConversionError):
            SasToPySparkConverter().convert(
                source.replace('"&region"', '"&hidden"')
            )
        with self.assertRaises(ConversionError):
            SasToPySparkConverter().convert(
                "data a; set b; call symput('x', y); run;"
            )

    def test_proc_means_and_summary_reject_unsupported_forms(self):
        unsupported_sources = (
            ("proc means; var amount; run;", "requires DATA="),
            ("proc means data=sales n;", "is missing RUN"),
            ("proc means data=sales skew; var amount; run;", "Unsupported"),
            (
                "proc means data=sales n; class region; run;",
                "requires one VAR statement",
            ),
            (
                "proc means data=sales n; var amount amount; run;",
                "column names must be distinct",
            ),
            (
                "proc means data=sales n; var amount; output out=stats mean=; run;",
                "supports OUT= and statistic=output-name lists",
            ),
            (
                "proc summary data=sales n n; var amount; run;",
                "statistic 'n' is repeated",
            ),
            (
                "proc means data=sales n; var amount units; "
                "output out=stats n=count; run;",
                "requires exactly 2 output names",
            ),
            (
                "proc means data=sales n; var amount; "
                "output out=stats sum=total; run;",
                "must be requested on the PROC statement",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_proc_transpose_generates_ordered_wide_output(self):
        result = self.converter.convert(
            """
            proc transpose data=work.values out=work.transposed order=sequence;
              by region;
              var amount quantity;
            run;
            """
        )
        self.assertIn("F.row_number().over(", result.code)
        self.assertIn("partitionBy(F.col('region')).orderBy(F.col('sequence').asc())", result.code)
        self.assertIn("F.lit('amount').alias('_NAME_')", result.code)
        self.assertIn("F.lit('quantity').alias('_NAME_')", result.code)
        self.assertIn("alias(f'COL{index}')", result.code)
        self.assertIn("createOrReplaceTempView('transposed')", result.code)
        self.assertIn("values must be unique within each BY group", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_transpose_rejects_unsupported_or_ambiguous_forms(self):
        unsupported_sources = (
            (
                "proc transpose data=values; var amount; run;",
                "requires ORDER=",
            ),
            (
                "proc transpose data=values order=id; run;",
                "requires one VAR statement",
            ),
            (
                "proc transpose data=values order=id; var amount; id category; run;",
                "Unsupported PROC TRANSPOSE statement",
            ),
            (
                "proc transpose data=values order=id; by id; var id amount; run;",
                "columns must be distinct",
            ),
            (
                "proc transpose data=values order=amount; var amount; run;",
                "columns must be distinct",
            ),
            (
                "proc transpose data=values order=id order=seq; var amount; run;",
                "accepts ORDER= only once",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_proc_import_csv_generates_configured_spark_reader(self):
        result = self.converter.convert(
            """
            proc import datafile="C:\\data folder\\people.csv"
              out=work.people dbms=csv replace;
              getnames=yes;
              delimiter='|';
            run;
            """
        )
        self.assertIn(".option('header', True)", result.code)
        self.assertIn(".option('inferSchema', True)", result.code)
        self.assertIn(".option('sep', '|')", result.code)
        self.assertIn(".option('mode', 'FAILFAST')", result.code)
        self.assertIn(".csv('C:\\\\data folder\\\\people.csv')", result.code)
        self.assertIn("createOrReplaceTempView('people')", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_import_csv_rejects_unsupported_options(self):
        unsupported_sources = (
            (
                "proc import out=people dbms=csv; run;",
                "requires DATAFILE=",
            ),
            (
                "proc import datafile='people.csv' dbms=csv; run;",
                "requires OUT=",
            ),
            (
                "proc import datafile='people.xlsx' out=people dbms=xlsx; run;",
                "supports only DBMS=CSV",
            ),
            (
                "proc import datafile='people.csv' out=people dbms=csv; guessingrows=max; run;",
                "Unsupported PROC IMPORT option: GUESSINGROWS",
            ),
            (
                "proc import datafile='people.csv' out=people dbms=csv; getnames=maybe; run;",
                "GETNAMES= must be YES or NO",
            ),
            (
                "proc import datafile='people.csv' out=people dbms=csv; delimiter='::'; run;",
                "DELIMITER= must be exactly one character",
            ),
            (
                "proc import datafile='people.csv' datafile='other.csv' out=people dbms=csv; run;",
                "specified more than once",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_proc_export_csv_generates_configured_spark_writer(self):
        result = self.converter.convert(
            """
            proc export data=work.people
              outfile="C:\\data folder\\people.csv"
              dbms=csv replace;
              putnames=no;
              delimiter='|';
            run;
            """
        )
        self.assertIn("df_people.write.mode('overwrite')", result.code)
        self.assertIn(".option('header', False)", result.code)
        self.assertIn(".option('sep', '|')", result.code)
        self.assertIn(".csv('C:\\\\data folder\\\\people.csv')", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_proc_export_csv_rejects_unsupported_options(self):
        unsupported_sources = (
            (
                "proc export outfile='people.csv' dbms=csv; run;",
                "requires DATA=",
            ),
            (
                "proc export data=people dbms=csv; run;",
                "requires OUTFILE=",
            ),
            (
                "proc export data=people outfile='people.xlsx' dbms=xlsx; run;",
                "supports only DBMS=CSV",
            ),
            (
                "proc export data=people outfile='people.csv' dbms=csv; putnames=maybe; run;",
                "PUTNAMES= must be YES or NO",
            ),
            (
                "proc export data=people outfile='people.csv' dbms=csv; delimiter='::'; run;",
                "DELIMITER= must be exactly one character",
            ),
            (
                "proc export data=people outfile='people.csv' dbms=csv; label; run;",
                "Unsupported PROC EXPORT option: LABEL",
            ),
            (
                "proc export data=people outfile='people.csv' outfile='other.csv' dbms=csv; run;",
                "specified more than once",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_proc_sql_create_table(self):
        result = self.converter.convert(
            """
            proc sql;
              create table work.summary as
              select p.team, count(*) as n from work.people as p
              where p.score ge 10 group by p.team;
            quit;
            """
        )
        self.assertIn("df_summary = spark.sql(", result.code)
        self.assertIn("from people", result.code)
        self.assertIn("p.score >= 10", result.code)
        self.assertIn("p.team", result.code)
        self.assertIn("createOrReplaceTempView('summary')", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_data_step_preserves_assignment_and_if_order(self):
        result = self.converter.convert(
            """
            data output;
              set input;
              adjusted = value + 1;
              if adjusted gt 5;
            run;
            """
        )
        assignment = result.code.index("withColumn('adjusted'")
        row_filter = result.code.index(".filter(F.expr('adjusted > 5'))")
        self.assertLess(assignment, row_filter)

    def test_conditional_assignment_generates_when_otherwise(self):
        result = self.converter.convert(
            """
            data graded;
              set employees;
              if salary ge 100000 then grade = 'A';
              else grade = 'B';
            run;
            """
        )
        self.assertIn(
            "F.when(F.expr('salary >= 100000'), F.expr(\"'A'\")).otherwise(F.expr(\"'B'\"))",
            result.code,
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_select_when_generates_ordered_when_expressions(self):
        result = self.converter.convert(
            """
            data graded;
              set employees;
              select;
                when (score ge 90) grade = 'A';
                when (score ge 75) grade = 'B';
                otherwise grade = 'C';
              end;
            run;
            """
        )
        self.assertIn(
            "F.when(F.expr('score >= 90'), F.expr(\"'A'\")).otherwise("
            "F.when(F.expr('score >= 75'), F.expr(\"'B'\")).otherwise(F.expr(\"'C'\")))",
            result.code,
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_select_when_rejects_unmodeled_semantics(self):
        unsupported_sources = (
            (
                "data out; set source; select; when (score gt 0) grade='A'; end; run;",
                "requires OTHERWISE",
            ),
            (
                "data out; set source; select; when (score gt 0) output; otherwise output; end; run;",
                "simple assignments",
            ),
            (
                "data out; set source; select; when (score gt 0) grade='A'; otherwise level='B'; end; run;",
                "same column",
            ),
            (
                "data out; set source; select; when (score gt 0) grade='A'; otherwise grade='B';",
                "missing END",
            ),
            (
                "data out; set source; select; when (score gt 0) grade='A'; otherwise grade='B'; when (score lt 0) grade='C'; end; run;",
                "WHEN cannot follow OTHERWISE",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_if_without_else_and_else_if_chains(self):
        no_else = self.converter.convert(
            "data out; set source; if score gt 0 then grade = 'A'; run;"
        )
        self.assertIn("'grade' in df_out.columns", no_else.code)
        chain = self.converter.convert(
            "data out; set source; if score gt 90 then g = 'A'; "
            "else if score gt 80 then g = 'B'; else g = 'C'; run;"
        )
        self.assertIn("F.when(F.expr('score > 90')", chain.code)
        self.assertIn("F.when(F.expr('score > 80')", chain.code)
        partial = self.converter.convert(
            "data out; set source; if a gt 1 then g = 1; else if a gt 0 then g = 2; run;"
        )
        for result in (no_else, chain, partial):
            compile(result.code, "<generated-pyspark>", "exec")

    def test_date_functions_and_literals_translate(self):
        result = self.converter.convert(
            """
            data out;
              set source;
              format d date9.;
              when_made = mdy(month, day, year);
              gap = intck('month', '01JAN2024'd, d);
              nxt = intnx('month', d, 1, 'end');
              text = put(d, yymmdd10.);
              back = input(text, yymmdd10.);
            run;
            """
        )
        self.assertIn("DATE'2024-01-01'", result.code)
        self.assertIn("last_day(add_months(", result.code)
        self.assertIn("date_format(CAST(d AS DATE), 'yyyy-MM-dd')", result.code)
        self.assertTrue(any("FORMAT statement" in w for w in result.warnings))
        compile(result.code, "<generated-pyspark>", "exec")

    def test_date_functions_reject_unsupported_forms(self):
        unsupported_sources = (
            ("data o; set s; x = intck(unit, a, b); run;", "must be a quoted literal"),
            ("data o; set s; x = intck('hour', a, b); run;", "interval 'HOUR' is unsupported"),
            ("data o; set s; x = intnx('month', a, 1, 'middle'); run;", "alignment"),
            ("data o; set s; x = put(a, comma12.2); run;", "unsupported"),
            ("data o; set s; x = input(a, anydtdte.); run;", "unsupported"),
            ("data o; set s; x = datdif(a, b, '30/360'); run;", "ACT/ACT"),
            ("data o; set s; x = '31FOO2024'd; run;", "Invalid SAS date literal"),
            ("data o; set s; format d comma12.; run;", "unsupported"),
            ("data o; set s; format d; run;", "FORMAT supports"),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_if_then_do_blocks_and_delete_rewrite_to_conditionals(self):
        result = self.converter.convert(
            """
            data out;
              set source;
              if score ge 90 then do;
                grade = 'A';
                bonus = 10;
              end;
              else if score ge 80 then do;
                grade = 'B';
                bonus = 5;
              end;
              else do;
                grade = 'C';
                bonus = 0;
              end;
              if status = 'X' then delete;
            run;
            """
        )
        self.assertIn("F.when(F.expr('(score >= 90)')", result.code)
        self.assertIn("coalesce", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_if_then_do_rejects_unsafe_forms(self):
        unsupported_sources = (
            (
                "data out; set s; if a gt 1 then do; if b gt 1 then c = 1; end; run;",
                "nested DO, SELECT and IF/ELSE",
            ),
            (
                "data out; set s; if a gt 1 then do; a = 2; b = 3; end; run;",
                "references 'a'",
            ),
            (
                "data out; set s; if a gt 1 then do; c = 2;",
                "missing END",
            ),
            (
                "data out; set s; if a gt 1 then do; output; end; run;",
                "simple assignment",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_conditional_assignment_rejects_unsupported_forms(self):
        unsupported_sources = (
            (
                "data out; set source; if score gt 0 then grade = 'A'; else level = 'B'; run;",
                "must assign the same column",
            ),
            (
                "data out; set source; else grade = 'B'; run;",
                "ELSE must immediately follow",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_output_do_loop_generates_sequence_explode_and_body_assignment(self):
        result = self.converter.convert(
            """
            data expanded;
              set source;
              do index = 1 to 5 by 2;
                scaled = index * 10;
                output;
              end;
              keep id index scaled;
            run;
            """
        )
        self.assertIn(
            "F.explode(F.sequence(F.lit(1), F.lit(5), F.lit(2)))",
            result.code,
        )
        self.assertIn("withColumn('scaled', F.expr('index * 10'))", result.code)
        self.assertIn(".select(*['id', 'index', 'scaled'])", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_output_do_loop_rejects_unmodeled_semantics(self):
        unsupported_sources = (
            (
                "data out; set source; do index = 1 to 5; index = index + 1; output; end; run;",
                "cannot reassign its index variable",
            ),
            (
                "data out; set source; do index = 1 to 5; total = total + index; output; end; run;",
                "cannot depend on their own previous-iteration value",
            ),
            (
                "data out; set source; do index = 1.5 to 5; output; end; run;",
                "integer-literal DO index",
            ),
            (
                "data out; set source; do index = 1 to 5; output; run;",
                "missing END",
            ),
            (
                "data out; set source; do index = 1 to 5; index = index + 1; output; end; run;",
                "cannot reassign its index variable",
            ),
            (
                "data out; set source; do index = 1 to 5; value = index + 1; end; run;",
                "exactly one OUTPUT",
            ),
            (
                "data out; set source; do index = 1 to 5; output; end; value = index; run;",
                "must be the final data-processing statement",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_merge_by_generates_guarded_full_outer_match_merge(self):
        result = self.converter.convert(
            """
            data combined;
              merge work.left_data work.right_data;
              by customer_id;
              keep customer_id left_value right_value shared_value;
            run;
            """
        )
        self.assertIn("__sas2pyspark_merge_inputs = [df_left_data, df_right_data]", result.code)
        self.assertIn("F.row_number().over(", result.code)
        self.assertIn("F.least(", result.code)
        self.assertIn(".eqNullSafe(", result.code)
        self.assertIn("'left'", result.code)
        self.assertIn("otherwise(__sas2pyspark_value)", result.code)
        self.assertIn(".orderBy(F.col('customer_id').asc(), F.col('__sas2pyspark_k').asc())", result.code)
        self.assertIn("createOrReplaceTempView('combined')", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_merge_by_respects_descending_key_order(self):
        result = self.converter.convert(
            """
            data combined;
              merge work.left_data work.right_data;
              by descending period region;
            run;
            """
        )
        self.assertIn(
            ".orderBy(F.col('period').desc(), F.col('region').asc(), F.col('__sas2pyspark_k').asc())",
            result.code,
        )
        compile(result.code, "<generated-pyspark>", "exec")

    def test_merge_in_flags_are_parsed_and_dropped(self):
        result = self.converter.convert(
            """
            data out;
              merge left(in=a) right (in = b);
              by id;
              if a and b=1;
            run;
            """
        )
        self.assertIn("alias('a')", result.code)
        self.assertIn("alias('b')", result.code)
        self.assertIn("a AND b", result.code)
        self.assertIn(".drop(*['a', 'b'])", result.code)
        compile(result.code, "<generated-pyspark>", "exec")
        with self.assertRaises(ConversionError):
            self.converter.convert("data o; merge l(in=a) r(in=a); by id; run;")

    def test_merge_by_rejects_unsupported_forms(self):
        unsupported_sources = (
            (
                "data out; merge only_one; by id; run;",
                "at least two input datasets",
            ),
            (
                "data out; merge left right; run;",
                "MERGE followed by BY",
            ),
            (
                "data out; merge left right; by id id; run;",
                "must be distinct",
            ),
            (
                "data out; merge left right; by id; where active = 1; run;",
                "WHERE in a MERGE DATA step is unsupported",
            ),
            (
                "data out; set source; merge left right; by id; run;",
                "either one SET statement or one supported MERGE",
            ),
            (
                "data out; value = 1; merge left right; by id; run;",
                "MERGE and BY must appear before",
            ),
            (
                "data out; where active = 1; merge left right; by id; run;",
                "MERGE and BY must appear before",
            ),
            (
                "data out; merge left(keep=a) right; by id; run;",
                "Unsupported SAS dataset reference",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_common_sas_functions_translate_and_support_nesting(self):
        result = self.converter.convert(
            """
            data function_output;
              set function_input;
              joined = catx(':', substr(code, 1, 2), upcase(name));
              compact = compress(raw, '- ');
              rounded = round(amount, 0.01);
              whole = round(amount);
            run;
            """
        )
        self.assertIn("concat_ws(coalesce(CAST(':' AS STRING), '')", result.code)
        self.assertIn("substring(code, 1, 2)", result.code)
        self.assertIn("upper(name)", result.code)
        self.assertIn("translate(CAST(raw AS STRING), CAST('- ' AS STRING), '')", result.code)
        self.assertIn("(round((amount) / (0.01), 0) * (0.01))", result.code)
        self.assertIn("round(amount, 0)", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_missing_and_concatenation_functions_translate(self):
        result = self.converter.convert(
            """
            data missing_output;
              set missing_input;
              raw_join = cat(first, middle, last);
              clean_join = cats(first, middle, last);
              first_present = coalescec(first, middle, last);
              numeric_present = coalesce(score, backup_score);
              cleaned = strip(comment);
            run;
            """
        )
        self.assertIn(
            "concat(coalesce(CAST(first AS STRING), ''), coalesce(CAST(middle AS STRING), ''), coalesce(CAST(last AS STRING), ''))",
            result.code,
        )
        self.assertIn("coalesce(trim(CAST(first AS STRING)), '')", result.code)
        self.assertIn(
            "CASE WHEN nullif(trim(CAST(first AS STRING)), '') IS NOT NULL THEN CAST(first AS STRING)",
            result.code,
        )
        self.assertIn("coalesce(score, backup_score)", result.code)
        self.assertIn("coalesce(trim(CAST(comment AS STRING)), '')", result.code)
        compile(result.code, "<generated-pyspark>", "exec")

    def test_missing_and_concatenation_functions_reject_empty_arguments(self):
        unsupported_sources = (
            "data out; set source; value = cat(a,); run;",
            "data out; set source; value = cats(); run;",
            "data out; set source; value = coalesce(a,); run;",
            "data out; set source; value = coalescec(); run;",
            "data out; set source; value = strip(a, b); run;",
        )
        for source in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError):
                    self.converter.convert(source)

    def test_quoted_function_names_are_not_translated(self):
        result = self.converter.convert(
            "data output; set input; label = 'catx(a,b)'; run;"
        )
        self.assertIn("F.expr(\"'catx(a,b)'\")", result.code)
        self.assertNotIn("concat_ws", result.code)

    def test_unsupported_function_arguments_report_conversion_error(self):
        unsupported_sources = (
            (
                "data out; set source; value = catx('-'); run;",
                "CATX requires a delimiter and at least one value",
            ),
            (
                "data out; set source; value = compress(text, ' ', 'ka'); run;",
                "COMPRESS supports a source and optional character set",
            ),
            (
                "data out; set source; value = round(amount, unit); run;",
                "positive numeric literal",
            ),
            (
                "data out; set source; value = substr(text, 0, 2); run;",
                "start position must be a positive integer",
            ),
        )
        for source, expected_message in unsupported_sources:
            with self.subTest(source=source):
                with self.assertRaises(ConversionError) as context:
                    self.converter.convert(source)
                self.assertIn(expected_message, str(context.exception))

    def test_comments_and_quoted_semicolons(self):
        result = self.converter.convert(
            """
            /* block ; comment */
            data out; * statement comment;
              set input;
              label = 'a; eq';
            run;
            """
        )
        self.assertIn("F.expr(\"'a; eq'\")", result.code)

    def test_unknown_statement_fails_explicitly(self):
        with self.assertRaises(ConversionError) as context:
            self.converter.convert(
                "\n/* first line\n   second line */\nproc unsupported data=work.people; run;"
            )
        self.assertIn("SAS line 4:", str(context.exception))
        self.assertIn("> proc unsupported data=work.people", str(context.exception))
        self.assertIn("Unsupported top-level", str(context.exception))

    def test_unsupported_data_step_statement_reports_its_line(self):
        with self.assertRaises(ConversionError) as context:
            self.converter.convert(
                "data output;\n  set input;\n  put x;\nrun;"
            )
        self.assertIn("SAS line 3:", str(context.exception))
        self.assertIn("> put x", str(context.exception))

    def test_missing_block_terminator_fails_explicitly(self):
        with self.assertRaises(ConversionError) as context:
            self.converter.convert("data out; set source;")
        self.assertIn("SAS line 1:", str(context.exception))
        self.assertIn("missing RUN", str(context.exception))

    def test_unterminated_string_reports_opening_line(self):
        with self.assertRaises(ConversionError) as context:
            self.converter.convert("data out;\n  set source;\n  label = 'unfinished;\n")
        self.assertIn("SAS line 3:", str(context.exception))


if __name__ == "__main__":
    unittest.main()
