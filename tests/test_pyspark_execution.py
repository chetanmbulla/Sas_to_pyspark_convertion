import importlib.util
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from decimal import Decimal

from sas2pyspark import SasToPySparkConverter


HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None


def _java_major_version():
    java_home = os.environ.get("JAVA_HOME")
    java = Path(java_home, "bin", "java.exe") if java_home else None
    executable = str(java) if java and java.is_file() else shutil.which("java")
    if executable is None:
        return None
    try:
        result = subprocess.run(
            [executable, "-version"],
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = result.stdout + result.stderr
    match = re.search(r'version "(?:1\.)?(\d+)', output)
    return int(match.group(1)) if match else None


@unittest.skipUnless(HAS_PYSPARK, "install the 'test' extra to run Spark integration tests")
class GeneratedPySparkExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import pyspark

        java_version = _java_major_version()
        spark_major = int(pyspark.__version__.split(".", maxsplit=1)[0])
        required_java = 17 if spark_major >= 4 else 8
        if java_version is None:
            raise unittest.SkipTest("a Java runtime is required for Spark integration tests")
        if java_version < required_java:
            raise unittest.SkipTest(
                f"PySpark {pyspark.__version__} requires Java {required_java}+; "
                f"found Java {java_version}"
            )

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("sas2pyspark-integration-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_data_step_sort_and_sql_run_on_sample_dataframe(self):
        source = """
        data work.adults;
          set work.people;
          where age ge 18;
          full_name = upcase(name);
          keep full_name age department salary;
        run;

        proc sort data=work.adults out=work.adults_sorted;
          by department descending salary;
        run;

        proc sql;
          create table work.department_summary as
          select department, count(*) as employee_count, avg(salary) as average_salary
          from work.adults_sorted
          group by department;
        quit;
        """
        df_people = self.spark.createDataFrame(
            [
                ("alice", 17, "sales", 30.0),
                ("bob", 35, "sales", 80.0),
                ("cara", 29, "sales", 60.0),
                ("dave", 41, "engineering", 90.0),
            ],
            ["name", "age", "department", "salary"],
        )
        namespace = {"spark": self.spark, "df_people": df_people}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        adults = namespace["df_adults"].collect()
        self.assertEqual(
            [(row.full_name, row.age) for row in adults],
            [("BOB", 35), ("CARA", 29), ("DAVE", 41)],
        )
        self.assertEqual(
            [
                (row.department, row.salary)
                for row in namespace["df_adults_sorted"].collect()
            ],
            [("engineering", 90.0), ("sales", 80.0), ("sales", 60.0)],
        )
        summaries = {
            row.department: (row.employee_count, row.average_salary)
            for row in namespace["df_department_summary"].collect()
        }
        self.assertEqual(summaries["sales"], (2, 70.0))
        self.assertEqual(summaries["engineering"], (1, 90.0))

    def test_set_concatenates_datasets_and_fills_missing_columns(self):
        source = """
        data work.all_records;
          set work.first_records work.second_records;
        run;
        """
        df_first_records = self.spark.createDataFrame(
            [(1, "first")], ["id", "first_value"]
        )
        df_second_records = self.spark.createDataFrame(
            [(2, "second")], ["id", "second_value"]
        )
        namespace = {
            "spark": self.spark,
            "df_first_records": df_first_records,
            "df_second_records": df_second_records,
        }

        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        actual = {
            (row.id, row.first_value, row.second_value)
            for row in namespace["df_all_records"].collect()
        }
        self.assertEqual(
            actual,
            {(1, "first", None), (2, None, "second")},
        )

    def test_simple_macro_variables_execute_in_generated_data_step(self):
        source = """
        %let source=work.people;
        %let minimum_age=18;
        %let output_name=adults;
        data &output_name.;
          set &source.;
          where age ge &minimum_age;
        run;
        """
        df_people = self.spark.createDataFrame(
            [("A", 17), ("B", 18), ("C", 25)],
            ["name", "age"],
        )
        namespace = {"spark": self.spark, "df_people": df_people}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        self.assertEqual(
            [(row.name, row.age) for row in namespace["df_adults"].collect()],
            [("B", 18), ("C", 25)],
        )

    def test_if_without_else_and_else_if_chains_execute(self):
        source = """
        data out;
          set people;
          if age ge 60 then band = 'SENIOR';
          else if age ge 40 then band = 'MID';
          if age lt 20 then flag = 1;
          if age ge 60 then label = 'old';
        run;
        """
        df_people = self.spark.createDataFrame(
            [("A", 17, "x"), ("B", 45, "y"), ("C", 70, "z")],
            ["name", "age", "label"],
        )
        namespace = {"spark": self.spark, "df_people": df_people}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        rows = {
            row.name: (row.band, row.flag, row.label)
            for row in namespace["df_out"].collect()
        }
        self.assertEqual(
            rows,
            {"A": (None, 1, "x"), "B": ("MID", None, "y"), "C": ("SENIOR", None, "old")},
        )

    def test_date_functions_formats_and_literals_execute(self):
        source = """
        data out;
          set events;
          format event_date date9.;
          d = input(raw, yymmdd10.);
          year_part = year(d);
          qtr_part = qtr(d);
          wk = weekday(d);
          months_since = intck('month', '15JAN2024'd, d);
          days_since = intck('day', '01JAN2024'd, d);
          years_since = intck('year', '15JAN2024'd, d);
          weeks_since = intck('week', '01JAN2024'd, d);
          qtrs_since = intck('qtr', '01JAN2024'd, d);
          m_begin = intnx('month', d, 1);
          m_end = intnx('month', d, 0, 'end');
          m_same = intnx('month', d, 1, 'same');
          q_begin = intnx('qtr', d, 0);
          y_end = intnx('year', d, 0, 'e');
          w_begin = intnx('week', d, 0);
          built = mdy(2, 30, 2024);
          built_ok = mdy(2, 29, 2024);
          shown = put(d, date9.);
          shown_iso = put(d, yymmdd10.);
          padded = put(num, z5.);
          fixed = put(amt, 8.2);
          parsed = input(txt, 8.);
          diff = datdif('01JAN2024'd, d, 'act/act');
          keep raw year_part qtr_part wk months_since days_since years_since weeks_since qtrs_since m_begin m_end m_same q_begin y_end w_begin built built_ok shown shown_iso padded fixed parsed diff;
        run;
        """
        df_events = self.spark.createDataFrame(
            [("2024-03-31", 42, 1234.567, " 12.5 "), ("2024-11-05", 7, None, "abc")],
            "raw string, num int, amt double, txt string",
        )
        namespace = {"spark": self.spark, "df_events": df_events}
        generated = SasToPySparkConverter().convert(source)
        self.assertTrue(any("FORMAT statement" in w for w in generated.warnings))
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        rows = {row.raw: row.asDict() for row in namespace["df_out"].collect()}
        first, second = rows["2024-03-31"], rows["2024-11-05"]
        self.assertEqual(
            (first["year_part"], first["qtr_part"], first["wk"]), (2024, 1, 1)
        )
        self.assertEqual(
            (first["months_since"], first["days_since"], first["years_since"]),
            (2, 90, 0),
        )
        self.assertEqual((first["weeks_since"], first["qtrs_since"]), (13, 0))
        self.assertEqual(str(first["m_begin"]), "2024-04-01")
        self.assertEqual(str(first["m_end"]), "2024-03-31")
        self.assertEqual(str(first["m_same"]), "2024-04-30")
        self.assertEqual(str(first["q_begin"]), "2024-01-01")
        self.assertEqual(str(first["y_end"]), "2024-12-31")
        self.assertEqual(str(first["w_begin"]), "2024-03-31")
        self.assertIsNone(first["built"])
        self.assertEqual(str(first["built_ok"]), "2024-02-29")
        self.assertEqual(first["shown"], "31MAR2024")
        self.assertEqual(first["shown_iso"], "2024-03-31")
        self.assertEqual(first["padded"], "00042")
        self.assertEqual(first["fixed"], " 1234.57")
        self.assertEqual(first["parsed"], 12.5)
        self.assertEqual(first["diff"], 90)
        self.assertEqual(second["fixed"], "       .")
        self.assertIsNone(second["parsed"])
        self.assertEqual(str(second["w_begin"]), "2024-11-03")

    def test_proc_sort_dupout_nodup_and_where_execute(self):
        source = """
        proc sort data=people out=unique dupout=dups nodupkey;
          where score ge 10;
          by name;
        run;
        proc sort data=people out=distinct nodup;
          by name;
        run;
        """
        df_people = self.spark.createDataFrame(
            [("A", 50), ("A", 60), ("B", 20), ("B", 20), ("C", 5)],
            "name string, score int",
        )
        namespace = {"spark": self.spark, "df_people": df_people}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        self.assertEqual(
            [row.name for row in namespace["df_unique"].collect()], ["A", "B"]
        )
        self.assertEqual(
            sorted((r.name, r.score) for r in namespace["df_dups"].collect()),
            [("A", 50), ("B", 20)]
            if namespace["df_unique"].filter("name = 'A'").first().score == 60
            else [("A", 60), ("B", 20)],
        )
        self.assertEqual(namespace["df_distinct"].count(), 4)

    def test_proc_append_and_datasets_execute(self):
        source = """
        proc append base=big data=small force;
        run;
        proc datasets lib=work nolist;
          change small=tiny;
        quit;
        """
        namespace = {
            "spark": self.spark,
            "df_big": self.spark.createDataFrame([(1, "a")], "id int, x string"),
            "df_small": self.spark.createDataFrame([(2,)], "id int"),
        }
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        self.assertEqual(
            sorted((r.id, r.x) for r in namespace["df_big"].collect()),
            [(1, "a"), (2, None)],
        )
        self.assertEqual(namespace["df_tiny"].count(), 1)
        self.assertIsNone(namespace["df_small"])

    def test_proc_format_executes(self):
        source = """
        proc format;
          value agegrp low-<18='Minor' 18-64='Adult' 65-high='Senior' .='Unknown';
          value $region 'N','S'='Ns' other='Other';
          value score 1=one 2=two;
        run;
        """
        source = source.replace("1=one 2=two", "1='one' 2='two'")
        source += """
        data out;
          set people;
          grp = put(age, agegrp.);
          reg = put(code, $region.);
          sc = put(age, score.);
        run;
        """
        df_people = self.spark.createDataFrame(
            [(10, "N"), (18, "S"), (65, "E"), (None, "W"), (1, "N")],
            "age int, code string",
        )
        namespace = {"spark": self.spark, "df_people": df_people}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        rows = [(r.age, r.grp, r.reg, r.sc) for r in namespace["df_out"].collect()]
        self.assertEqual(
            sorted(rows, key=lambda r: (r[0] is None, r[0])),
            [
                (1, "Minor", "Ns", "one"),
                (10, "Minor", "Ns", "10"),
                (18, "Adult", "Ns", "18"),
                (65, "Senior", "Other", "65"),
                (None, "Unknown", "Other", "."),
            ],
        )

    def test_multi_output_routing_executes(self):
        source = """
        data high mid low;
          set scores;
          if score >= 80 then output high;
          else if score >= 50 then output mid;
          else output low;
        run;
        data both_a both_b;
          set scores;
          output;
        run;
        """
        namespace = {
            "spark": self.spark,
            "df_scores": self.spark.createDataFrame(
                [(1, 90), (2, 50), (3, 10), (4, None)], "id int, score int"
            ),
        }
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        ids = lambda name: sorted(r.id for r in namespace[name].collect())
        self.assertEqual(ids("df_high"), [1])
        self.assertEqual(ids("df_mid"), [2])
        self.assertEqual(ids("df_low"), [3, 4])
        self.assertEqual(ids("df_both_a"), [1, 2, 3, 4])
        self.assertEqual(ids("df_both_b"), [1, 2, 3, 4])

    def test_means_median_range_default_execute(self):
        source = """
        proc means data=t;
          var x;
        run;
        proc summary data=t median range;
          class g;
          var x;
          output out=s median=med range=rng;
        run;
        """
        namespace = {
            "spark": self.spark,
            "df_t": self.spark.createDataFrame(
                [("a", 1), ("a", 3), ("a", 8), ("b", 2), ("b", 4)], "g string, x int"
            ),
        }
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        row = namespace["df_means_1"].first()
        self.assertEqual((row.n_x, row.min_x, row.max_x), (5, 1, 8))
        self.assertAlmostEqual(row.mean_x, 3.6)
        groups = {r.g: (r.med, r.rng) for r in namespace["df_s"].collect()}
        self.assertEqual(groups, {"a": (3.0, 7), "b": (3.0, 2)})

    def test_sql_calculated_joins_and_case_execute(self):
        source = """
        proc sql noprint;
          create table o as
          select a.id, a.x * 2 as dbl, calculated dbl + 1 as d2,
                 case when b.v is null then 'none' else 'yes' end as flag
          from a left join b on a.id = b.id
          where calculated dbl > 2
          order by a.id;
        quit;
        """
        self.spark.createDataFrame([(1, 1), (2, 5), (3, 7)], "id int, x int"
            ).createOrReplaceTempView("a")
        self.spark.createDataFrame([(2, "p")], "id int, v string"
            ).createOrReplaceTempView("b")
        namespace = {"spark": self.spark}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        rows = [(r.id, r.dbl, r.d2, r.flag) for r in namespace["df_o"].collect()]
        self.assertEqual(rows, [(2, 10, 11, "yes"), (3, 14, 15, "none")])

    def test_runtime_macro_variables_into_and_symput_execute(self):
        source = """
        proc sql noprint;
          select max(x), min(label) into :mx, :lab trimmed from a;
          select max(x) into :nothing from a where x > 100;
        quit;
        data hi;
          set a;
          where x >= &mx;
          tag = &lab;
          missing_value = &nothing;
        run;
        data _null_;
          set a;
          if x < 5 then call symputx('last_small', x * 10);
        run;
        data small;
          set a;
          scaled = x + &last_small;
        run;
        """
        frame = self.spark.createDataFrame(
            [(1, "b"), (7, "a"), (3, "c")], "x int, label string"
        )
        frame.createOrReplaceTempView("a")
        namespace = {"spark": self.spark, "df_a": frame}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        hi = namespace["df_hi"].collect()
        self.assertEqual([(r.x, r.tag, r.missing_value) for r in hi], [(7, "a", None)])
        small = sorted((r.x, r.scaled) for r in namespace["df_small"].collect())
        self.assertEqual(small, [(1, 31), (3, 33), (7, 37)])

    def test_if_then_do_blocks_and_delete_execute(self):
        source = """
        data out;
          set people;
          if age ge 60 then do;
            band = 'SENIOR';
            bonus = 10;
          end;
          else if age ge 40 then do;
            band = 'MID';
            bonus = 5;
          end;
          else do;
            band = 'JUNIOR';
            bonus = 0;
          end;
          if name = 'D' then delete;
        run;
        """
        df_people = self.spark.createDataFrame(
            [("A", 17), ("B", 45), ("C", 70), ("D", 50), ("E", None)],
            "name string, age int",
        )
        namespace = {"spark": self.spark, "df_people": df_people}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        rows = {
            row.name: (row.band, row.bonus) for row in namespace["df_out"].collect()
        }
        self.assertEqual(
            rows,
            {
                "A": ("JUNIOR", 0),
                "B": ("MID", 5),
                "C": ("SENIOR", 10),
                "E": ("JUNIOR", 0),
            },
        )

    def test_proc_import_csv_executes_with_header_and_delimiter(self):
        with tempfile.TemporaryDirectory(prefix="sas import ") as directory:
            csv_path = Path(directory, "people data.csv")
            csv_path.write_text(
                "name|age\nAlice|31\nBob|42\n",
                encoding="utf-8",
            )
            sas_path = str(csv_path).replace('"', '""')
            source = (
                f'proc import datafile="{sas_path}" out=work.people dbms=csv;\n'
                "  getnames=yes;\n"
                "  delimiter='|';\n"
                "run;"
            )
            generated = SasToPySparkConverter().convert(source)
            namespace = {"spark": self.spark}
            exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

            rows = namespace["df_people"].orderBy("age").collect()
            self.assertEqual(
                [(row.name, row.age) for row in rows],
                [("Alice", 31), ("Bob", 42)],
            )
            self.assertEqual(
                self.spark.table("people").count(),
                2,
            )

    def test_proc_export_csv_writes_header_and_delimited_rows(self):
        with tempfile.TemporaryDirectory(prefix="sas export ") as directory:
            output_path = Path(directory, "people output")
            source = (
                f"proc export data=work.people outfile='{output_path}' "
                "dbms=csv replace;\n"
                "  putnames=yes;\n"
                "  delimiter='|';\n"
                "run;"
            )
            df_people = self.spark.createDataFrame(
                [("Alice", 31), ("Bob", 42)],
                ["name", "age"],
            )
            namespace = {"spark": self.spark, "df_people": df_people}
            generated = SasToPySparkConverter().convert(source)
            exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

            output_files = list(output_path.glob("part-*.csv"))
            self.assertEqual(len(output_files), 1)
            lines = output_files[0].read_text(encoding="utf-8").splitlines()
            self.assertEqual(lines[0], "name|age")
            self.assertEqual(set(lines[1:]), {"Alice|31", "Bob|42"})

    def test_retain_executes_supported_running_total(self):
        source = """
        data work.running;
          retain total 10;
          set work.amounts;
          total = total + amount;
        run;
        """
        df_amounts = self.spark.createDataFrame(
            [(1, 1), (2, 2), (3, 3)],
            ["row_id", "amount"],
        )
        namespace = {"spark": self.spark, "df_amounts": df_amounts}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        totals = {
            row.row_id: row.total
            for row in namespace["df_running"].collect()
        }
        self.assertEqual(totals, {1: 11, 2: 13, 3: 16})

    def test_retain_executes_by_group_reset_running_total(self):
        source = """
        data work.running;
          retain total 10;
          set work.amounts;
          by region department;
          if first.department then total = 0;
          total = total + amount;
        run;
        """
        df_amounts = self.spark.createDataFrame(
            [
                (1, "A", "x", 2),
                (2, "A", "x", 3),
                (3, "A", "y", 4),
                (4, "B", "z", 5),
                (5, "B", "z", 6),
            ],
            ["row_id", "region", "department", "amount"],
        )
        namespace = {"spark": self.spark, "df_amounts": df_amounts}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        totals = {
            row.row_id: row.total
            for row in namespace["df_running"].orderBy("row_id").collect()
        }
        self.assertEqual(totals, {1: 2, 2: 5, 3: 4, 4: 5, 5: 11})

    def test_retain_multiple_variables_execute(self):
        source = """
        data work.multi;
          retain total count 0 bonus 100;
          set work.amounts;
          total = total + amount;
          count = count + 1;
          bonus = bonus + 5;
          avg = total / count;
        run;
        data work.grouped;
          retain total 0 count 0;
          set work.amounts;
          by region;
          if first.region then total = 0;
          if first.region then count = 0;
          total = total + amount;
          count = count + 1;
        run;
        """
        df_amounts = self.spark.createDataFrame(
            [(1, "A", 2), (2, "A", 4), (3, "B", 6)],
            ["row_id", "region", "amount"],
        )
        namespace = {"spark": self.spark, "df_amounts": df_amounts}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        multi = {
            r.row_id: (r.total, r["count"], r.bonus, r.avg)
            for r in namespace["df_multi"].collect()
        }
        self.assertEqual(
            multi, {1: (2, 1, 105, 2.0), 2: (6, 2, 110, 3.0), 3: (12, 3, 115, 4.0)}
        )
        grouped = {
            r.row_id: (r.total, r["count"]) for r in namespace["df_grouped"].collect()
        }
        self.assertEqual(grouped, {1: (2, 1), 2: (6, 2), 3: (6, 1)})

    def test_retain_rejects_missing_accumulator_input(self):
        source = """
        data work.running;
          retain total 0;
          set work.amounts;
          total = total + amount;
        run;
        """
        df_amounts = self.spark.createDataFrame(
            [(1, 2), (2, None)],
            ["row_id", "amount"],
        )
        namespace = {"spark": self.spark, "df_amounts": df_amounts}
        generated = SasToPySparkConverter().convert(source)
        with self.assertRaisesRegex(ValueError, "cannot contain missing values"):
            exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

    def test_sas_arrays_execute_using_explicit_element_columns(self):
        source = """
        data work.calculated;
          set work.scores;
          array marks[3] mark1-mark3;
          total = marks[1] + marks[3];
          marks[2] = marks[1] + 1;
        run;
        """
        df_scores = self.spark.createDataFrame(
            [(2, 4, 8), (3, 5, 9)],
            ["mark1", "mark2", "mark3"],
        )
        namespace = {"spark": self.spark, "df_scores": df_scores}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        rows = namespace["df_calculated"].orderBy("mark1").collect()
        self.assertEqual(
            [(row.mark1, row.mark2, row.mark3, row.total) for row in rows],
            [(2, 3, 8, 10), (3, 4, 9, 12)],
        )

    def test_by_first_and_last_flags_mark_one_row_per_group(self):
        source = """
        data work.flagged;
          set work.source;
          by region department;
          first_region = first.region;
          first_department = first.department;
          last_region = last.region;
          last_department = last.department;
        run;
        """
        df_source = self.spark.createDataFrame(
            [
                ("north", "a", 1),
                ("north", "a", 2),
                ("north", "b", 3),
                ("south", "a", 4),
                ("south", "a", 5),
            ],
            ["region", "department", "row_id"],
        )
        namespace = {"spark": self.spark, "df_source": df_source}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        flagged = namespace["df_flagged"].collect()
        by_row = {row.row_id: row for row in flagged}
        self.assertEqual(sum(row.first_region for row in flagged), 2)
        self.assertEqual(sum(row.last_region for row in flagged), 2)
        self.assertEqual(sum(row.first_department for row in flagged), 3)
        self.assertEqual(sum(row.last_department for row in flagged), 3)
        for region in ("north", "south"):
            region_rows = [row for row in flagged if row.region == region]
            self.assertEqual(sum(row.first_region for row in region_rows), 1)
            self.assertEqual(sum(row.last_region for row in region_rows), 1)
        for region, department in (
            ("north", "a"),
            ("north", "b"),
            ("south", "a"),
        ):
            group_rows = [
                row
                for row in flagged
                if row.region == region and row.department == department
            ]
            self.assertEqual(sum(row.first_department for row in group_rows), 1)
            self.assertEqual(sum(row.last_department for row in group_rows), 1)
        self.assertEqual(set(by_row), {1, 2, 3, 4, 5})

    def test_proc_freq_executes_one_way_and_two_way_tables(self):
        source = """
        proc freq data=work.people;
          tables team / out=work.team_frequency;
        run;
        proc freq data=work.people;
          tables team * status / out=work.team_status_frequency;
        run;
        proc freq data=work.people missing;
          tables team / out=work.team_frequency_missing;
        run;
        """
        df_people = self.spark.createDataFrame(
            [
                ("A", "yes"),
                ("A", "yes"),
                ("A", "no"),
                ("B", "yes"),
                ("B", "no"),
                (None, "yes"),
                ("B", None),
            ],
            ["team", "status"],
        )
        namespace = {"spark": self.spark, "df_people": df_people}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        one_way = {
            row.team: (row.COUNT, row.PERCENT)
            for row in namespace["df_team_frequency"].collect()
        }
        self.assertEqual(one_way["A"][0], 3)
        self.assertAlmostEqual(one_way["A"][1], 50.0)
        self.assertEqual(one_way["B"][0], 3)
        self.assertAlmostEqual(one_way["B"][1], 50.0)

        two_way = {
            (row.team, row.status): (
                row.COUNT,
                row.PERCENT,
                row.ROW_PERCENT,
                row.COL_PERCENT,
            )
            for row in namespace["df_team_status_frequency"].collect()
        }
        self.assertEqual(two_way[("A", "yes")][0], 2)
        self.assertAlmostEqual(two_way[("A", "yes")][1], 40.0)
        self.assertAlmostEqual(two_way[("A", "yes")][2], 200 / 3)
        self.assertAlmostEqual(two_way[("A", "yes")][3], 200 / 3)
        self.assertEqual(two_way[("B", "no")][0], 1)
        self.assertAlmostEqual(two_way[("B", "no")][2], 50.0)
        self.assertAlmostEqual(two_way[("B", "no")][3], 50.0)
        missing_rows = {
            row.team: row.COUNT
            for row in namespace["df_team_frequency_missing"].collect()
        }
        self.assertEqual(missing_rows[None], 1)
        self.assertEqual(sum(missing_rows.values()), 7)

    def test_proc_means_and_summary_execute_with_and_without_class(self):
        source = """
        proc means data=work.sales n sum mean std min max;
          class region;
          var amount units;
          output out=work.region_summary;
        run;
        proc summary data=work.sales sum mean;
          var amount;
        run;
        proc means data=work.sales n missing;
          class region;
          var amount;
          output out=work.missing_class_summary;
        run;
        """
        df_sales = self.spark.createDataFrame(
            [
                ("A", 10.0, 2.0),
                ("A", 20.0, None),
                ("B", 5.0, 4.0),
                (None, 99.0, 9.0),
                ("B", None, 6.0),
            ],
            ["region", "amount", "units"],
        )
        namespace = {"spark": self.spark, "df_sales": df_sales}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        grouped = {
            row.region: row
            for row in namespace["df_region_summary"].collect()
        }
        self.assertEqual(set(grouped), {"A", "B"})
        self.assertEqual(grouped["A"].n_amount, 2)
        self.assertEqual(grouped["A"].sum_amount, 30.0)
        self.assertEqual(grouped["A"].mean_amount, 15.0)
        self.assertAlmostEqual(grouped["A"].std_amount, 2 ** 0.5 * 5)
        self.assertEqual(grouped["A"].min_amount, 10.0)
        self.assertEqual(grouped["A"].max_amount, 20.0)
        self.assertEqual(grouped["A"].n_units, 1)
        self.assertEqual(grouped["B"].n_amount, 1)
        self.assertEqual(grouped["B"].sum_amount, 5.0)

        overall = namespace["df_means_2"].first()
        self.assertEqual(overall.sum_amount, 134.0)
        self.assertAlmostEqual(overall.mean_amount, 33.5)
        missing_class = {
            row.region: row.n_amount
            for row in namespace["df_missing_class_summary"].collect()
        }
        self.assertEqual(missing_class[None], 1)

    def test_proc_means_additional_statistics_and_output_names_execute(self):
        source = """
        proc means data=work.sales n nmiss mean std stderr var cv;
          var amount units centered;
          output out=work.stats
            n=valid_amount valid_units valid_centered
            nmiss=missing_amount missing_units missing_centered
            mean=avg_amount avg_units avg_centered;
        run;
        """
        df_sales = self.spark.createDataFrame(
            [
                (10.0, 2.0, -1.0),
                (20.0, None, 1.0),
                (None, 6.0, 0.0),
            ],
            ["amount", "units", "centered"],
        )
        namespace = {"spark": self.spark, "df_sales": df_sales}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        result = namespace["df_stats"].first()
        self.assertEqual(result.valid_amount, 2)
        self.assertEqual(result.missing_amount, 1)
        self.assertEqual(result.avg_amount, 15.0)
        self.assertAlmostEqual(result.stderr_amount, 5.0)
        self.assertAlmostEqual(result.var_amount, 50.0)
        self.assertAlmostEqual(result.std_amount, 50.0**0.5)
        self.assertAlmostEqual(result.cv_amount, (50.0**0.5 / 15.0) * 100)
        self.assertEqual(result.valid_units, 2)
        self.assertEqual(result.missing_units, 1)
        self.assertIsNone(result.cv_centered)

    def test_proc_transpose_executes_in_explicit_order_with_by_groups(self):
        source = """
        proc transpose data=work.values out=work.transposed order=sequence;
          by region;
          var amount quantity;
        run;
        """
        df_values = self.spark.createDataFrame(
            [
                ("north", 2, 20, 200),
                ("south", 1, 10, 100),
                ("north", 1, 11, 110),
                ("south", 2, 21, 210),
            ],
            ["region", "sequence", "amount", "quantity"],
        )
        namespace = {"spark": self.spark, "df_values": df_values}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        rows = {
            (row.region, row._NAME_): (row.COL1, row.COL2)
            for row in namespace["df_transposed"].collect()
        }
        self.assertEqual(rows[("north", "amount")], (11, 20))
        self.assertEqual(rows[("north", "quantity")], (110, 200))
        self.assertEqual(rows[("south", "amount")], (10, 21))
        self.assertEqual(rows[("south", "quantity")], (100, 210))

    def test_proc_transpose_rejects_duplicate_order_values(self):
        source = """
        proc transpose data=work.values order=sequence;
          var amount;
        run;
        """
        df_values = self.spark.createDataFrame(
            [(1, 10), (1, 20)],
            ["sequence", "amount"],
        )
        namespace = {"spark": self.spark, "df_values": df_values}
        generated = SasToPySparkConverter().convert(source)
        with self.assertRaisesRegex(ValueError, "must be unique"):
            exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

    def test_conditional_assignment_executes_on_sample_dataframe(self):
        source = """
        data work.graded;
          set work.employees;
          if salary ge 100000 then grade = 'A';
          else grade = 'B';
        run;
        """
        df_employees = self.spark.createDataFrame(
            [(120000, "Alice"), (85000, "Bob")],
            ["salary", "name"],
        )
        namespace = {"spark": self.spark, "df_employees": df_employees}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        grades = {
            row.name: row.grade for row in namespace["df_graded"].collect()
        }
        self.assertEqual(grades, {"Alice": "A", "Bob": "B"})

    def test_select_when_executes_in_source_order(self):
        source = """
        data work.graded;
          set work.scores;
          select;
            when (score ge 90) grade = 'A';
            when (score ge 75) grade = 'B';
            otherwise grade = 'C';
          end;
        run;
        """
        df_scores = self.spark.createDataFrame(
            [(95,), (80,), (50,), (None,)],
            ["score"],
        )
        namespace = {"spark": self.spark, "df_scores": df_scores}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        grades = {
            row.score: row.grade for row in namespace["df_graded"].collect()
        }
        self.assertEqual(grades, {95: "A", 80: "B", 50: "C", None: "C"})

    def test_output_do_loop_emits_rows_for_each_index(self):
        source = """
        data work.expanded;
          set work.source;
          do index = 1 to 5 by 2;
            scaled = index * 10;
            output;
          end;
          keep id index scaled;
        run;
        """
        df_source = self.spark.createDataFrame([(7,), (9,)], ["id"])
        namespace = {"spark": self.spark, "df_source": df_source}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        rows = namespace["df_expanded"].orderBy("id", "index").collect()
        self.assertEqual(
            [(row["id"], row["index"], row["scaled"]) for row in rows],
            [
                (7, 1, 10),
                (7, 3, 30),
                (7, 5, 50),
                (9, 1, 10),
                (9, 3, 30),
                (9, 5, 50),
            ],
        )

    def test_output_do_loop_with_no_iterations_emits_no_rows(self):
        source = """
        data work.empty_expansion;
          set work.source;
          do index = 5 to 1;
            output;
          end;
          keep id index;
        run;
        """
        df_source = self.spark.createDataFrame([(7,)], ["id"])
        namespace = {"spark": self.spark, "df_source": df_source}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        result = namespace["df_empty_expansion"]
        self.assertEqual(result.count(), 0)
        self.assertEqual(result.columns, ["id", "index"])

    def test_merge_by_matches_unique_keys_and_later_dataset_overwrites(self):
        source = """
        data work.merged;
          merge work.left_data work.right_data;
          by id;
        run;
        """
        df_left_data = self.spark.createDataFrame(
            [(1, "left-1", "left"), (2, "left-2", "old")],
            ["id", "left_value", "shared_value"],
        )
        df_right_data = self.spark.createDataFrame(
            [
                (2, "right-2", None),
                (3, "right-3", "right"),
                (None, "right-missing", "missing-key"),
            ],
            ["id", "right_value", "shared_value"],
        )
        df_final_data = self.spark.createDataFrame(
            [
                (2, "final"),
                (4, "final-only"),
                (None, "final-missing"),
            ],
            ["id", "shared_value"],
        )
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        left_schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("left_value", StringType(), True),
                StructField("shared_value", StringType(), True),
            ]
        )
        df_left_data = df_left_data.unionByName(
            self.spark.createDataFrame(
                [(None, "left-missing", "old-missing")],
                left_schema,
            )
        )
        namespace = {
            "spark": self.spark,
            "df_left_data": df_left_data,
            "df_right_data": df_right_data,
            "df_final_data": df_final_data,
        }
        source = """
        data work.merged;
          merge work.left_data work.right_data work.final_data;
          by id;
        run;
        """
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        rows = namespace["df_merged"].collect()
        self.assertEqual(
            [
                (
                    row["id"],
                    row["left_value"],
                    row["right_value"],
                    row["shared_value"],
                )
                for row in rows
            ],
            [
                (None, "left-missing", "right-missing", "final-missing"),
                (1, "left-1", None, "left"),
                (2, "left-2", "right-2", "final"),
                (3, None, "right-3", "right"),
                (4, None, None, "final-only"),
            ],
        )

    def test_merge_by_orders_output_using_descending_modifiers(self):
        source = """
        data work.merged_descending;
          merge work.left_data work.right_data;
          by descending period region;
        run;
        """
        namespace = {
            "spark": self.spark,
            "df_left_data": self.spark.createDataFrame(
                [(1, "A", "left-1"), (3, "A", "left-3")],
                ["period", "region", "left_value"],
            ),
            "df_right_data": self.spark.createDataFrame(
                [(1, "A", "right-1"), (3, "A", "right-3")],
                ["period", "region", "right_value"],
            ),
        }
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        self.assertEqual(
            [row.period for row in namespace["df_merged_descending"].collect()],
            [3, 1],
        )

    def test_merge_by_handles_duplicate_keys_and_in_flags(self):
        source = """
        data work.one_to_many;
          merge work.dup_left(in=inl) work.single_right(in=inr);
          by id;
          if inl;
        run;
        data work.many_to_many;
          merge work.dup_left work.dup_right;
          by id;
        run;
        data work.only_right;
          merge work.dup_left(in=inl) work.single_right(in=inr);
          by id;
          if inr and not inl;
        run;
        """
        namespace = {
            "spark": self.spark,
            "df_dup_left": self.spark.createDataFrame(
                [(1, "a1"), (1, "a2"), (2, "b1")], ["id", "lv"]
            ),
            "df_single_right": self.spark.createDataFrame(
                [(1, "r1"), (3, "r3")], ["id", "rv"]
            ),
            "df_dup_right": self.spark.createDataFrame(
                [(1, "x1"), (1, "x2"), (1, "x3"), (2, "y1")], ["id", "xv"]
            ),
        }
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)
        one = sorted(
            (r.id, r.lv, r.rv) for r in namespace["df_one_to_many"].collect()
        )
        self.assertEqual(one, [(1, "a1", "r1"), (1, "a2", "r1"), (2, "b1", None)])
        many = sorted(
            (r.id, r.lv, r.xv) for r in namespace["df_many_to_many"].collect()
        )
        self.assertEqual(
            many,
            [(1, "a1", "x1"), (1, "a2", "x2"), (1, "a2", "x3"), (2, "b1", "y1")],
        )
        self.assertEqual(
            [(r.id, r.rv) for r in namespace["df_only_right"].collect()],
            [(3, "r3")],
        )
        self.assertNotIn("inl", namespace["df_one_to_many"].columns)

    def test_common_sas_functions_execute_with_expected_values(self):
        source = """
        data work.function_results;
          set work.function_input;
          joined = catx(' ', first, middle, last);
          part = substr(code, 2, 2);
          selected = compress(raw, '- ');
          no_spaces = compress(raw);
          rounded = round(amount, 0.01);
          whole = round(amount);
        run;
        """
        df_function_input = self.spark.createDataFrame(
            [
                (" Ada ", "", "Lovelace", "ABCD", "A-B C", Decimal("12.345")),
                ("Grace", None, "Hopper", "WXYZ", "X Y", Decimal("-1.235")),
            ],
            ["first", "middle", "last", "code", "raw", "amount"],
        )
        namespace = {"spark": self.spark, "df_function_input": df_function_input}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        rows = namespace["df_function_results"].collect()
        self.assertEqual(
            [
                (
                    row.joined,
                    row.part,
                    row.selected,
                    row.no_spaces,
                    row.rounded,
                    row.whole,
                )
                for row in rows
            ],
            [
                ("Ada Lovelace", "BC", "ABC", "A-BC", Decimal("12.35"), Decimal("12")),
                ("Grace Hopper", "XY", "XY", "XY", Decimal("-1.24"), Decimal("-1")),
            ],
        )

    def test_missing_and_concatenation_functions_match_documented_rules(self):
        source = """
        data work.missing_results;
          set work.missing_input;
          raw_join = cat(first, middle, last);
          clean_join = cats(first, middle, last);
          selected = catx('-', first, middle, last);
          first_present = coalescec(first, middle, last);
          numeric_present = coalesce(score, backup_score);
          cleaned = strip(comment);
        run;
        """
        from pyspark.sql.types import (
            DecimalType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("first", StringType(), True),
                StructField("middle", StringType(), True),
                StructField("last", StringType(), True),
                StructField("score", DecimalType(8, 2), True),
                StructField("backup_score", DecimalType(8, 2), True),
                StructField("comment", StringType(), True),
            ]
        )
        df_missing_input = self.spark.createDataFrame(
            [
                (" A ", "", None, None, Decimal("7.00"), "  note "),
                ("", "  ", None, None, None, None),
                (None, None, "B", Decimal("3.00"), Decimal("9.00"), "  "),
            ],
            schema,
        )
        namespace = {"spark": self.spark, "df_missing_input": df_missing_input}
        generated = SasToPySparkConverter().convert(source)
        exec(compile(generated.code, "<generated-pyspark>", "exec"), namespace)

        results = namespace["df_missing_results"].collect()
        self.assertEqual(
            [
                (
                    row.raw_join,
                    row.clean_join,
                    row.selected,
                    row.first_present,
                    row.numeric_present,
                    row.cleaned,
                )
                for row in results
            ],
            [
                (" A ", "A", "A", " A ", Decimal("7.00"), "note"),
                ("  ", "", "", "", None, ""),
                ("B", "B", "B", "B", Decimal("3.00"), ""),
            ],
        )


if __name__ == "__main__":
    unittest.main()
