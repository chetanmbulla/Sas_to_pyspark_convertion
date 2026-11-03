import unittest

from sas2pyspark.sas_parser import SasParseError, parse_program


class SasParserTests(unittest.TestCase):
    def test_parser_keeps_statement_text_tokens_and_source_spans(self):
        source = """
        data work.out;
          set work.source;
          where amount >= 1.25;
        run;
        """

        program = parse_program(source)

        self.assertEqual(
            [statement.text for statement in program.statements],
            [
                "data work.out",
                "set work.source",
                "where amount >= 1.25",
                "run",
            ],
        )
        data_statement = program.statements[0]
        self.assertEqual(data_statement.span.start_line, 2)
        self.assertEqual(data_statement.span.start_column, 9)
        self.assertEqual(data_statement.tokens[0].kind, "IDENTIFIER")
        self.assertEqual(data_statement.tokens[0].text.casefold(), "data")
        self.assertEqual(data_statement.tokens[1].text, "work")
        self.assertEqual(data_statement.tokens[2].text, ".")
        self.assertEqual(program.statements[2].tokens[2].kind, "OPERATOR")
        self.assertEqual(program.statements[2].tokens[2].text, ">=")

    def test_parser_handles_quoted_semicolons_doubled_quotes_and_comments(self):
        program = parse_program(
            "/* header ; comment */\n"
            "* statement comment ignored;\n"
            "data out;\n"
            "value = 'a;''b'; /* inline ; comment */\n"
            "run;\n"
        )

        self.assertEqual(
            [statement.text for statement in program.statements],
            ["data out", "value = 'a;''b'", "run"],
        )
        string_token = program.statements[1].tokens[-1]
        self.assertEqual(string_token.kind, "STRING")
        self.assertEqual(string_token.text, "'a;''b'")
        self.assertEqual(program.statements[-1].span.start_line, 5)

    def test_parser_tokenizes_macro_constructs_and_variables(self):
        program = parse_program(
            "%let input=work.people;\n"
            "data out;\n"
            "set &input.;\n"
            "%if 1 %then %do;\n"
            "run;\n"
        )

        self.assertEqual(program.statements[0].tokens[0].kind, "MACRO_DIRECTIVE")
        self.assertEqual(program.statements[2].tokens[1].kind, "MACRO_VARIABLE")
        self.assertEqual(
            [token.text.casefold() for token in program.statements[3].tokens[:3]],
            ["%if", "1", "%then"],
        )

    def test_parser_reports_unterminated_quotes_and_comments_with_source_lines(self):
        invalid_sources = (
            ("data out;\n  value='unfinished;\n", 2, "Unterminated quoted string"),
            ("data out;\n/* unfinished", 2, "Unterminated SAS block comment"),
            ("data out;\n* unfinished", 2, "Unterminated SAS statement comment"),
        )
        for source, line, expected in invalid_sources:
            with self.subTest(source=source):
                with self.assertRaises(SasParseError) as context:
                    parse_program(source)
                self.assertEqual(context.exception.line, line)
                self.assertIn(expected, str(context.exception))

    def test_parser_requires_semicolon_terminated_statements(self):
        with self.assertRaises(SasParseError) as context:
            parse_program("data out;\n  set source")

        self.assertEqual(context.exception.line, 2)
        self.assertEqual(context.exception.statement, "set source")


if __name__ == "__main__":
    unittest.main()
