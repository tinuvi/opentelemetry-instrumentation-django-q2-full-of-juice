from __future__ import annotations

from django.test import SimpleTestCase

from opentelemetry_instrumentation_django_q2.utils import parse_worker_result


class ParseWorkerResultTests(SimpleTestCase):
    def test_none_input_returns_all_none(self):
        self.assertEqual(parse_worker_result(None), (None, None, None))

    def test_no_separator_returns_message_only(self):
        self.assertEqual(parse_worker_result("plain error string"), ("plain error string", None, None))

    def test_empty_string_returns_all_none(self):
        self.assertEqual(parse_worker_result(""), (None, None, None))

    def test_typical_format_extracts_type_message_traceback(self):
        result = (
            "boom! : Traceback (most recent call last):\n"
            '  File "/app/tasks.py", line 17, in boom\n'
            '    raise RuntimeError("boom!")\n'
            "RuntimeError: boom!"
        )
        message, exc_type, stacktrace = parse_worker_result(result)
        self.assertEqual(message, "boom!")
        self.assertEqual(exc_type, "RuntimeError")
        self.assertIn("RuntimeError: boom!", stacktrace)

    def test_dotted_exception_type_is_preserved(self):
        result = (
            "user oops : Traceback (most recent call last):\n"
            '  File "/app/x.py", line 3, in f\n'
            '    raise mod.sub.CustomError("user oops")\n'
            "mod.sub.CustomError: user oops"
        )
        _, exc_type, _ = parse_worker_result(result)
        self.assertEqual(exc_type, "mod.sub.CustomError")

    def test_non_string_input_is_coerced(self):
        message, exc_type, stacktrace = parse_worker_result(42)
        self.assertEqual(message, "42")
        self.assertIsNone(exc_type)
        self.assertIsNone(stacktrace)

    def test_separator_inside_traceback_only_splits_on_first(self):
        # `traceback.format_exc()` can contain ' : ' inside nested chained
        # exception text; we must only split on the *first* occurrence so the
        # head stays a clean message.
        result = (
            "outer : Traceback (most recent call last):\n"
            '  File "/app/x.py", line 5, in outer\n'
            "    raise A() from B()\n"
            "ValueError: outer\n"
            "\n"
            "During handling of the above exception : another exception occurred:\n"
            "\n"
            "Traceback (most recent call last):\n"
            '  File "/app/x.py", line 7, in <module>\n'
            "    outer()\n"
            "RuntimeError: deeper"
        )
        message, exc_type, stacktrace = parse_worker_result(result)
        self.assertEqual(message, "outer")
        self.assertTrue(stacktrace.startswith("Traceback (most recent call last):"))
        self.assertEqual(exc_type, "RuntimeError")
