from __future__ import annotations

from unittest import mock

from django_q.signals import post_execute_in_worker, pre_execute
from opentelemetry import trace
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace.status import StatusCode

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.utils import (
    OTEL_CARRIER_KEY,
    retrieve_task_context,
)


def _carrier_from(trace_id: int, span_id: int, sampled: bool = True) -> dict[str, str]:
    flags = "01" if sampled else "00"
    return {"traceparent": f"00-{format(trace_id, '032x')}-{format(span_id, '016x')}-{flags}"}


class PreExecuteConsumerStartTests(TestBase):
    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _build_task(self, **overrides):
        task = {
            "id": "task-id-xyz",
            "name": "demo",
            "func": "tests.fixtures.noop",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
            OTEL_CARRIER_KEY: _carrier_from(0x11112222333344445555666677778888, 0xAAAABBBBCCCCDDDD),
        }
        task.update(overrides)
        return task

    def test_pre_execute_starts_consumer_span_as_child_of_carrier(self):
        from opentelemetry import context as context_api

        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)

        # Span hasn't ended yet — inspect via the stored context.
        ctx = retrieve_task_context(task["id"])
        self.assertIsNotNone(ctx)
        span, activation, token = ctx
        self.assertEqual(span.kind, trace.SpanKind.CONSUMER)
        parent = span.parent
        self.assertIsNotNone(parent)
        self.assertEqual(parent.trace_id, 0x11112222333344445555666677778888)
        self.assertEqual(parent.span_id, 0xAAAABBBBCCCCDDDD)
        activation.__exit__(None, None, None)
        if token is not None:
            context_api.detach(token)

    def test_pre_execute_sets_messaging_attributes(self):
        task = self._build_task(group="reports")

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        spans = self.memory_exporter.get_finished_spans()
        span = next(s for s in spans if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(span.attributes["messaging.message.id"], "task-id-xyz")
        self.assertEqual(span.attributes["messaging.system"], "django_q2")
        self.assertEqual(span.attributes["messaging.destination.name"], "test-cluster")
        self.assertEqual(span.attributes["messaging.operation.type"], "process")
        # The legacy `messaging.operation` key was deprecated by upstream semconv
        # in favour of `messaging.operation.type`. We don't emit it, and we guard
        # against a regression that quietly reintroduces it (it would show up as
        # duplicate data on dashboards that key on operation type).
        self.assertNotIn("messaging.operation", span.attributes)
        self.assertEqual(span.attributes["django_q2.task.name"], "demo")
        self.assertEqual(span.attributes["django_q2.group"], "reports")
        self.assertEqual(span.attributes["messaging.message.conversation_id"], "reports")
        self.assertEqual(span.attributes["django_q2.func"], "tests.fixtures.noop")

    def test_pre_execute_omits_conversation_id_when_no_group(self):
        task = self._build_task()  # no group override

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertNotIn("messaging.message.conversation_id", consumer.attributes)
        self.assertNotIn("django_q2.group", consumer.attributes)

    def test_pre_execute_sets_extra_attribute_pack(self):
        def some_callable():
            pass

        task = self._build_task(
            cached=True,
            sync=True,
            ack_failure=True,
            hook="tasks.callback",
            iter_count=3,
            chain=["task_b", "task_c", "task_d"],
        )
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertIs(consumer.attributes["django_q2.cached"], True)
        self.assertIs(consumer.attributes["django_q2.sync"], True)
        self.assertIs(consumer.attributes["django_q2.ack_failure"], True)
        self.assertEqual(consumer.attributes["django_q2.hook"], "tasks.callback")
        self.assertEqual(consumer.attributes["django_q2.iter_count"], 3)
        self.assertEqual(consumer.attributes["django_q2.chain_length"], 3)

        # And a callable `hook` is intentionally NOT recorded — the repr leaks a
        # memory address that's useless for filtering.
        task_callable = self._build_task(id="task-id-callable-hook", hook=some_callable)
        pre_execute.send(sender="django_q", func=lambda: None, task=task_callable)
        post_execute_in_worker.send(
            sender="django_q",
            func=None,
            task={**task_callable, "success": True, "result": None},
        )
        consumer_b = next(
            s
            for s in self.memory_exporter.get_finished_spans()
            if s.kind == trace.SpanKind.CONSUMER and s.attributes.get("messaging.message.id") == "task-id-callable-hook"
        )
        self.assertNotIn("django_q2.hook", consumer_b.attributes)

    def test_pre_execute_omits_extra_attribute_pack_when_absent(self):
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        for key in (
            "django_q2.cached",
            "django_q2.sync",
            "django_q2.ack_failure",
            "django_q2.hook",
            "django_q2.iter_count",
            "django_q2.chain_length",
            "django_q2.attempt",
        ):
            self.assertNotIn(key, consumer.attributes)
        # `django_q2.timeout` is the exception in this pack: even with a minimal task,
        # the consumer side falls back to Conf.TIMEOUT — the testapp settings declare
        # `"timeout": 60`, so the attribute IS present despite no task["timeout"].
        self.assertEqual(consumer.attributes["django_q2.timeout"], 60)

    def test_pre_execute_stamps_attempt_attribute_when_present(self):
        # Juice fork: `django_q/pusher.py` stamps the next-attempt number onto
        # the task dict before pre_execute fires. The handler reads it
        # verbatim — including N >= 2 on re-deliveries, which is the whole
        # reason this attribute exists.
        task = self._build_task(attempt=3)
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.attributes["django_q2.attempt"], 3)

    def test_pre_execute_stamps_attempt_one_on_first_delivery(self):
        # Don't gate on `>1`: stamping on attempt 1 too lets dashboards filter
        # "attempt > 1" themselves AND keeps "field absent" a clean
        # upstream/sync-mode signal (no instrumentation for retries).
        task = self._build_task(attempt=1)
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.attributes["django_q2.attempt"], 1)

    def test_pre_execute_omits_attempt_when_absent(self):
        # Upstream `django-q2 1.10.x` doesn't stamp the field. Sync-mode bypasses
        # the pusher entirely even on the fork. Either way: no attribute, so
        # operators reading dashboards know they're not on the fork's pusher path.
        task = self._build_task()
        self.assertNotIn("attempt", task)

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertNotIn("django_q2.attempt", consumer.attributes)

    def test_pre_execute_ignores_non_positive_attempt(self):
        # `attempt=0` should never reach us (the fork's formula always yields
        # >= 1), but the `_is_positive_int` guard turns a defensive accident
        # into "attribute not stamped" — the safer side. Same shape as the
        # `timeout` defenses.
        for bad in (0, -1, True, False, None, "1"):
            with self.subTest(bad=bad):
                task = self._build_task(id=f"task-id-attempt-{bad}", attempt=bad)
                pre_execute.send(sender="django_q", func=lambda: None, task=task)
                post_execute_in_worker.send(
                    sender="django_q", func=None, task={**task, "success": True, "result": None}
                )

                spans = self.memory_exporter.get_finished_spans()
                consumer = next(
                    s
                    for s in spans
                    if s.kind == trace.SpanKind.CONSUMER and s.attributes.get("messaging.message.id") == task["id"]
                )
                self.assertNotIn("django_q2.attempt", consumer.attributes)

    def test_pre_execute_sets_broker_type_from_configured_orm_backend(self):
        # testapp settings: `"orm": "default"` ⇒ broker.type resolves to "orm" at
        # _instrument() time and is stamped on the consumer span (and on the producer,
        # tested separately). Mirrors django-q2's get_broker precedence.
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.attributes["django_q2.broker.type"], "orm")

    def test_pre_execute_prefers_task_timeout_over_conf_fallback(self):
        # When the caller passed `timeout=N` on the task, that's the budget the
        # Sentinel will enforce for THIS task — even if Conf.TIMEOUT differs.
        # (Tests can still pass `timeout=` directly because they simulate
        # pre_execute manually; the real worker would have popped task["timeout"]
        # by this point — see the `_otel_timeout` stash test for that scenario.)
        task = self._build_task(timeout=120)
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.attributes["django_q2.timeout"], 120)

    def test_pre_execute_recovers_caller_timeout_from_stash_after_worker_popped_it(self):
        # In production django-q2's worker.py pops `task["timeout"]` before
        # firing pre_execute (uses it to set the Sentinel kill alarm). This test
        # mirrors that reality: the live key is gone, but the producer-side
        # `_otel_timeout` stash carries the caller's value through, and we read
        # it instead of falling back to Conf.TIMEOUT (which would be wrong here
        # — Conf.TIMEOUT is the cluster default, NOT the per-task override).
        task = self._build_task(_otel_timeout=120)
        # `task["timeout"]` is intentionally absent — that's what the worker leaves.
        self.assertNotIn("timeout", task)

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.attributes["django_q2.timeout"], 120)

    def test_pre_execute_uses_conf_timeout_when_task_lacks_one(self):
        # No task["timeout"] ⇒ stamp the worker's configured budget (testapp = 60).
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.attributes["django_q2.timeout"], 60)

    def test_pre_execute_skips_timeout_when_neither_task_nor_conf_has_one(self):
        # Production case where the user never sets `Q_CLUSTER["timeout"]`: django-q2
        # leaves Conf.TIMEOUT as None. The attribute must be absent — stamping `null`
        # or `0` would mislead "duration ≥ timeout" alerting queries.
        task = self._build_task()
        with mock.patch(
            "opentelemetry_instrumentation_django_q2.instrumentor._read_configured_timeout",
            return_value=None,
        ):
            pre_execute.send(sender="django_q", func=lambda: None, task=task)
            post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertNotIn("django_q2.timeout", consumer.attributes)

    def test_pre_execute_skips_timeout_when_task_value_is_zero_and_no_fallback(self):
        # `timeout=0` on the task is not a real budget. If Conf has no positive
        # timeout either, the attribute is absent (we don't pick up the 0).
        task = self._build_task(timeout=0)
        with mock.patch(
            "opentelemetry_instrumentation_django_q2.instrumentor._read_configured_timeout",
            return_value=None,
        ):
            pre_execute.send(sender="django_q", func=lambda: None, task=task)
            post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertNotIn("django_q2.timeout", consumer.attributes)

    def test_pre_execute_consumer_span_is_current_during_execution(self):
        from opentelemetry import context as context_api

        task = self._build_task()
        observed: list[int] = []

        def fake_func():
            observed.append(trace.get_current_span().get_span_context().span_id)

        pre_execute.send(sender="django_q", func=fake_func, task=task)
        fake_func()
        ctx = retrieve_task_context(task["id"])
        span, activation, token = ctx
        self.assertEqual(observed[-1], span.get_span_context().span_id)
        activation.__exit__(None, None, None)
        if token is not None:
            context_api.detach(token)

    def test_pre_execute_without_carrier_still_creates_span(self):
        task = self._build_task()
        task.pop(OTEL_CARRIER_KEY)

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        spans = self.memory_exporter.get_finished_spans()
        consumer = next(s for s in spans if s.kind == trace.SpanKind.CONSUMER)
        # No carrier means no parent.
        self.assertIsNone(consumer.parent)


class PostExecuteInWorkerEndTests(TestBase):
    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _build_task(self, **overrides):
        task = {
            "id": "task-id-end",
            "name": "demo",
            "func": "tests.fixtures.noop",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
        }
        task.update(overrides)
        return task

    def test_success_path_ends_span_unset_status(self):
        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        task["success"] = True
        task["result"] = 42
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        spans = self.memory_exporter.get_finished_spans()
        consumer = next(s for s in spans if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.UNSET)
        self.assertEqual(consumer.events, ())
        # Mirror of Celery's `celery.state` — lets dashboards filter terminal state
        # without parsing the OTel status code.
        self.assertEqual(consumer.attributes["django_q2.state"], "success")
        self.assertIsNone(retrieve_task_context(task["id"]))

    def test_failure_without_exc_info_falls_back_to_string_parsing(self):
        # Upstream `django-q2 1.10.x` doesn't pass an `exc_info` kwarg on
        # `post_execute_in_worker` — the handler must still recover the
        # exception shape from `task["result"]` (the regex-parse fallback).
        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        task["success"] = False
        task["result"] = (
            "boom! : Traceback (most recent call last):\n"
            '  File "/app/tests/fixtures.py", line 17, in boom\n'
            '    raise RuntimeError("boom!")\n'
            "RuntimeError: boom!"
        )
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.ERROR)
        self.assertEqual(consumer.status.description, "boom!")
        self.assertEqual(consumer.attributes["django_q2.state"], "error")
        self.assertEqual(len(consumer.events), 1)
        event = consumer.events[0]
        self.assertEqual(event.name, "exception")
        self.assertEqual(event.attributes["exception.type"], "RuntimeError")
        self.assertEqual(event.attributes["exception.message"], "boom!")
        self.assertIn("RuntimeError: boom!", event.attributes["exception.stacktrace"])

    def test_failure_without_traceback_separator_records_status_only(self):
        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        task["success"] = False
        task["result"] = "just-an-error-string"
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.ERROR)
        self.assertEqual(consumer.status.description, "just-an-error-string")
        # No traceback in the input ⇒ we still emit an event with just the message.
        self.assertEqual(len(consumer.events), 1)
        event = consumer.events[0]
        self.assertEqual(event.attributes["exception.message"], "just-an-error-string")
        self.assertNotIn("exception.type", event.attributes)
        self.assertNotIn("exception.stacktrace", event.attributes)

    def test_failure_with_live_exception_uses_record_exception(self):
        # Juice fork path: `django_q/worker.py` forwards `sys.exc_info()` as a
        # signal kwarg. The handler must prefer the live exception over
        # `task["result"]` and let the SDK emit the standard `exception` event
        # via `record_exception`. The status description must be `str(exc)` —
        # not the `" : "` split prefix from the formatted string — so dashboards
        # see the exception's real top-level message even when it spans
        # multiple lines.
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)

        try:
            raise RuntimeError("boom-live")
        except RuntimeError:
            import sys

            live_exc_info = sys.exc_info()

        # `task["result"]` carries a deliberately *different* message so we
        # can prove the live exception wins. If the handler regressed and used
        # the string path, the description would be "different-from-live".
        post_execute_in_worker.send(
            sender="django_q",
            func=None,
            task={**task, "success": False, "result": "different-from-live : ..."},
            exc_info=live_exc_info,
        )

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.ERROR)
        self.assertEqual(consumer.status.description, "boom-live")
        self.assertEqual(consumer.attributes["django_q2.state"], "error")
        self.assertEqual(len(consumer.events), 1)
        event = consumer.events[0]
        self.assertEqual(event.name, "exception")
        self.assertEqual(event.attributes["exception.type"], "RuntimeError")
        self.assertEqual(event.attributes["exception.message"], "boom-live")
        # The SDK fills `exception.stacktrace` from the live traceback — that's
        # the bit that's strictly richer than the string-parse fallback (real
        # frames, no formatting drift).
        self.assertIn("RuntimeError: boom-live", event.attributes["exception.stacktrace"])

    def test_failure_with_chained_cause_records_each_link(self):
        # Load-bearing test for why the live-exception path exists at all:
        # `raise B from A` produces a cause chain (`__cause__`) that the SDK's
        # `record_exception` walks, emitting one `exception` event per link.
        # The string-parse fallback can only see the outer exception — without
        # this assertion, the live path is indistinguishable from the fallback.
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)

        try:
            try:
                raise ValueError("inner-cause")
            except ValueError as inner:
                raise RuntimeError("outer-failure") from inner
        except RuntimeError:
            import sys

            live_exc_info = sys.exc_info()

        post_execute_in_worker.send(
            sender="django_q",
            func=None,
            task={**task, "success": False, "result": "outer-failure : ..."},
            exc_info=live_exc_info,
        )

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.ERROR)
        self.assertEqual(consumer.status.description, "outer-failure")
        # Two exception events — one per cause link. The order the SDK emits
        # them in is "outermost first" (the exception we passed, then its
        # __cause__).
        self.assertEqual(len(consumer.events), 2)
        types = [event.attributes["exception.type"] for event in consumer.events]
        messages = [event.attributes["exception.message"] for event in consumer.events]
        self.assertIn("RuntimeError", types)
        self.assertIn("ValueError", types)
        self.assertIn("outer-failure", messages)
        self.assertIn("inner-cause", messages)

    def test_failure_with_python_3_11_note_records_note(self):
        # PEP 678 `add_note()` lets callers attach extra context to an exception
        # (added in Python 3.11). The SDK's `record_exception` surfaces those
        # notes in `exception.stacktrace`. Falls back gracefully on older
        # Pythons (the `add_note` call is guarded), where the note is simply
        # not present in the emitted event.
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)

        try:
            exc = RuntimeError("notable-boom")
            if hasattr(exc, "add_note"):  # Python 3.11+
                exc.add_note("retryable=True")
            raise exc
        except RuntimeError:
            import sys

            live_exc_info = sys.exc_info()

        post_execute_in_worker.send(
            sender="django_q",
            func=None,
            task={**task, "success": False, "result": "notable-boom : ..."},
            exc_info=live_exc_info,
        )

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        event = consumer.events[0]
        self.assertEqual(event.attributes["exception.type"], "RuntimeError")
        self.assertEqual(event.attributes["exception.message"], "notable-boom")
        if hasattr(RuntimeError("x"), "add_note"):
            self.assertIn("retryable=True", event.attributes["exception.stacktrace"])

    def test_success_path_with_exc_info_none_kwarg_is_noop(self):
        # The juice fork always passes the kwarg, including `exc_info=None` on
        # success. The handler must treat that exactly like upstream's no-kwarg
        # success path: no exception event, no error status, `django_q2.state`
        # stamped "success".
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(
            sender="django_q",
            func=None,
            task={**task, "success": True, "result": 42},
            exc_info=None,
        )

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.UNSET)
        self.assertEqual(consumer.events, ())
        self.assertEqual(consumer.attributes["django_q2.state"], "success")

    def test_failure_with_malformed_exc_info_falls_back_to_string(self):
        # Defensive: if a third party (or a future fork variant) forwards a
        # malformed triple where the value slot isn't a BaseException, the
        # handler must NOT crash and must fall back to the string-parsing
        # path so the event is still emitted.
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        bad_exc_info = ("RuntimeError", "not-an-exception", None)
        post_execute_in_worker.send(
            sender="django_q",
            func=None,
            task={
                **task,
                "success": False,
                "result": ("boom-from-string : Traceback (most recent call last):\nRuntimeError: boom-from-string"),
            },
            exc_info=bad_exc_info,
        )

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.ERROR)
        self.assertEqual(consumer.status.description, "boom-from-string")
        self.assertEqual(len(consumer.events), 1)
        event = consumer.events[0]
        self.assertEqual(event.attributes["exception.type"], "RuntimeError")
        self.assertEqual(event.attributes["exception.message"], "boom-from-string")

    def test_sync_error_branch_missing_success_key_is_tolerated(self):
        # Mirrors worker.py:113 — sync error path may emit post_execute_in_worker
        # before task["success"] / task["result"] / task["stopped"] are set.
        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.UNSET)
        self.assertEqual(consumer.events, ())
        # No `django_q2.state` either — we don't have a terminal state to record,
        # and inventing one would mislead dashboards.
        self.assertNotIn("django_q2.state", consumer.attributes)

    def test_post_execute_without_prior_pre_execute_is_noop(self):
        task = self._build_task()
        task["success"] = True

        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        self.assertEqual(self.memory_exporter.get_finished_spans(), ())

    def test_post_execute_reinjects_carrier_with_consumer_span_context(self):
        # Re-injection is the load-bearing piece of juice-fork chain continuity:
        # the carrier on the task dict must end with a traceparent that points
        # at the CONSUMER span we just closed (not the PRODUCER one originally
        # injected at pre_enqueue time). When `pre_chain_progress` fires for the
        # next chain link it extracts THIS carrier, so the link parents under
        # the previous consumer. Done unconditionally — harmless on upstream,
        # load-bearing on the fork. See HANDOFF.md.
        original_trace_id = 0x11112222333344445555666677778888
        original_span_id = 0xAAAABBBBCCCCDDDD
        task = {
            "id": "task-id-reinject",
            "name": "demo",
            "func": "tests.fixtures.noop",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
            OTEL_CARRIER_KEY: _carrier_from(original_trace_id, original_span_id),
        }
        original_traceparent = task[OTEL_CARRIER_KEY]["traceparent"]

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        # 1. The carrier was overwritten — comparing the two traceparents
        # surfaces any silent regression where re-injection stops happening.
        new_traceparent = task[OTEL_CARRIER_KEY]["traceparent"]
        self.assertNotEqual(new_traceparent, original_traceparent)

        # 2. The new traceparent's span_id portion is the CONSUMER span's id, and
        # the trace_id portion is preserved (same trace — that's the whole point).
        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        consumer_ctx = consumer.get_span_context()
        # traceparent format: 00-<trace_id_32>-<span_id_16>-<flags_2>
        parts = new_traceparent.split("-")
        self.assertEqual(parts[1], format(consumer_ctx.trace_id, "032x"))
        self.assertEqual(parts[2], format(consumer_ctx.span_id, "016x"))
        # And explicitly NOT the producer's original span_id.
        self.assertNotEqual(parts[2], format(original_span_id, "016x"))
