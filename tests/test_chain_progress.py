from __future__ import annotations

import django_q.signals as django_q_signals
from django.dispatch import Signal
from django.test import TestCase
from opentelemetry.test.test_base import TestBase

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.utils import OTEL_CARRIER_KEY

# Pinned `django-q2` (upstream 1.10.0) ships only `pre_execute` / `post_execute_in_worker`
# / `pre_enqueue` / `post_spawn` / `post_execute`. The juice fork adds two more
# Signals on top: `pre_chain_progress` and `post_chain_progress`. Tests in this
# module simulate the fork by attaching/removing those attributes on the
# imported `django_q.signals` module — that's exactly what `from django_q.signals
# import pre_chain_progress` resolves against.
_FORK_SIGNAL_NAMES = ("pre_chain_progress", "post_chain_progress")


def _carrier(trace_id: int, span_id: int, sampled: bool = True) -> dict[str, str]:
    flags = "01" if sampled else "00"
    return {"traceparent": f"00-{format(trace_id, '032x')}-{format(span_id, '016x')}-{flags}"}


class _ForkPresentMixin:
    """Adds/removes the juice-fork chain-progress signals on `django_q.signals`."""

    def _install_fork_signals(self) -> None:
        for name in _FORK_SIGNAL_NAMES:
            setattr(django_q_signals, name, Signal())

    def _remove_fork_signals(self) -> None:
        for name in _FORK_SIGNAL_NAMES:
            if hasattr(django_q_signals, name):
                delattr(django_q_signals, name)


class _ForkAbsentMixin:
    """Pre-clears the chain-progress signals so the fork-absent path runs."""

    def _ensure_fork_signals_absent(self) -> None:
        for name in _FORK_SIGNAL_NAMES:
            if hasattr(django_q_signals, name):
                delattr(django_q_signals, name)


class ForkPresentInstrumentTests(_ForkPresentMixin, TestBase, TestCase):
    def setUp(self):
        super().setUp()
        self._install_fork_signals()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        self._remove_fork_signals()
        super().tearDown()

    def test_instrument_connects_when_signals_present(self):
        # Hard contract: the flag mirrors a successful connect. Other tests in this
        # module rely on it being True before sending signals.
        self.assertTrue(self.instrumentor._chain_signals_connected)

    def test_pre_chain_progress_attaches_carrier_context_for_next_link(self):
        # The fork fires `pre_chain_progress` after a link finished, with the task
        # dict whose `otel_carrier` was re-injected during `post_execute_in_worker`.
        # While that context is attached, opening a new span must parent under the
        # carrier's encoded span_id — that's what makes the next chain link land
        # on the same trace as the previous link's CONSUMER span.
        carrier_trace_id = 0xDEADBEEFCAFEBABE0011223344556677
        carrier_span_id = 0xAABBCCDDEEFF0011
        task = {
            "id": "chain-task-1",
            OTEL_CARRIER_KEY: _carrier(carrier_trace_id, carrier_span_id),
        }

        django_q_signals.pre_chain_progress.send(sender="django_q", task=task)
        try:
            tracer = self.tracer_provider.get_tracer(__name__)
            with tracer.start_as_current_span("next-link-producer") as next_span:
                self.assertEqual(next_span.get_span_context().trace_id, carrier_trace_id)
                parent = next_span.parent
                self.assertIsNotNone(parent)
                self.assertEqual(parent.span_id, carrier_span_id)
        finally:
            django_q_signals.post_chain_progress.send(sender="django_q", task=task)

    def test_post_chain_progress_detaches_token(self):
        # Token bookkeeping must drain so attaches don't leak across chains.
        task = {
            "id": "chain-task-2",
            OTEL_CARRIER_KEY: _carrier(0x11112222333344445555666677778888, 0xAAAABBBBCCCCDDDD),
        }

        django_q_signals.pre_chain_progress.send(sender="django_q", task=task)
        self.assertIn("chain-task-2", self.instrumentor._chain_progress_tokens)

        django_q_signals.post_chain_progress.send(sender="django_q", task=task)
        self.assertNotIn("chain-task-2", self.instrumentor._chain_progress_tokens)

    def test_pre_chain_progress_without_carrier_is_noop(self):
        # Missing/empty carrier — nothing to extract, nothing to store. Must not
        # raise, and must not leave a token behind that the matching
        # post_chain_progress can't clean up.
        task_no_carrier: dict = {"id": "chain-task-3"}
        task_empty_carrier = {"id": "chain-task-4", OTEL_CARRIER_KEY: {}}

        django_q_signals.pre_chain_progress.send(sender="django_q", task=task_no_carrier)
        django_q_signals.pre_chain_progress.send(sender="django_q", task=task_empty_carrier)

        self.assertEqual(self.instrumentor._chain_progress_tokens, {})

    def test_pre_chain_progress_without_task_id_detaches_immediately(self):
        # Without an id we can't pair the attach with a later post_chain_progress;
        # leaving it attached would leak the consumer context into subsequent
        # monitor work. Guard: the handler must detach immediately and not record
        # the token.
        task = {OTEL_CARRIER_KEY: _carrier(0x12345678901234567890123456789012, 0x0123456789ABCDEF)}

        django_q_signals.pre_chain_progress.send(sender="django_q", task=task)

        self.assertEqual(self.instrumentor._chain_progress_tokens, {})
        # And outside any send-driven context, the current span must not have the
        # carrier's trace_id (i.e. the attach didn't survive the handler call).
        tracer = self.tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span("after-noid") as span:
            self.assertNotEqual(span.get_span_context().trace_id, 0x12345678901234567890123456789012)

    def test_uninstrument_disconnects_chain_signals(self):
        self.instrumentor.uninstrument()
        try:
            # Sending the signal after uninstrument must be a no-op for the
            # handler — assert it via the token map.
            task = {
                "id": "chain-task-5",
                OTEL_CARRIER_KEY: _carrier(0x11112222333344445555666677778888, 0xAAAABBBBCCCCDDDD),
            }
            django_q_signals.pre_chain_progress.send(sender="django_q", task=task)
            self.assertNotIn("chain-task-5", self.instrumentor._chain_progress_tokens)
            self.assertFalse(self.instrumentor._chain_signals_connected)
        finally:
            # Re-instrument so the shared tearDown's uninstrument doesn't double-call.
            self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def test_uninstrument_drains_stranded_tokens(self):
        # Simulate a chain interrupted between pre and post (e.g. the worker
        # process was killed mid-chain). The attach token would otherwise leak.
        task = {
            "id": "chain-task-stranded",
            OTEL_CARRIER_KEY: _carrier(0x11112222333344445555666677778888, 0xAAAABBBBCCCCDDDD),
        }
        django_q_signals.pre_chain_progress.send(sender="django_q", task=task)
        self.assertIn("chain-task-stranded", self.instrumentor._chain_progress_tokens)

        self.instrumentor.uninstrument()
        try:
            self.assertEqual(self.instrumentor._chain_progress_tokens, {})
        finally:
            self.instrumentor.instrument(tracer_provider=self.tracer_provider)


class ForkAbsentInstrumentTests(_ForkAbsentMixin, TestBase, TestCase):
    def setUp(self):
        super().setUp()
        self._ensure_fork_signals_absent()

    def tearDown(self):
        # Be paranoid: some other test in the suite might re-install the attributes
        # while this one runs. Clear again to keep failures from cross-contaminating.
        for name in _FORK_SIGNAL_NAMES:
            if hasattr(django_q_signals, name):
                delattr(django_q_signals, name)
        super().tearDown()

    def test_instrument_does_not_raise_when_signals_absent(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        try:
            self.assertFalse(instrumentor._chain_signals_connected)
        finally:
            instrumentor.uninstrument()

    def test_uninstrument_safe_when_chain_signals_were_never_connected(self):
        # Pure smoke — exercises the `if self._chain_signals_connected:` short
        # circuit so upstream-only deployments don't trip an AttributeError /
        # ImportError on teardown.
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        instrumentor.uninstrument()
        instrumentor.uninstrument()
