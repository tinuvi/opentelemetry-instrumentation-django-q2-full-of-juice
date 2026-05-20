from importlib.metadata import version as _pkg_version
from unittest import mock

import django_q.tasks
from django.test import TestCase
from django_q.conf import Conf
from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor, __version__
from opentelemetry_instrumentation_django_q2.instrumentor import (
    _SCHEMA_URL,
    _read_configured_timeout,
    _resolve_broker_type,
)


class VersionTests(TestCase):
    """
    Guard the version.py → importlib.metadata wiring.

    Background: pyproject.toml's `version` is rewritten by `poetry version $TAG_NAME`
    at publish time, so the only source of truth at runtime is the installed dist
    metadata. If anything regresses (a placeholder string, a try/except fallback,
    a missing editable install), the tracer/meter would silently stamp the wrong
    value on every emitted span/metric.
    """

    def test_version_matches_installed_package_metadata(self):
        # Both sides read from the same dist-info entry — this asserts that
        # version.py is using importlib.metadata and not a hard-coded literal.
        self.assertEqual(__version__, _pkg_version("opentelemetry-instrumentation-django-q2-full-of-juice"))

    def test_version_is_nonempty_string(self):
        self.assertIsInstance(__version__, str)
        self.assertTrue(__version__)


class DjangoQ2InstrumentorTests(TestCase):
    def test_instrument_and_uninstrument_smoke(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument()
        instrumentor.uninstrument()


class DependencyCheckTests(TestCase):
    """
    Pins the any-of dependency check that supports the juice fork.

    `BaseInstrumentor`'s default `_check_dependency_conflicts` only accepts an
    all-of list. The two distributions `django-q2` (upstream) and
    `django-q2-full-of-juice` (fork) ship the same `django_q` import package
    under different names — only one is ever installed. Without an any-of
    override, installing the fork would make `instrument()` silently no-op
    (DependencyConflict short-circuits before `_instrument()` runs).
    """

    def test_dep_check_passes_with_upstream_django_q2_installed(self):
        # testapp's container has upstream `django-q2 1.10.0`. The any-of
        # check must accept this and return no conflict.
        instrumentor = DjangoQ2Instrumentor()
        self.assertIsNone(instrumentor._check_dependency_conflicts())

    def test_dep_check_passes_when_only_fork_is_installed(self):
        # Simulate the fork-only install: upstream `django-q2` distribution is
        # gone, `django-q2-full-of-juice` matches the version spec. Mocking the
        # metadata reader is the safest way to avoid touching the live env.
        from opentelemetry.util._importlib_metadata import PackageNotFoundError

        def fake_version(name: str) -> str:
            if name == "django-q2":
                raise PackageNotFoundError(name)
            if name == "django-q2-full-of-juice":
                return "0.1.0"
            raise PackageNotFoundError(name)

        with mock.patch(
            "opentelemetry.instrumentation.dependencies.version",
            side_effect=fake_version,
        ):
            instrumentor = DjangoQ2Instrumentor()
            self.assertIsNone(instrumentor._check_dependency_conflicts())

    def test_dep_check_reports_conflict_when_neither_distribution_is_installed(self):
        # Pure smoke: no django-q2 at all surfaces a conflict (auto-instrumentation
        # will then refuse to call _instrument). Reused contract: the conflict's
        # required_any must list BOTH options so an operator can see the choice.
        from opentelemetry.util._importlib_metadata import PackageNotFoundError

        def fake_version(name: str) -> str:
            raise PackageNotFoundError(name)

        with mock.patch(
            "opentelemetry.instrumentation.dependencies.version",
            side_effect=fake_version,
        ):
            instrumentor = DjangoQ2Instrumentor()
            conflict = instrumentor._check_dependency_conflicts()
            self.assertIsNotNone(conflict)
            # Both distributions should appear in `required_any` so the user
            # knows either upstream or the fork would resolve the conflict.
            self.assertIn("django-q2 >= 1.10.0", list(conflict.required_any))
            self.assertIn("django-q2-full-of-juice >= 0.1.0", list(conflict.required_any))

    def test_instrument_caches_resolved_broker_type(self):
        # _broker_type is computed once at _instrument() time so every span pays
        # zero Conf-read cost. Verify the cache lands and clears on uninstrument.
        instrumentor = DjangoQ2Instrumentor()
        self.assertIsNone(instrumentor._broker_type)

        instrumentor.instrument()
        try:
            # testapp's settings declare `"orm": "default"` ⇒ resolver picks "orm".
            self.assertEqual(instrumentor._broker_type, "orm")
        finally:
            instrumentor.uninstrument()

        self.assertIsNone(instrumentor._broker_type)


class ResolveBrokerTypeTests(TestCase):
    """Unit tests for the broker.type resolver — mirrors django-q2's get_broker order."""

    def test_returns_orm_for_default_testapp_settings(self):
        # The testapp Q_CLUSTER configures `"orm": "default"`, which is the default
        # case for any Django-only deployment (no Redis / Mongo / cloud broker).
        self.assertEqual(_resolve_broker_type(), "orm")

    def test_broker_class_takes_highest_precedence(self):
        # A custom BROKER_CLASS dotted path overrides every other broker hint, so
        # users running a bespoke backend can identify it precisely on every span.
        with (
            mock.patch.object(Conf, "BROKER_CLASS", "myapp.brokers.CustomBroker"),
            mock.patch.object(Conf, "ORM", "default"),
        ):
            self.assertEqual(_resolve_broker_type(), "myapp.brokers.CustomBroker")

    def test_iron_mq_wins_over_lower_priority_brokers(self):
        with (
            mock.patch.object(Conf, "BROKER_CLASS", None),
            mock.patch.object(Conf, "IRON_MQ", {"token": "x"}),
            mock.patch.object(Conf, "ORM", "default"),
        ):
            self.assertEqual(_resolve_broker_type(), "iron_mq")

    def test_sqs_resolves_only_when_config_is_a_dict(self):
        # django-q2 itself checks `isinstance(Conf.SQS, dict)` — we mirror that so a
        # truthy-but-not-dict value (e.g. someone setting `sqs=True`) doesn't slip
        # past the orm/redis fallback later in the chain.
        with (
            mock.patch.object(Conf, "BROKER_CLASS", None),
            mock.patch.object(Conf, "IRON_MQ", None),
            mock.patch.object(Conf, "SQS", {"aws_access_key_id": "x"}),
            mock.patch.object(Conf, "ORM", "default"),
        ):
            self.assertEqual(_resolve_broker_type(), "sqs")

    def test_mongo_resolves_only_when_other_brokers_are_unset(self):
        with (
            mock.patch.object(Conf, "BROKER_CLASS", None),
            mock.patch.object(Conf, "IRON_MQ", None),
            mock.patch.object(Conf, "SQS", None),
            mock.patch.object(Conf, "ORM", None),
            mock.patch.object(Conf, "MONGO", "mongodb://localhost"),
        ):
            self.assertEqual(_resolve_broker_type(), "mongo")

    def test_redis_is_the_default_fallback(self):
        # If no broker hint is present at all, django-q2 picks redis. We mirror that
        # so the attribute is always populated for any reachable code path.
        with (
            mock.patch.object(Conf, "BROKER_CLASS", None),
            mock.patch.object(Conf, "IRON_MQ", None),
            mock.patch.object(Conf, "SQS", None),
            mock.patch.object(Conf, "ORM", None),
            mock.patch.object(Conf, "MONGO", None),
        ):
            self.assertEqual(_resolve_broker_type(), "redis")


class SchemaURLTests(TestBase, TestCase):
    """
    Guard the schema URL stamped on the tracer/meter scope.

    Backends like the OTel collector use `instrumentation_scope.schema_url` to
    translate attributes between schema versions. Bumping the constant without
    a regression test would let a typo (e.g. `1.34` vs `1.34.0`) ship silently.
    """

    def test_emitted_spans_carry_the_declared_schema_url(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        try:
            django_q.tasks.async_task("tests.fixtures.noop", sync=True)
        finally:
            instrumentor.uninstrument()

        spans = self.memory_exporter.get_finished_spans()
        producer = next(s for s in spans if s.kind == trace.SpanKind.PRODUCER)
        consumer = next(s for s in spans if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(producer.instrumentation_scope.schema_url, _SCHEMA_URL)
        self.assertEqual(consumer.instrumentation_scope.schema_url, _SCHEMA_URL)
        # The constant itself must be a real opentelemetry.io schema URL; this
        # catches a bare-version typo (`1.34.0` instead of the full URL).
        self.assertTrue(_SCHEMA_URL.startswith("https://opentelemetry.io/schemas/"))


class ReadConfiguredTimeoutTests(TestCase):
    """Unit tests for the Q_CLUSTER timeout reader — guards the positive-int policy."""

    def test_returns_positive_integer_from_conf(self):
        # testapp settings declare `"timeout": 60` ⇒ helper surfaces it for the
        # consumer-side fallback in `_on_pre_execute`.
        self.assertEqual(_read_configured_timeout(), 60)

    def test_returns_none_when_conf_timeout_is_none(self):
        # django-q2's default when `Q_CLUSTER["timeout"]` is unset. The helper must
        # NOT surface None as an attribute (would mislead alerting queries).
        with mock.patch.object(Conf, "TIMEOUT", None):
            self.assertIsNone(_read_configured_timeout())

    def test_returns_none_when_conf_timeout_is_zero(self):
        # `0` would be picked up as a "valid int" by a naive truthy check, but it's
        # not a real budget — guard explicitly.
        with mock.patch.object(Conf, "TIMEOUT", 0):
            self.assertIsNone(_read_configured_timeout())

    def test_returns_none_when_conf_timeout_is_bool(self):
        # bool is an int subclass — `True` reads as 1, which is technically positive.
        # Explicitly excluded so a misconfigured `timeout=True` doesn't land as `1s`.
        with mock.patch.object(Conf, "TIMEOUT", True):
            self.assertIsNone(_read_configured_timeout())
