from unittest import mock

from django.test import TestCase
from django_q.conf import Conf

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.instrumentor import (
    _read_configured_timeout,
    _resolve_broker_type,
)


class DjangoQ2InstrumentorTests(TestCase):
    def test_instrument_and_uninstrument_smoke(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument()
        instrumentor.uninstrument()

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
