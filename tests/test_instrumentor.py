from django.test import TestCase

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor


class DjangoQ2InstrumentorTests(TestCase):
    def test_instrument_and_uninstrument_smoke(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument()
        instrumentor.uninstrument()
