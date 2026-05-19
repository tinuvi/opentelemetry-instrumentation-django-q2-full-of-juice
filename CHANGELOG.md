# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Added
- Initial project scaffolding for `opentelemetry-instrumentation-django-q2-full-of-juice`.
- `DjangoQ2Instrumentor` skeleton (signal wiring pending) following the OpenTelemetry `BaseInstrumentor` contract.
- Per-task context storage helpers in `opentelemetry_instrumentation_django_q2.utils`.
- Test scaffolding (`tests/testapp/`) with a minimal Django settings module wired to `django-q2` in `sync=True` mode.
