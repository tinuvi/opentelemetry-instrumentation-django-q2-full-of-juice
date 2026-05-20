# Upstream `django-q2` is the canonical target. The instrumentor also
# transparently activates against `django-q2-full-of-juice` (a drop-in fork
# under a different PyPI distribution name that ships extra signals — see
# `_check_dependency_conflicts` on the instrumentor). Both expose the same
# `django_q` import package, so only one of them can be installed at a time.
_instruments = ("django-q2 >= 1.10.0",)
# Either-or dependencies satisfied by *any one* match. Used to relax the
# strict `_instruments` check when the user installs the juice fork instead
# of upstream. Keep this list ordered upstream-first so error messages list
# the canonical option first.
_instruments_any = ("django-q2 >= 1.10.0", "django-q2-full-of-juice >= 0.1.0")
