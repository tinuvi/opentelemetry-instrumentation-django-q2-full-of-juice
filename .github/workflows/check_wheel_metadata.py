#!/usr/bin/env python3
"""Assert a built wheel's Requires-Dist matches the issue #3 packaging contract.

Usage:
    python .github/workflows/check_wheel_metadata.py dist/*.whl

The glob is expanded by the shell into a single wheel path (poetry build emits
exactly one wheel). This is the regression guard for the poetry-core legacy-mode
footgun: adding a [project.optional-dependencies] table turns legacy mode off, so
any main dependency left only in [tool.poetry.dependencies] would silently vanish
from Requires-Dist. This script fails the build if that happens.

Checks:
    - django, opentelemetry-api, opentelemetry-instrumentation,
      opentelemetry-semantic-conventions and wrapt each appear as an
      unconditional Requires-Dist (no `extra ==` marker).
    - Neither django-q2 nor django-q2-full-of-juice appears unconditionally
      (they must only ride the instruments-any extra, never the runtime deps).
    - Both django-q2 and django-q2-full-of-juice appear gated by
      `extra == "instruments-any"`, and Provides-Extra: instruments-any is present.

Exit code: 0 if every check passes, 1 otherwise.
"""

import email
import re
import sys
import zipfile

REQUIRED_UNCONDITIONAL = (
    "django",
    "opentelemetry-api",
    "opentelemetry-instrumentation",
    "opentelemetry-semantic-conventions",
    "wrapt",
)
INSTRUMENTS_EXTRA = "instruments-any"
INSTRUMENTS_PROVIDERS = ("django-q2", "django-q2-full-of-juice")

_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
_EXTRA_RE = re.compile(r"""extra\s*==\s*['"]([^'"]+)['"]""")


def normalize(name):
    """Normalize a distribution name per PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_requirement(line):
    """Return (normalized_name, extra_or_none) for one Requires-Dist value."""
    requirement, _, marker = line.partition(";")
    match = _NAME_RE.match(requirement)
    if not match:
        return None, None
    extra_match = _EXTRA_RE.search(marker)
    extra = extra_match.group(1) if extra_match else None
    return normalize(match.group(1)), extra


def read_metadata(wheel_path):
    """Extract the METADATA message from a wheel's dist-info."""
    with zipfile.ZipFile(wheel_path) as archive:
        metadata_names = [
            name
            for name in archive.namelist()
            if name.endswith(".dist-info/METADATA")
        ]
        if not metadata_names:
            raise SystemExit(f"ERROR: no .dist-info/METADATA found in {wheel_path}")
        with archive.open(metadata_names[0]) as handle:
            return email.message_from_bytes(handle.read())


def check(wheel_path):
    """Validate the wheel metadata; return a list of error strings (empty = OK)."""
    metadata = read_metadata(wheel_path)
    requires_dist = metadata.get_all("Requires-Dist") or []
    provides_extra = metadata.get_all("Provides-Extra") or []

    unconditional = set()
    extra_gated = {}  # normalized name -> set of extras it is gated by
    for line in requires_dist:
        name, extra = parse_requirement(line)
        if name is None:
            continue
        if extra is None:
            unconditional.add(name)
        else:
            extra_gated.setdefault(name, set()).add(extra)

    errors = []

    for name in REQUIRED_UNCONDITIONAL:
        if name not in unconditional:
            errors.append(f"missing unconditional Requires-Dist for '{name}'")

    for name in INSTRUMENTS_PROVIDERS:
        if name in unconditional:
            errors.append(
                f"'{name}' appears as an UNCONDITIONAL Requires-Dist — it must "
                f"only ride the '{INSTRUMENTS_EXTRA}' extra (poetry-core legacy-mode footgun, issue #3)"
            )
        if INSTRUMENTS_EXTRA not in extra_gated.get(name, set()):
            errors.append(
                f"'{name}' is missing the `extra == \"{INSTRUMENTS_EXTRA}\"` Requires-Dist marker"
            )

    if INSTRUMENTS_EXTRA not in provides_extra:
        errors.append(f"missing 'Provides-Extra: {INSTRUMENTS_EXTRA}'")

    return requires_dist, errors


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python .github/workflows/check_wheel_metadata.py <wheel-path>")

    wheel_path = sys.argv[1]
    requires_dist, errors = check(wheel_path)

    if errors:
        print(f"FAILED: wheel metadata check for {wheel_path}", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        sys.exit(1)

    print(f"OK: wheel metadata check passed for {wheel_path}")
    print("Validated Requires-Dist lines:")
    for line in requires_dist:
        print(f"  Requires-Dist: {line}")
    sys.exit(0)


if __name__ == "__main__":
    main()
