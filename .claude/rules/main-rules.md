# SDLC rules

Imperative guidance for working on the project. Follow these end-to-end on every change.

## Code style

- Write a test for every implementation change. No exception for "trivial" fixes.
- Use parameterized logging only: `_logger.info("Message %s", value)`. Do **not** pre-format log strings with f-strings, `%`, or `.format()`.
- When you see `getLogger(__name__)`, change it to `getLogger("opentelemetry_instrumentation_django_q2")`.

## Updating poetry settings

- Update `poetry.lock` when `pyproject.toml` changes:
    ```bash
    docker compose run --rm --remove-orphans integration-tests poetry update
    ```
- Build the Docker images to reflect the changes:
    ```bash
    docker compose build integration-tests lint-formatter
    ```

## Testing

- **Selective runs**: replace `<test_module>` with the dotted path of the test module you want to run (e.g. `tests.test_instrumentor`):
    ```bash
    docker compose run --remove-orphans --rm integration-tests bash -c 'python manage.py test --noinput <test_module> > /tmp/test-output.txt 2>&1; cat /tmp/test-output.txt | python scripts/filter_failed_tests.py'
    ```
- **Coverage for selective testing**: replace `<test_module>` with the dotted path of the test module and `<path/to/source_file>.py` with the source file you want a coverage report on (e.g. `opentelemetry_instrumentation_django_q2/__init__.py`). Multiple files can be passed comma-separated:
    ```bash
    docker compose run --remove-orphans --rm integration-tests bash -c 'coverage run manage.py test --noinput <test_module> && coverage combine && coverage report --include=<path/to/source_file>.py'
    ```
- Run the full library suite before declaring a change complete:
    ```bash
    docker compose run --remove-orphans --rm integration-tests
    ```

## Lint & format

- Run after the implementation is complete (no need to re-run tests after):
    ```bash
    docker compose run --remove-orphans --rm lint-formatter
    ```

## Documentation

- Update `CHANGELOG.md` only when `./opentelemetry_instrumentation_django_q2/` (the library source) changes, but not including the `tests` folder. Use the active `[X.Y.Z]` heading and `Added` / `Changed` / `Fixed` / `Removed` subsections. Skip it for repo-tooling or sample-project edits.
- Update `README.md` when public API, install steps, or supported Python/Django/`django-q2` versions change.
- Do **not** create new top-level docs (`*.md`) unless explicitly asked.

## Commits

- Use Conventional Commits.

## Deployment

- Releases are tag-driven via `.github/workflows/publish-package.yml`. Pushing an annotated tag matching the version number publishes to PyPI.
- Never edit `pyproject.toml`'s `version` by hand — the publish workflow runs `poetry version $TAG_NAME` from the tag.
