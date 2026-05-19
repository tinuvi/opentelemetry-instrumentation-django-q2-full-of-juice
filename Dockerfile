FROM python:3.14-slim

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install poetry==2.4.1 && \
    poetry config virtualenvs.create false --local

COPY poetry.lock pyproject.toml LICENSE ./

RUN poetry install --no-root --with dev

COPY . ./

# Install the project itself (editable, no deps) so importlib.metadata can resolve
# `__version__` at runtime. version.py reads the version via package metadata; without
# this step the dist-info wouldn't exist and import would raise PackageNotFoundError.
RUN pip install --no-deps -e .
