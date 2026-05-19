FROM python:3.14-slim

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install poetry==2.4.1 && \
    poetry config virtualenvs.create false --local

COPY poetry.lock pyproject.toml LICENSE ./

RUN poetry install --no-root --with dev

COPY . ./
