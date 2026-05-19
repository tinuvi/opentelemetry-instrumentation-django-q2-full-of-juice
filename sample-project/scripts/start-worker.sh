#!/usr/bin/env bash
set -e

# Web service runs `migrate` and the worker waits on its health probe before
# starting (see depends_on in docker-compose.yml), so the schema is already in place.
exec python manage.py qcluster
