# Note: This can (and MUST) be combined with the Dockerfile in the project's root directory,
# this is just a quick workaround to test dev Ray clusters
FROM python:3.12-slim

WORKDIR /opt/app

COPY pyproject.toml poetry.lock* ./

RUN pip install --no-cache-dir --upgrade pip && \
   pip install poetry poetry-plugin-export && \
   poetry export -f requirements.txt --without-hashes > requirements.txt && \
   sh -c 'pip install -r requirements.txt'
