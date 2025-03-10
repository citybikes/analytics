FROM debian as build

RUN apt-get update && apt-get install -y build-essential curl

# Get sqlite 3.49
RUN mkdir -p /tmp/build/sqlite && \
    curl -L https://www.sqlite.org/2025/sqlite-autoconf-3490100.tar.gz | \
        tar xz -C /tmp/build/sqlite --strip-components=1 && \
    cd /tmp/build/sqlite && \
    ./configure && \
    make && \
    make DESTDIR=/build/sqlite install

FROM python:3-slim as python-build

RUN apt-get update && apt-get install -y --no-install-recommends \
  git \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m venv /venv

RUN /venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3-slim

COPY --from=build /build/sqlite /
ENV LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}

COPY --from=python-build /venv /venv
ENV PATH=/venv/bin:$PATH

# assert sqlite version
RUN python -c "import sqlite3; assert sqlite3.sqlite_version == '3.49.1'"

WORKDIR /usr/src/app

COPY migrations ./migrations
COPY collect.py .
