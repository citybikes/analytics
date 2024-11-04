FROM python:3.12

# Get sqlite 3.47
ENV LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}
RUN mkdir -p /tmp/build/sqlite && \
    curl -L https://www.sqlite.org/2024/sqlite-autoconf-3470000.tar.gz | \
        tar xz -C /tmp/build/sqlite --strip-components=1 && \
    cd /tmp/build/sqlite && \
    ./configure && \
    make && \
    make install && \
    python -c "import sqlite3; assert sqlite3.sqlite_version == '3.47.0'" && \
    cd / && rm -rf /tmp/build

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY collect.py .
