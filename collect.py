import os
import sys
import signal
import json
import logging
import argparse
import sqlite3

from hyper.consumer import ZMQConsumer


DB_URI = os.getenv("DB_URI", "citybikes.db")
ZMQ_ADDR = os.getenv("ZMQ_ADDR", "tcp://127.0.0.1:5555")

conn = sqlite3.connect(DB_URI)

cur = conn.cursor()
cur.executescript("""
    CREATE TABLE IF NOT EXISTS stats (
        id INTEGER PRIMARY KEY,
        network_tag TEXT,
        station BLOB,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        -- critical fields candidate for STORED (but VIRTUAL atm)
        entity_id TEXT GENERATED ALWAYS AS (json_extract(station, '$.id')) VIRTUAL,
        bikes INT GENERATED ALWAYS AS (json_extract(station, '$.bikes')) VIRTUAL,
        free INT GENERATED ALWAYS AS (json_extract(station, '$.free')) VIRTUAL,
        -- query fields
        name TEXT GENERATED ALWAYS AS (json_extract(station, '$.name')) VIRTUAL,
        latitude FLOAT GENERATED ALWAYS AS (json_extract(station, '$.latitude')) VIRTUAL,
        longitude FLOAT GENERATED ALWAYS AS (json_extract(station, '$.longitude')) VIRTUAL
    );

    CREATE INDEX IF NOT EXISTS idx_entity_tag_timestamp ON stats (
        entity_id, network_tag, timestamp DESC
    );

    CREATE INDEX IF NOT EXISTS idx_bikes_free ON stats (bikes, free);

    -- this is an expensive query that can be used to get the last status
    -- for every station. Useful for warming up a cache
    CREATE VIEW IF NOT EXISTS last_stat AS
      SELECT entity_id, network_tag, bikes, free, json(station) FROM (
        SELECT *, ROW_NUMBER() OVER (
                PARTITION BY entity_id, network_tag
                ORDER BY timestamp DESC
             ) AS row_n
        FROM stats
      )
      WHERE row_n = 1
    ;

    PRAGMA journal_mode = WAL;
    PRAGMA cache_size = 1000000000;
    PRAGMA foreign_keys = true;
    PRAGMA busy_timeout = 5000;

""")
conn.commit()


log = logging.getLogger("collector")

# log.info("Warming up stat dedupe cache...")
#
# cur = conn.execute(""" SELECT * FROM last_stat """)

last_stat = {}

# while (data:=cur.fetchmany(1000)):
#     for uid, tag, bikes, free, _ in data:
#         key = f"{uid}-{tag}"
#         last_stat[key] = (bikes, free)

def cache_filter(tag, uid, station):
    key = f"{uid}-{tag}"
    val = (station['bikes'], station['free'])

    last = last_stat.setdefault(key, (None, None))

    last_stat[key] = val

    return last == val


class StatCollector(ZMQConsumer):
    def handle_message(self, topic, message):
        network = json.loads(message)
        meta = network["meta"]
        log.info("Processing %s", meta)

        cursor = conn.cursor()

        tag = network['tag']

        stations = filter(
            lambda s: not cache_filter(tag, s['id'], s),
            network['stations']
        )

        # unroll for now - useful for debugging
        stations = list(stations)

        log.info("[%s] %d from %d stations changed", tag, len(stations), len(network['stations']))

        data_iter = (
            (
                tag,
                json.dumps(s),
            ) for s in stations
        )

        cursor.executemany(
            """
            INSERT INTO stats (network_tag, station)
            VALUES (?, jsonb(?))
            """,
            data_iter,
        )
        conn.commit()
        log.info("[%s] Finished processing %d stations", tag, len(stations))


def shutdown(* args, ** kwargs):
    conn.close()
    sys.exit(0)


if __name__ == "__main__":
    ZMQ_ADDR = os.getenv("ZMQ_ADDR", "tcp://127.0.0.1:5555")
    ZMQ_TOPIC = os.getenv("ZMQ_TOPIC", "")
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--addr", default=ZMQ_ADDR)
    parser.add_argument("-t", "--topic", default=ZMQ_TOPIC)
    args, _ = parser.parse_known_args()
    collector = StatCollector(args.addr, args.topic)
    signal.signal(signal.SIGINT, shutdown)
    collector.reader()
