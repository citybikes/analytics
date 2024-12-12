import os
import sys
import signal
import json
import logging
import argparse
import sqlite3
from importlib import resources

from hyper.subscriber import ZMQSubscriber


DB_URI = os.getenv("DB_URI", "citybikes.db")
ZMQ_ADDR = os.getenv("ZMQ_ADDR", "tcp://127.0.0.1:5555")

conn = sqlite3.connect(DB_URI)
cur = conn.cursor()
cur.executescript("""
    PRAGMA journal_mode = WAL;
    -- default: 2000 (page) - 1 page: 1.5k
    PRAGMA cache_size = 20000;
    PRAGMA foreign_keys = true;
    PRAGMA busy_timeout = 5000;
""")
conn.commit()

log = logging.getLogger("collector")

# XXX move into a function/module
current_version,  = next(
    conn.cursor().execute('PRAGMA user_version'),
    (None, )
)

migrations = [
    f for f in resources.files('migrations').iterdir()
]

for migration in migrations[current_version:]:
    cur = conn.cursor()
    try:
        log.info("Applying %s", migration.name)
        cur.executescript("begin;" + migration.read_text())
    except Exception as e:
        log.error("Failed migration %s: %s. Bye", migration.name, e)
        cur.execute("rollback")
        sys.exit(1)
    else:
        cur.execute("commit")
# XXX end

log.info("Warming up stat dedupe cache...")

cur = conn.execute("""
    SELECT entity_id, nuid, network_tag, bikes, free FROM last_stats
""")


last_stat = {}

while (data:=cur.fetchmany(1000)):
    for uid, nuid, tag, bikes, free in data:
        key = '-'.join(map(str, filter(None, [uid, nuid, tag])))
        last_stat[key] = (bikes, free)


def cache_filter(tag, station):
    uid = station['id']
    nuid = station['extra'].get('uid')
    key = '-'.join(map(str, filter(None, [uid, nuid, tag])))
    val = (station['bikes'], station['free'])

    last = last_stat.setdefault(key, (None, None))

    last_stat[key] = val

    return last == val


class StatCollector(ZMQSubscriber):
    def handle_message(self, topic, message):
        network = json.loads(message)
        meta = network["meta"]
        log.info("Processing %s", meta)

        cursor = conn.cursor()

        tag = network['tag']

        stations = filter(lambda s: not cache_filter(tag, s), network['stations'])

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
