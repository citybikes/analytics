import os
import sys
import signal
import json
import time
import logging
import argparse
import sqlite3
import threading
from importlib import resources

from citybikes.hyper.subscriber import ZMQSubscriber


log = logging.getLogger("collector")

DB_URI = os.getenv("DB_URI", "citybikes.db")
ZMQ_ADDR = os.getenv("ZMQ_ADDR", "tcp://127.0.0.1:5555")
ZMQ_TOPIC = os.getenv("ZMQ_TOPIC", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DB_RETENTION = os.getenv("DB_RETENTION", None)
DB_GC_INTERVAL = int(os.getenv("DB_GC_INTERVAL", 60))
DB_GC_BATCH_SIZE = int(os.getenv("DB_GC_BATCH_SIZE", 1000))
DB_GC_BATCH_INTERVAL = int(os.getenv("DB_GC_BATCH_INTERVAL", 0))


class StatCollector(ZMQSubscriber):
    def __init__(self, conn, *args, **kwargs):
        super(StatCollector, self).__init__(*args, **kwargs)
        self.conn = conn
        self.cache_warmup()

    def cache_warmup(self):
        self.last_stat = {}
        log.info("Warming up stat dedupe cache...")

        cur = self.conn.execute("""
            SELECT entity_id, nuid, network_tag, bikes, free FROM last_stats
        """)

        while data := cur.fetchmany(1000):
            for uid, nuid, tag, bikes, free in data:
                key = "-".join(map(str, filter(None, [uid, nuid, tag])))
                self.last_stat[key] = (bikes, free)

    def cache_filter(self, tag, station):
        uid = station["id"]
        nuid = station["extra"].get("uid")
        key = "-".join(map(str, filter(None, [uid, nuid, tag])))
        val = (station["bikes"], station["free"])

        last = self.last_stat.setdefault(key, (None, None))

        self.last_stat[key] = val

        return last == val

    def handle_message(self, topic, message):
        network = json.loads(message)
        meta = network["meta"]
        log.info("Processing %s", meta)

        cursor = self.conn.cursor()

        tag = network["tag"]

        stations = filter(lambda s: not self.cache_filter(
            tag, s), network["stations"])

        # unroll for now - useful for debugging
        stations = list(stations)

        log.info(
            "[%s] %d from %d stations changed",
            tag,
            len(stations),
            len(network["stations"]),
        )

        data_iter = (
            (
                tag,
                json.dumps(s),
            )
            for s in stations
        )

        cursor.executemany(
            """
            INSERT INTO stats (network_tag, station)
            VALUES (?, jsonb(?))
            """,
            data_iter,
        )
        self.conn.commit()
        log.info("[%s] Finished processing %d stations", tag, len(stations))


class GarbageCollector(threading.Thread):
    def __init__(self, interval, db_uri, retention="-1 month", batch_size=1000):
        super(GarbageCollector, self).__init__(daemon=True)
        self.interval = interval
        # huh
        self.conn = sqlite3.connect(db_uri, check_same_thread=False)
        self.retention = retention
        self.batch_size = batch_size
        self.stop_ev = threading.Event()

    def run(self):
        while not self.stop_ev.is_set():
            # XXX do proper batches and enqueue these as tasks
            deleted = 0
            while not self.stop_ev.is_set():
                (n_rows,) = self.conn.execute(
                    """
                    SELECT count(*) FROM stats WHERE timestamp < datetime('now', ?)
                """,
                    (self.retention,),
                ).fetchone()

                # Only show log info on first batch run
                if not deleted:
                    log.info(
                        "GC | Found %d rows older than '%s'", n_rows, self.retention
                    )

                # all good?
                if n_rows == 0:
                    break

                self.conn.execute(
                    """
                    DELETE FROM stats WHERE id IN (
                        SELECT id FROM stats WHERE timestamp <= datetime('now', ?)
                        LIMIT ?
                    )
                """,
                    (
                        self.retention,
                        self.batch_size,
                    ),
                )
                deleted += self.batch_size
                log.info(
                    "GC | deleted - %.2f %% - remaining %d",
                    deleted * 100 / n_rows,
                    n_rows,
                )
                self.conn.commit()
                self.stop_ev.wait(DB_GC_BATCH_INTERVAL)
                do_log = False
            self.stop_ev.wait(self.interval)
        self.conn.close()

    def stop(self):
        self.stop_ev.set()


def migrate(conn):
    # XXX move into a function/module
    (current_version,) = next(conn.cursor().execute("PRAGMA user_version"), (None,))

    migrations = [f for f in resources.files("migrations").iterdir()]

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


def main(args):
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

    # XXX
    migrate(conn)
    if args.migrate:
        return

    collector = StatCollector(conn, args.addr, args.topic)
    if DB_RETENTION:
        gc = GarbageCollector(
            interval=DB_GC_INTERVAL,
            db_uri=DB_URI,
            retention=DB_RETENTION,
            batch_size=DB_GC_BATCH_SIZE,
        )

    # XXX blergh
    def shutdown(*args, **kwargs):
        if DB_RETENTION:
            log.info("GC | Shutting down")
            gc.stop()
            gc.join()

        log.info("Closing DB conn")
        conn.close()
        sys.exit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, shutdown)

    if DB_RETENTION:
        gc.start()
    collector.reader()


if __name__ == "__main__":
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(stream=sys.stderr)],
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--addr", default=ZMQ_ADDR)
    parser.add_argument("-t", "--topic", default=ZMQ_TOPIC)
    parser.add_argument("--migrate", default=False, action="store_true")
    args, _ = parser.parse_known_args()
    main(args)
