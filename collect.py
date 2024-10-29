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
        entity_id TEXT,
        network_tag TEXT,
        stat BLOB,
        latitude REAL,
        longitude REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_entity_tag_timestamp ON stats (
        entity_id, network_tag, timestamp DESC
    );

    PRAGMA journal_mode = WAL;
    PRAGMA cache_size = 1000000000;
    PRAGMA foreign_keys = true;
    PRAGMA busy_timeout = 5000;

""")
conn.commit()


log = logging.getLogger("collector")


class StatCollector(ZMQConsumer):
    def handle_message(self, topic, message):
        network = json.loads(message)
        meta = network["meta"]
        log.info("Processing %s", meta)

        cursor = conn.cursor()

        data_iter = (
            (
                s["id"],
                network["tag"],
                json.dumps(
                    {
                        "bikes": s["bikes"],
                        "free": s["free"],
                        "timestamp": s["timestamp"],
                        "extra": s["extra"],
                    }
                ),
                s["latitude"],
                s["longitude"],
                s["id"],
                network["tag"],
                s["bikes"],
                s["free"],
            ) for s in network['stations']
        )

        cursor.executemany(
            """
            INSERT INTO stats (entity_id, network_tag, stat, latitude, longitude)
            SELECT ?, ?, json(?), ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM (
                    SELECT stat FROM stats
                    WHERE entity_id = ?
                      AND network_tag = ?
                    ORDER BY TIMESTAMP DESC
                    LIMIT 1
                ) as ls
                WHERE json(ls.stat)->>'bikes' = ? AND
                      json(ls.stat)->>'free' = ?
            )

            """,
            data_iter,
        )
        conn.commit()
        log.info(
            "[%s] Finished processing %d stations"
            % (network["tag"], len(network["stations"]))
        )


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
