"""
This module is a proof of concept for a stats API that supports:
    * query stats by station id
    * aggregate stats hourly by station id
"""

import contextlib
import json
import os
import sqlite3

import aiosqlite

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.routing import Route

DB_URI = os.getenv("DB_URI", "citybikes.db")


def named_params(handler):
    async def _handler(request):
        args = request.path_params
        r = await handler(request, **args)
        return r

    return _handler


def find_station_q(tag, uid):
    return (
        """
        SELECT json(station) AS station FROM last_stats
        WHERE network_tag = ?
          AND entity_id = ?
    """,
        (
            tag,
            uid,
        ),
    )


async def station_stats(request, uid=None, suid=None):
    if not uid or not suid:
        raise HTTPException(status_code=400)

    p_from = request.query_params.get("from")
    p_to = request.query_params.get("to")

    if not p_from or not p_to:
        raise HTTPException(status_code=400)

    db = request.app.db

    # Find station
    cur = await db.execute(*find_station_q(uid, suid))
    station = await cur.fetchone()

    if not station:
        raise HTTPException(status_code=404)

    station = json.loads(station["station"])

    cur = await db.execute(
        """
    WITH st_window AS (
        SELECT network_tag,
               entity_id,
               json(station) AS station,
               bikes,
               free,
               lag(bikes, 1, NULL) OVER st AS prev_bikes,
               lag(free,  1, NULL) OVER st AS prev_free,
               timestamp
        FROM stats
        WHERE timestamp >= ? AND timestamp < ?
          AND network_tag = ?
          AND entity_id = ?
        WINDOW st AS(
            PARTITION BY network_tag, entity_id
            ORDER BY timestamp ASC
        )
        ORDER BY network_tag, entity_id,
                 timestamp ASC
    ),
    deduped AS (
      SELECT * FROM st_window
      WHERE
          bikes <> prev_bikes OR free <> prev_free OR
          (prev_bikes IS NULL and prev_free IS NULL)
    )

    SELECT json_extract(station, '$.extra') as extra,
           bikes,
           free,
           timestamp
    FROM deduped
    """,
        (
            p_from,
            p_to,
            uid,
            suid,
        ),
    )

    # XXX paginate response
    data = await cur.fetchall()

    # XXX Use some kind of model
    data = map(
        lambda r: {
            "bikes": r["bikes"],
            "free": r["free"],
            "timestamp": r["timestamp"] + "Z",
            "extra": json.loads(r["extra"]),
        },
        data,
    )

    response = {
        "id": station["id"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "name": station["name"],
        "stats": list(data),
    }
    return JSONResponse(response)


async def station_stats_agg(request, uid=None, suid=None, agg=None):
    # XXX support hourly / daily / 30min agg
    if agg not in ["hourly"]:
        raise HTTPException(status_code=400)

    if not uid or not suid:
        raise HTTPException(status_code=400)

    p_from = request.query_params.get("from")
    p_to = request.query_params.get("to")

    if not p_from or not p_to:
        raise HTTPException(status_code=400)

    db = request.app.db

    # Find station
    cur = await db.execute(*find_station_q(uid, suid))
    station = await cur.fetchone()

    if not station:
        raise HTTPException(status_code=404)

    station = json.loads(station["station"])

    query = """
        WITH resampled as (
            SELECT
                entity_id,
                strftime('%Y-%m-%d %H:00:00', timestamp) AS timestamp,
                LAST_VALUE(bikes) OVER h_win AS bikes,
                LAST_VALUE(free) OVER h_win AS free
            FROM stats
            WHERE timestamp >= ? AND timestamp < ?
              AND network_tag = ?
              AND entity_id = ?

            WINDOW
            h_win AS (
                PARTITION BY entity_id, strftime('%Y-%m-%d %H:00:00', timestamp)
                ORDER BY timestamp
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        )

        SELECT timestamp,
               AVG(bikes) as bikes_avg,
               MAX(bikes) as bikes_max,
               MIN(bikes) as bikes_min,
               AVG(free) as free_avg,
               MAX(free) as free_max,
               MIN(free) as free_min
        FROM resampled
        GROUP BY timestamp
    """

    cur = await db.execute(
        query,
        (
            p_from,
            p_to,
            uid,
            suid,
        ),
    )

    # XXX paginate response
    data = await cur.fetchall()

    # XXX use some kind of model
    # break bikes and free stats into ns
    data = map(
        lambda d: {
            "timestamp": d["timestamp"] + "Z",
            "bikes": {
                "avg": d["bikes_avg"],
                "max": d["bikes_max"],
                "min": d["bikes_min"],
            },
            "free": {
                "avg": d["free_avg"],
                "max": d["free_max"],
                "min": d["free_min"],
            },
        },
        data,
    )
    response = {
        "id": station["id"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "stats": list(data),
    }
    return JSONResponse(response)


@contextlib.asynccontextmanager
async def lifespan(app):
    async with aiosqlite.connect(DB_URI) as db:
        # XXX Check perf penalty on this
        db.row_factory = lambda *a: dict(sqlite3.Row(*a))
        app.db = db
        yield


routes = [
    Route("/networks/{uid}/stations/{suid}/stats", named_params(station_stats)),
    Route(
        "/networks/{uid}/stations/{suid}/stats/{agg}", named_params(station_stats_agg)
    ),
]

app = Starlette(
    routes=routes,
    lifespan=lifespan,
)
