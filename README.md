# Citybikes analytics playground

This repo is for testing some ideas on storing historical information and doing
analytics on citybikes data.

## Install

```console
$ pip install -r requirements.txt
```

## Usage

First, let's fire up a producer for bike share information, by using citybikes
[hyper publisher].

```console
$ hyper publisher
```

[hyper publisher]: https://github.com/citybikes/hyper

The producer will open a tcp socket at port 5555, so we can connect our data
collector to

```console
$ python collect.py
```

This will create a `citybikes.db` sqlite file. Run aggregated queries like

### Get most active systems

```console
$ sqlite3 citybikes.db << EOF
    SELECT count(*) as activity, network_tag
    FROM stats
    GROUP BY entity_id
    ORDER BY activity DESC
    LIMIT 25
EOF
```

### Get most active stations overall

```console
$ sqlite3 citybikes.db << EOF
    SELECT count(*) as activity, entity_id, latitude, longitude, network_tag
    FROM stats
    GROUP BY entity_id
    ORDER BY activity DESC
    LIMIT 25
EOF
```

### Get most active stations on a particular system

```console
$ sqlite3 citybikes.db << EOF
    SELECT count(*) as activity, entity_id, latitude, longitude, network_tag
    FROM stats
    WHERE network_tag = 'bicing'
    GROUP BY entity_id
    ORDER BY activity DESC
    LIMIT 25
EOF
```

### Export data

Exporting data requires duckdb installed on the system.

```console
$ bash export.sh quack stats.db stats.duck
$ bash export.sh parquet stats.duck --from 2024-11-01 --to 2024-11-15 --network bicing > bicing.parquet
$ bash export.sh csv stats.duck --from 2024-11-01 --to 2024-11-15 --network bicing > bicing.csv
$ bash export.sh parquet stats.duck -o world.parquet
```

### Visualizing

Use the plot script to visualize information. This script requires duckdb and
a python environment with matplotlib and pandas.

```console
$ pip install matplotlib pandas
```

```console
duckdb -s "COPY(\
    select tag, nuid, name, bikes, extra.ebikes, bikes::int-extra.ebikes::int as normal, free, timestamp \
    from read_parquet('cb.parquet') \
    where tag='bicing' and nuid='100' \
) TO '/dev/stdout' WITH (FORMAT 'csv', HEADER)" | python plot.py -s 5min - -p Blues
```
![plot](https://github.com/user-attachments/assets/afc5cea3-279f-4c2c-957e-5df320dd1cba)


```console
duckdb -s "COPY(\
    select tag, nuid, name, bikes, extra.ebikes, bikes::int-extra.ebikes::int as normal, free, timestamp \
    from read_parquet('cb.parquet') \
    where tag='citi-bike-nyc' and nuid='66ddbd20-0aca-11e7-82f6-3863bb44ef7c' \
) TO '/dev/stdout' WITH (FORMAT 'csv', HEADER)" | python plot.py -s 5min -
```
![plot](https://github.com/user-attachments/assets/ba6269a1-c6c0-4c8a-bc60-e1d09d505f6f)


```console
duckdb -s "COPY(\
    select tag, nuid, name, bikes, extra.ebikes, bikes::int-extra.ebikes::int as normal, free, timestamp \
    from read_parquet('cb.parquet') \
    where tag='bicing' \
) TO '/dev/stdout' WITH (FORMAT 'csv', HEADER)" | python plot.py -s 5min -
```
![plot](https://github.com/user-attachments/assets/e5502607-ce95-47a7-8252-b4fdc1a1cf79)


### Running a stats API

To start the stands endpoint, run:

```console
$ uvicorn api:app
```

#### Supported queries

Unaggregated stats, filtered by network, station and time frame

```console
$ http :8000/networks/bicing/stations/cd2e90920bcbabea6840fc65d766ac43/stats from=='2025-01-01' to=='2025-02-01'
{
  "id": "cd2e90920bcbabea6840fc65d766ac43",
  "latitude": 41.4074444,
  "longitude": 2.1492066,
  "name": "AV. VALLCARCA, 11",
  "stats": [
    {
      "bikes": 2,
      "free": 12,
      "timestamp": "2025-01-01 00:00:28Z",
      "extra": {
        "uid": 502,
        "online": true,
        "normal_bikes": 0,
        "has_ebikes": true,
        "ebikes": 2
      }
    },
    {
      "bikes": 3,
      "free": 11,
      "timestamp": "2025-01-01 00:06:28Z",
      "extra": {
        "uid": 502,
        "online": true,
        "normal_bikes": 0,
        "has_ebikes": true,
        "ebikes": 3
      }
    },
    {
      "bikes": 4,
      "free": 10,
      "timestamp": "2025-01-01 00:09:28Z",
      "extra": {
        "uid": 502,
        "online": true,
        "normal_bikes": 0,
        "has_ebikes": true,
        "ebikes": 4
      }
    },
    ...
}

```

Aggregated hourly stats filtered by network, station and time frame

```console
$ http :8000/networks/bicing/stations/cd2e90920bcbabea6840fc65d766ac43/stats/hourly from=='2025-01-01' to=='2025-02-01
{
  "id": "cd2e90920bcbabea6840fc65d766ac43",
  "latitude": 41.4074444,
  "longitude": 2.1492066,
  "stats": [
    {
      "timestamp": "2025-01-01 00:00:00Z",
      "bikes": {
        "avg": 3.0,
        "max": 5,
        "min": 1
      },
      "free": {
        "avg": 11.0,
        "max": 13,
        "min": 9
      }
    },
    {
      "timestamp": "2025-01-01 01:00:00Z",
      "bikes": {
        "avg": 2.4,
        "max": 4,
        "min": 1
      },
      "free": {
        "avg": 11.4,
        "max": 13,
        "min": 10
      }
    },
    {
      "timestamp": "2025-01-01 02:00:00Z",
      "bikes": {
        "avg": 2.8333333333333335,
        "max": 4,
        "min": 1
      },
      "free": {
        "avg": 10.166666666666666,
        "max": 12,
        "min": 8
      }
    },
    {
      "timestamp": "2025-01-01 03:00:00Z",
      "bikes": {
        "avg": 3.7,
        "max": 6,
        "min": 2
      },
      "free": {
        "avg": 8.6,
        "max": 11,
        "min": 7
      }
    },
    ...
}

```
