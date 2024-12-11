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
$ sqlite3 stats.db << EOF
    SELECT count(*) as activity, network_tag
    FROM stats
    GROUP BY entity_id
    ORDER BY activity DESC
    LIMIT 25
EOF
```

### Get most active stations overall

```console
$ sqlite3 stats.db << EOF
    SELECT count(*) as activity, entity_id, latitude, longitude, network_tag
    FROM stats
    GROUP BY entity_id
    ORDER BY activity DESC
    LIMIT 25
EOF
```

### Get most active stations on a particular system

```console
$ sqlite3 stats.db << EOF
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
