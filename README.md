# Citybikes analytics playground

This repo is for testing some ideas on storing historical information and doing
analytics on citybikes data.

## Install

```console
$ pip install -r requirements.txt
```

## Usage

First, let's fire up a producer for bike share information, by using citybikes
[hyper producer].

```console
$ python -m hyper.producer
```

[hyper producer]: https://github.com/citybikes/hyper

The producer will open a tcp socket at port 5555, so we can connect our data
collector to

```console
$ python collect.py
```

This will create a `citybikes.db` sqlite file. Run aggregated queries like

### Get most active systems

```console
$ sqlite3 stats.db << EOF
    SELECT count(*) as activity, network_tag, n.name, json(n.meta)->'city', json(n.meta)->'country'
    FROM stats
    JOIN networks n ON n.tag = network_tag
    GROUP BY entity_id
    ORDER BY activity DESC
    LIMIT 25
EOF
```

### Get most active stations overall

```console
$ sqlite3 stats.db << EOF
    SELECT count(*) as activity, entity_id, s.name, latitude, longitude, network_tag, json(n.meta)->'city', json(n.meta)->'country'
    FROM stats
    JOIN stations s ON s.hash = entity_id
    JOIN networks n ON n.tag = network_tag
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
