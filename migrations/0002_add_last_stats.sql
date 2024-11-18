PRAGMA user_version=2;

-- "materialized view" of last stat for entity. Used for deduping stats

CREATE TABLE IF NOT EXISTS last_stats (
    network_tag TEXT,
    entity_id TEXT,
    station BLOB,
    bikes INT GENERATED ALWAYS AS (json_extract(station, '$.bikes')) STORED,
    free INT GENERATED ALWAYS AS (json_extract(station, '$.free')) STORED,
    PRIMARY KEY (network_tag, entity_id)
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS insert_stat AFTER INSERT ON stats
BEGIN
    INSERT INTO last_stats (network_tag, entity_id, station)
                VALUES (NEW.network_tag, NEW.entity_id, NEW.station)
    ON CONFLICT(network_tag, entity_id) DO UPDATE SET
        entity_id=excluded.entity_id,
        station=excluded.station
    ;
END;
