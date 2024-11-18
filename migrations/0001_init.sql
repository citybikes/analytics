PRAGMA user_version=1;

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
