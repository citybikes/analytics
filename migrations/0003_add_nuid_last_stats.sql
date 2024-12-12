PRAGMA user_version=3;

-- add virtual column nuid for extra.uid

ALTER TABLE last_stats
    ADD COLUMN nuid TEXT GENERATED ALWAYS AS (
        json_extract(station, '$.extra.uid')
    ) VIRTUAL
;
