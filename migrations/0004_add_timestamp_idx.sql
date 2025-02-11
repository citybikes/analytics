PRAGMA user_version=4;

-- index timestamp in stats
CREATE INDEX IF NOT EXISTS idx_timestamp ON stats (timestamp);
