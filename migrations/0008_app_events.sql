CREATE TABLE IF NOT EXISTS app_events (
  id TEXT PRIMARY KEY,
  level TEXT NOT NULL,
  event_type TEXT NOT NULL,
  message TEXT,
  path TEXT,
  details_json TEXT,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_app_events_time ON app_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_app_events_level_time ON app_events(level, created_at DESC);
