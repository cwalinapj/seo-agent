CREATE TABLE IF NOT EXISTS audit_logs (
  id TEXT PRIMARY KEY,
  site_id TEXT,
  action TEXT NOT NULL,
  actor TEXT,
  request_path TEXT,
  payload_json TEXT,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_site_time ON audit_logs(site_id, created_at DESC);
