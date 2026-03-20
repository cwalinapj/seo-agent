CREATE TABLE IF NOT EXISTS sites (
  id TEXT PRIMARY KEY,          -- site_... you generate
  url TEXT NOT NULL,
  domain TEXT NOT NULL,         -- hostname without www
  business_name TEXT,
  primary_city TEXT,
  primary_state TEXT,
  email TEXT,

  baseline_start_date TEXT,     -- YYYY-MM-DD (optional)
  baseline_end_date TEXT,       -- YYYY-MM-DD (optional)

  is_active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sites_domain ON sites(domain);
CREATE INDEX IF NOT EXISTS idx_sites_active ON sites(is_active);
