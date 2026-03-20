-- Master list of citation sources
CREATE TABLE IF NOT EXISTS citation_sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL,          -- primary | maps | social | directory | aggregator
  claim_url TEXT,
  notes TEXT,
  created_at INTEGER NOT NULL
);

-- Per-client/site progress tracking
CREATE TABLE IF NOT EXISTS citations (
  id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  status TEXT NOT NULL,            -- todo|in_progress|needs_verification|submitted|live|rejected
  listing_url TEXT,
  login_email TEXT,
  last_step TEXT,
  evidence_json TEXT,
  updated_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  UNIQUE(site_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_citations_site ON citations(site_id);
CREATE INDEX IF NOT EXISTS idx_citations_source ON citations(source_id);
