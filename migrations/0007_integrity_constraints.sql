CREATE TRIGGER IF NOT EXISTS trg_citations_validate_insert
BEFORE INSERT ON citations
FOR EACH ROW
BEGIN
  SELECT
    CASE
      WHEN NEW.status NOT IN ('todo','in_progress','needs_verification','submitted','live','rejected')
      THEN RAISE(ABORT, 'invalid citations.status')
    END;

  SELECT
    CASE
      WHEN NEW.listing_url IS NOT NULL AND NEW.listing_url <> '' AND NEW.listing_url NOT LIKE 'http%'
      THEN RAISE(ABORT, 'invalid citations.listing_url')
    END;

  SELECT
    CASE
      WHEN NEW.follow_up_at IS NOT NULL AND NEW.follow_up_at <> '' AND NEW.follow_up_at NOT GLOB '????-??-??'
      THEN RAISE(ABORT, 'invalid citations.follow_up_at')
    END;
END;

CREATE TRIGGER IF NOT EXISTS trg_citations_validate_update
BEFORE UPDATE ON citations
FOR EACH ROW
BEGIN
  SELECT
    CASE
      WHEN NEW.status NOT IN ('todo','in_progress','needs_verification','submitted','live','rejected')
      THEN RAISE(ABORT, 'invalid citations.status')
    END;

  SELECT
    CASE
      WHEN NEW.listing_url IS NOT NULL AND NEW.listing_url <> '' AND NEW.listing_url NOT LIKE 'http%'
      THEN RAISE(ABORT, 'invalid citations.listing_url')
    END;

  SELECT
    CASE
      WHEN NEW.follow_up_at IS NOT NULL AND NEW.follow_up_at <> '' AND NEW.follow_up_at NOT GLOB '????-??-??'
      THEN RAISE(ABORT, 'invalid citations.follow_up_at')
    END;
END;
