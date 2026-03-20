-- Remove TomTom from the citation catalog and any per-site tracking rows.
DELETE FROM citations WHERE source_id = 'src_tomtom';
DELETE FROM citation_sources WHERE id = 'src_tomtom';
