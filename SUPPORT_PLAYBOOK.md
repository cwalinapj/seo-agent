# Support Playbook

## Common Incidents

### 1) Bad bulk update
1. Export affected site from dashboard (`Export Site JSON`).
2. Identify incorrect `source_id` rows and desired rollback values.
3. Re-apply correct status/follow-up values from dashboard or API.
4. Confirm changes in `/api/sites/<site_id>/audit`.

### 2) Missing evidence file
1. Check citation `evidence_json` and `r2_key`.
2. Verify object exists in bound R2 bucket.
3. If expired by retention policy, re-upload evidence and add note.

### 3) Unauthorized/forbidden API responses
1. Confirm `ADMIN_API_TOKEN` in Worker vars.
2. Ensure dashboard URL includes `?api_key=...` when auth is enabled.
3. Check `ALLOWED_SITE_IDS` for site access restrictions.
4. Check `CORS_ALLOWED_ORIGINS` if browser-origin requests are blocked.

## Recovery Validation
1. `GET /api/health`
2. Load dashboard for affected site.
3. Save one citation update and verify it appears in source progress.
4. Verify audit log contains the action.

## Escalation
- Capture the exported JSON snapshot.
- Capture error payload and timestamp.
- Include impacted `site_id`, `source_id`, and endpoint path in incident report.
