# Deployment Runbook

## Environments
- `staging` uses `seo_agent_staging` + `seo-agent-evidence-staging`
- `production` uses `seo_agent_prod` + `seo-agent-evidence-prod`

Update `database_id` values in `wrangler.toml` before first deploy.

## Release Checklist
1. `npm run ci`
2. `npm run db:migrate:staging`
3. `npm run deploy:staging`
4. Verify staging:
   - `GET /api/health`
   - dashboard load
   - create/update citation
   - evidence upload/download
5. `npm run db:migrate:prod`
6. `npm run deploy:prod`
7. Verify production with the same checks.

## Rollback Plan
1. Re-deploy previous Worker version from Cloudflare dashboard.
2. If a migration introduced app breakage:
   - hotfix Worker code to be backward-compatible with the latest schema.
   - avoid destructive rollback migrations on live data.
3. Disable write endpoints by clearing `ADMIN_API_TOKEN` only if emergency freeze is required.
4. Post-incident:
   - capture timeline
   - add regression test in `tests/`
   - add migration compatibility guard in Worker code.

## Notes
- Always apply DB migrations before deploying a Worker that depends on new columns/tables.
- Keep staging and production secrets isolated.
