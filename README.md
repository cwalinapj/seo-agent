# seo-agent

[![CI](https://github.com/cwalinapj/seo-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/cwalinapj/seo-agent/actions/workflows/ci.yml)

Cloudflare Worker + D1 + R2 citation operations dashboard.

## Status
- `main` is protected by CI (`.github/workflows/ci.yml`).
- Every production slice in this repo is shipped as a commit to `main` after local CI pass.

## Local Validation
```bash
npm ci
npm run ci
```

## Deploy
Use explicit environment targets:
```bash
npm run db:migrate:staging
npm run deploy:staging
npm run db:migrate:prod
npm run deploy:prod
```

## Operations Endpoints
- `GET /api/ops/metrics`
- `GET /api/sites/:site_id/audit?limit=100&offset=0`
- `GET /api/sites/:site_id/export`
