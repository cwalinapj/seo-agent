# Browser Checker (D&B)

This service runs Playwright for sources that need real browser interactions.

## Start

1. Install runtime dependency:
   - `npm run browser-checker:install`
2. Install browser binary once:
   - `npx playwright install chromium`
3. Run service:
   - `BROWSER_CHECKER_TOKEN=your-token npm run browser-checker:run`

Default URL: `http://localhost:8788`

## Endpoint

- `POST /check/dnb`
  - body:
    - `business_name` (required)
    - `domain` (optional but recommended)

Sample response:

```json
{
  "result": "listed",
  "confidence": "high",
  "matched_links": ["https://www.dnb.com/..."],
  "detail": "D&B browser check found matching listing text."
}
```

## Worker config

Set these Worker vars:

- `BROWSER_CHECKER_URL` (example: `https://checker.yourdomain.com`)
- `BROWSER_CHECKER_TOKEN` (same token configured on this service)

Then deploy Worker:

- `npm run deploy`
