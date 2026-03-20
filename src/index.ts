export interface Env {
  DB: D1Database;
  EVIDENCE_BUCKET: R2Bucket;
  ADMIN_API_TOKEN?: string;
  ALLOWED_SITE_IDS?: string;
  CORS_ALLOWED_ORIGINS?: string;
  RATE_LIMIT_PER_MINUTE?: string;
  MAX_EVIDENCE_BYTES?: string;
  EVIDENCE_RETENTION_DAYS?: string;
  OTRUST_SSO_LOGIN_URL?: string;
  OTRUST_SSO_CLIENT_ID?: string;
  BROWSER_CHECKER_URL?: string;
  BROWSER_CHECKER_TOKEN?: string;
}

type CitationRow = {
  source_id: string;
  name: string;
  category: string;
  claim_url: string | null;
  status: string | null;
  listing_url: string | null;
  last_step: string | null;
  notes: string | null;
  follow_up_at: string | null;
  evidence_json: string | null;
  updated_at: number | null;
  created_at: number | null;
};

type EvidenceItem = {
  id: string;
  kind: "link" | "file";
  url?: string;
  file_name?: string;
  content_type?: string;
  data_base64?: string;
  r2_key?: string;
  note?: string;
  created_at: number;
  expires_at?: number;
};

type SourceRow = {
  source_id: string;
  name: string;
  category: string;
  claim_url: string | null;
};

type CheckResult = {
  source_id: string;
  name: string;
  category: string;
  claim_url: string | null;
  source_host: string;
  result: "listed" | "possible" | "not_found" | "error";
  confidence: "high" | "medium" | "low";
  matched_links: string[];
  detail: string;
  provider: "http_fetch" | "headless_browser";
};

const corsBaseHeaders = {
  "access-control-allow-methods": "GET,POST,OPTIONS",
  "access-control-allow-headers": "content-type,authorization,x-api-key",
};
const rateBuckets = new Map<string, { count: number; resetAt: number }>();

const jsonBase = (data: unknown, status = 200) =>
  new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });

const textBase = (body: string, status = 200) =>
  new Response(body, {
    status,
    headers: {
      "content-type": "text/plain; charset=utf-8",
    },
  });

const optionsBase = () =>
  new Response(null, {
    status: 204,
    headers: {
      "access-control-max-age": "86400",
    },
  });

function normalizeDomain(input: string): string {
  const raw = input.trim();
  if (!raw) return "";

  const withScheme = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;

  try {
    const host = new URL(withScheme).hostname.toLowerCase();
    return host.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function normalizeListingUrl(input: string): string {
  const raw = input.trim();
  if (!raw) return "";
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "";
    return parsed.toString();
  } catch {
    return "";
  }
}

function normalizeLookupUrl(input: string): string {
  const raw = input.trim();
  if (!raw) return "";
  try {
    return new URL(raw).toString();
  } catch {
    return "";
  }
}

function normalizeLookupDomain(input: string): string {
  const raw = input.trim().toLowerCase();
  if (!raw) return "";
  const withScheme = /^https?:\/\//.test(raw) ? raw : `https://${raw}`;
  try {
    const host = new URL(withScheme).hostname.toLowerCase();
    return host.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function rootDomainFromHost(host: string): string {
  const clean = host.trim().toLowerCase().replace(/^www\./, "");
  if (!clean) return "";
  const parts = clean.split(".").filter(Boolean);
  if (parts.length <= 2) return clean;

  const compoundSuffixes = new Set([
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "com.au",
    "net.au",
    "org.au",
    "co.nz",
    "com.br",
    "com.mx",
    "co.jp",
    "co.in",
  ]);
  const lastTwo = parts.slice(-2).join(".");
  const lastThree = parts.slice(-3).join(".");
  return compoundSuffixes.has(lastTwo) ? lastThree : lastTwo;
}

function rootDomainFromUrl(input: string): string {
  const host = normalizeLookupDomain(input);
  return host ? rootDomainFromHost(host) : "";
}

function makeSiteIdFromDomain(domain: string): string {
  const base = domain.replace(/[^a-z0-9]+/gi, "_").replace(/^_+|_+$/g, "").toLowerCase();
  return `site_${base || "new"}`;
}

function makeCustomSourceIdFromDomain(domain: string): string {
  const base = domain.replace(/[^a-z0-9]+/gi, "_").replace(/^_+|_+$/g, "").toLowerCase();
  return `src_custom_${base || "site"}`;
}

function normalizeStatus(input: string): string {
  const value = input.trim().toLowerCase();
  const allowed = new Set(["todo", "in_progress", "needs_verification", "submitted", "live", "rejected"]);
  return allowed.has(value) ? value : "";
}

function normalizeNotes(input: string): string {
  return input.trim().slice(0, 4000);
}

function normalizeEmail(input: string): string {
  const value = input.trim().toLowerCase();
  if (!value) return "";
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(value)) return "";
  return value;
}

function normalizeFollowUpAt(input: string): string {
  const value = input.trim();
  if (!value) return "";
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return "";
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return "";
  return value;
}

async function citationsHasColumn(env: Env, columnName: string): Promise<boolean> {
  const result = await env.DB.prepare(`PRAGMA table_info(citations)`).all<{
    name: string;
  }>();
  return (result.results ?? []).some((column) => column.name === columnName);
}

function parseEvidenceJson(input: string | null): EvidenceItem[] {
  if (!input) return [];
  try {
    const parsed = JSON.parse(input);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function normalizeEvidenceNote(input: string): string {
  return input.trim().slice(0, 500);
}

function tokenFromRequest(request: Request): string {
  const auth = request.headers.get("authorization") || "";
  if (auth.toLowerCase().startsWith("bearer ")) return auth.slice(7).trim();
  const headerToken = request.headers.get("x-api-key");
  if (headerToken) return headerToken.trim();
  const queryToken = new URL(request.url).searchParams.get("api_key");
  return (queryToken || "").trim();
}

function auditActorFromRequest(request: Request): string {
  const token = tokenFromRequest(request);
  if (!token) return "anonymous";
  return `token:${token.slice(0, 6)}...`;
}

async function writeAuditLog(
  env: Env,
  request: Request,
  siteId: string | null,
  action: string,
  payload: Record<string, unknown>
) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const id = `audit_${now}_${Math.random().toString(36).slice(2, 10)}`;
    await env.DB.prepare(
      `INSERT INTO audit_logs (id, site_id, action, actor, request_path, payload_json, created_at)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)`
    )
      .bind(id, siteId, action, auditActorFromRequest(request), new URL(request.url).pathname, JSON.stringify(payload), now)
      .run();
  } catch {
    // Fail open until the audit_logs migration is applied in every environment.
  }
}

async function writeAppEvent(
  env: Env,
  level: "info" | "warn" | "error",
  eventType: string,
  message: string,
  path: string,
  details: Record<string, unknown>
) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const id = `evt_${now}_${Math.random().toString(36).slice(2, 10)}`;
    await env.DB.prepare(
      `INSERT INTO app_events (id, level, event_type, message, path, details_json, created_at)
       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)`
    )
      .bind(id, level, eventType, message, path, JSON.stringify(details), now)
      .run();
  } catch {
    // fail open
  }
}

function requiresAuth(env: Env): boolean {
  return Boolean((env.ADMIN_API_TOKEN || "").trim());
}

function authorizedForRequest(request: Request, env: Env): boolean {
  if (!requiresAuth(env)) return true;
  return tokenFromRequest(request) === (env.ADMIN_API_TOKEN || "").trim();
}

function allowedSiteSet(env: Env): Set<string> {
  return new Set(
    String(env.ALLOWED_SITE_IDS || "")
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean)
  );
}

function extractSiteIdFromPath(pathname: string): string {
  const m = pathname.match(/^\/api\/sites\/([^/]+)/);
  return m ? decodeURIComponent(m[1]) : "";
}

function siteIsAllowed(siteId: string, env: Env): boolean {
  if (!siteId) return true;
  const allowed = allowedSiteSet(env);
  if (allowed.size === 0) return true;
  return allowed.has(siteId);
}

function originIsAllowed(request: Request, env: Env): boolean {
  const raw = String(env.CORS_ALLOWED_ORIGINS || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  if (!raw.length) return true;
  const origin = request.headers.get("origin") || "";
  if (!origin) return true;
  return raw.includes(origin);
}

function corsOriginForRequest(request: Request, env: Env): string {
  const raw = String(env.CORS_ALLOWED_ORIGINS || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  if (!raw.length) return "*";
  const origin = request.headers.get("origin") || "";
  if (!origin) return "";
  return raw.includes(origin) ? origin : "";
}

function withCors(response: Response, request: Request, env: Env): Response {
  const headers = new Headers(response.headers);
  const allowOrigin = corsOriginForRequest(request, env);
  if (allowOrigin) headers.set("access-control-allow-origin", allowOrigin);
  headers.set("access-control-allow-methods", corsBaseHeaders["access-control-allow-methods"]);
  headers.set("access-control-allow-headers", corsBaseHeaders["access-control-allow-headers"]);
  headers.set("vary", "Origin");
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

function isWriteMethod(method: string): boolean {
  return method === "POST" || method === "PUT" || method === "PATCH" || method === "DELETE";
}

function parsePagination(url: URL, defaults = { limit: 100, max: 500 }) {
  const requestedLimit = Number(url.searchParams.get("limit") || String(defaults.limit));
  const requestedOffset = Number(url.searchParams.get("offset") || "0");
  const limit = Number.isFinite(requestedLimit)
    ? Math.max(1, Math.min(defaults.max, Math.floor(requestedLimit)))
    : defaults.limit;
  const offset = Number.isFinite(requestedOffset) ? Math.max(0, Math.floor(requestedOffset)) : 0;
  return { limit, offset };
}

function checkRateLimit(request: Request, env: Env): { allowed: boolean; retryAfter: number } {
  const limit = Number(env.RATE_LIMIT_PER_MINUTE || "120");
  const now = Date.now();
  const key = request.headers.get("cf-connecting-ip") || "unknown";
  const bucket = rateBuckets.get(key);
  if (!bucket || now >= bucket.resetAt) {
    rateBuckets.set(key, { count: 1, resetAt: now + 60_000 });
    return { allowed: true, retryAfter: 0 };
  }
  if (bucket.count >= limit) {
    return { allowed: false, retryAfter: Math.max(1, Math.ceil((bucket.resetAt - now) / 1000)) };
  }
  bucket.count += 1;
  return { allowed: true, retryAfter: 0 };
}

function makeEvidenceId(): string {
  return `ev_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function base64ToBytes(base64: string): Uint8Array {
  const bin = atob(base64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

function maxEvidenceBytes(env: Env): number {
  const parsed = Number(env.MAX_EVIDENCE_BYTES || "");
  if (!Number.isFinite(parsed) || parsed <= 0) return 2 * 1024 * 1024;
  return Math.floor(parsed);
}

function evidenceRetentionSeconds(env: Env): number {
  const days = Number(env.EVIDENCE_RETENTION_DAYS || "");
  if (!Number.isFinite(days) || days <= 0) return 0;
  return Math.floor(days * 86400);
}

function isExpiredEvidence(item: EvidenceItem, nowSeconds: number): boolean {
  return Boolean(item.expires_at && nowSeconds >= item.expires_at);
}

function safeFileName(input: string): string {
  return input.replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 120) || "evidence";
}

function makeEvidenceKey(siteId: string, sourceId: string, evidenceId: string, fileName: string): string {
  return `sites/${siteId}/${sourceId}/${evidenceId}/${safeFileName(fileName)}`;
}

function contentDispositionForEvidence(contentType: string, fileName: string): string {
  const disposition = contentType.startsWith("image/") ? "inline" : "attachment";
  return `${disposition}; filename="${safeFileName(fileName)}"`;
}

function claimHost(claimUrl: string | null): string {
  if (!claimUrl) return "";
  try {
    return new URL(claimUrl).hostname.toLowerCase().replace(/^www\./, "");
  } catch {
    return "";
  }
}

async function findOrCreateSourceForListingUrl(env: Env, listingUrl: string) {
  const host = normalizeLookupDomain(listingUrl);
  const rootDomain = rootDomainFromUrl(listingUrl);
  if (!host || !rootDomain) {
    return { error: "Unable to derive source domain from listing_url." } as const;
  }

  const sources = await env.DB.prepare(
    `SELECT id AS source_id, name, category, claim_url
     FROM citation_sources`
  ).all<SourceRow>();

  const existing =
    (sources.results ?? []).find((source) => {
      const sourceHost = claimHost(source.claim_url);
      return sourceHost && rootDomainFromHost(sourceHost) === rootDomain;
    }) || null;

  if (existing) {
    return {
      source_id: existing.source_id,
      source_name: existing.name,
      category: existing.category,
      claim_url: existing.claim_url,
      inferred_domain: rootDomain,
      created: false,
    } as const;
  }

  const sourceId = makeCustomSourceIdFromDomain(rootDomain);
  const sourceName = `Custom: ${rootDomain}`;
  const now = Math.floor(Date.now() / 1000);
  const claimUrl = `https://${rootDomain}`;

  await env.DB.prepare(
    `INSERT OR REPLACE INTO citation_sources (id, name, category, claim_url, notes, created_at)
     VALUES (?1, ?2, 'directory', ?3, 'Custom citation source added from listing URL root domain', ?4)`
  )
    .bind(sourceId, sourceName, claimUrl, now)
    .run();

  return {
    source_id: sourceId,
    source_name: sourceName,
    category: "directory",
    claim_url: claimUrl,
    inferred_domain: rootDomain,
    created: true,
  } as const;
}

function extractLinksFromHtml(html: string): string[] {
  const links: string[] = [];
  const regex = /href="(https?:\/\/[^"]+)"/gi;
  let match: RegExpExecArray | null = null;

  while ((match = regex.exec(html)) !== null) {
    links.push(match[1]);
    if (links.length >= 20) break;
  }

  return links;
}

async function checkOneSourceWithHttpFetch(source: SourceRow, domain: string): Promise<CheckResult> {
  const sourceHost = claimHost(source.claim_url);
  if (!sourceHost) {
    return {
      source_id: source.source_id,
      name: source.name,
      category: source.category,
      claim_url: source.claim_url,
      source_host: "",
      result: "error",
      confidence: "low",
      matched_links: [],
      detail: "Missing or invalid source URL.",
      provider: "http_fetch",
    };
  }

  const query = `site:${sourceHost} \"${domain}\"`;
  const url = `https://duckduckgo.com/html/?q=${encodeURIComponent(query)}`;

  try {
    const resp = await fetch(url, {
      headers: {
        "user-agent": "Mozilla/5.0 (compatible; SEOAgent/1.0)",
      },
    });

    if (!resp.ok) {
      return {
        source_id: source.source_id,
        name: source.name,
        category: source.category,
        claim_url: source.claim_url,
        source_host: sourceHost,
        result: "error",
        confidence: "low",
        matched_links: [],
        detail: `Search request failed (${resp.status}).`,
        provider: "http_fetch",
      };
    }

    const html = await resp.text();
    const links = extractLinksFromHtml(html)
      .filter((link) => {
        try {
          return new URL(link).hostname.toLowerCase().includes(sourceHost);
        } catch {
          return false;
        }
      })
      .slice(0, 5);

    const mentionsDomain = html.toLowerCase().includes(domain.toLowerCase());

    if (links.length > 0 && mentionsDomain) {
      return {
        source_id: source.source_id,
        name: source.name,
        category: source.category,
        claim_url: source.claim_url,
        source_host: sourceHost,
        result: "listed",
        confidence: "medium",
        matched_links: links,
        detail: "Found source links with the submitted domain in search results.",
        provider: "http_fetch",
      };
    }

    if (links.length > 0) {
      return {
        source_id: source.source_id,
        name: source.name,
        category: source.category,
        claim_url: source.claim_url,
        source_host: sourceHost,
        result: "possible",
        confidence: "low",
        matched_links: links,
        detail: "Found source links but no clear domain match.",
        provider: "http_fetch",
      };
    }

    return {
      source_id: source.source_id,
      name: source.name,
      category: source.category,
      claim_url: source.claim_url,
      source_host: sourceHost,
      result: "not_found",
      confidence: "low",
      matched_links: [],
      detail: "No obvious listing match from search results.",
      provider: "http_fetch",
    };
  } catch (err) {
    return {
      source_id: source.source_id,
      name: source.name,
      category: source.category,
      claim_url: source.claim_url,
      source_host: sourceHost,
      result: "error",
      confidence: "low",
      matched_links: [],
      detail: `Lookup error: ${String(err)}`,
      provider: "http_fetch",
    };
  }
}

function usesHeadlessBrowser(source: SourceRow): boolean {
  return source.source_id === "src_dnb";
}

async function checkOneSourceWithHeadlessBrowser(
  source: SourceRow,
  domain: string,
  businessName: string,
  env: Env
): Promise<CheckResult> {
  const sourceHost = claimHost(source.claim_url);
  if (!businessName) {
    return {
      source_id: source.source_id,
      name: source.name,
      category: source.category,
      claim_url: source.claim_url,
      source_host: sourceHost,
      result: "error",
      confidence: "low",
      matched_links: [],
      detail: "Missing business_name for browser-based check.",
      provider: "headless_browser",
    };
  }

  if (!env.BROWSER_CHECKER_URL) {
    return {
      source_id: source.source_id,
      name: source.name,
      category: source.category,
      claim_url: source.claim_url,
      source_host: sourceHost,
      result: "error",
      confidence: "low",
      matched_links: [],
      detail: "BROWSER_CHECKER_URL is not configured for headless checks.",
      provider: "headless_browser",
    };
  }

  try {
    const endpoint = `${env.BROWSER_CHECKER_URL.replace(/\/+$/, "")}/check/dnb`;
    const resp = await fetch(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(env.BROWSER_CHECKER_TOKEN ? { authorization: `Bearer ${env.BROWSER_CHECKER_TOKEN}` } : {}),
      },
      body: JSON.stringify({
        business_name: businessName,
        domain,
        source_id: source.source_id,
        source_name: source.name,
      }),
    });

    if (!resp.ok) {
      return {
        source_id: source.source_id,
        name: source.name,
        category: source.category,
        claim_url: source.claim_url,
        source_host: sourceHost,
        result: "error",
        confidence: "low",
        matched_links: [],
        detail: `Headless provider failed (${resp.status}).`,
        provider: "headless_browser",
      };
    }

    const payload = (await resp.json()) as Partial<CheckResult>;
    return {
      source_id: source.source_id,
      name: source.name,
      category: source.category,
      claim_url: source.claim_url,
      source_host: sourceHost,
      result: payload.result ?? "possible",
      confidence: payload.confidence ?? "low",
      matched_links: payload.matched_links ?? [],
      detail: payload.detail ?? "Headless check completed.",
      provider: "headless_browser",
    };
  } catch (err) {
    return {
      source_id: source.source_id,
      name: source.name,
      category: source.category,
      claim_url: source.claim_url,
      source_host: sourceHost,
      result: "error",
      confidence: "low",
      matched_links: [],
      detail: `Headless lookup error: ${String(err)}`,
      provider: "headless_browser",
    };
  }
}

async function checkListings(env: Env, domain: string, businessName: string) {
  const sourcesResult = await env.DB.prepare(
    `SELECT id AS source_id, name, category, claim_url
     FROM citation_sources
     ORDER BY category, name`
  ).all<SourceRow>();

  const sources = sourcesResult.results ?? [];
  const checks = await Promise.all(
    sources.map((source) =>
      usesHeadlessBrowser(source)
        ? checkOneSourceWithHeadlessBrowser(source, domain, businessName, env)
        : checkOneSourceWithHttpFetch(source, domain)
    )
  );

  return {
    domain,
    checked_sources: checks.length,
    summary: {
      listed: checks.filter((x) => x.result === "listed").length,
      possible: checks.filter((x) => x.result === "possible").length,
      not_found: checks.filter((x) => x.result === "not_found").length,
      error: checks.filter((x) => x.result === "error").length,
    },
    checks,
  };
}

async function cleanupExpiredEvidence(env: Env): Promise<void> {
  const now = Math.floor(Date.now() / 1000);
  const rows = await env.DB.prepare(
    `SELECT id, site_id, source_id, evidence_json
     FROM citations
     WHERE evidence_json IS NOT NULL AND evidence_json <> ''`
  ).all<{
    id: string;
    site_id: string;
    source_id: string;
    evidence_json: string | null;
  }>();

  for (const row of rows.results ?? []) {
    const evidenceItems = parseEvidenceJson(row.evidence_json ?? "[]");
    const keep: EvidenceItem[] = [];
    const remove: EvidenceItem[] = [];
    for (const item of evidenceItems) {
      if (isExpiredEvidence(item, now)) remove.push(item);
      else keep.push(item);
    }
    if (!remove.length) continue;

    for (const item of remove) {
      if (item.kind === "file" && item.r2_key) {
        await env.EVIDENCE_BUCKET.delete(item.r2_key);
      }
    }

    await env.DB.prepare(
      `UPDATE citations
       SET evidence_json = ?1, updated_at = ?2
       WHERE id = ?3`
    )
      .bind(JSON.stringify(keep), now, row.id)
      .run();
  }
}

function buildSsoLoginRedirect(request: Request, env: Env): Response {
  if (!env.OTRUST_SSO_LOGIN_URL) {
    return jsonBase(
      {
        error: "OTRUST_SSO_LOGIN_URL is not configured.",
        hint: "Set this in Wrangler vars/secrets, then retry login.",
      },
      501
    );
  }

  const reqUrl = new URL(request.url);
  const returnTo = reqUrl.searchParams.get("return_to") || reqUrl.origin;

  const loginUrl = new URL(env.OTRUST_SSO_LOGIN_URL);
  loginUrl.searchParams.set("return_to", returnTo);

  if (env.OTRUST_SSO_CLIENT_ID) {
    loginUrl.searchParams.set("client_id", env.OTRUST_SSO_CLIENT_ID);
  }

  return Response.redirect(loginUrl.toString(), 302);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const siteIdFromPath = extractSiteIdFromPath(url.pathname);
    const startedAt = Date.now();
    const json = (data: unknown, status = 200) => withCors(jsonBase(data, status), request, env);
    const text = (body: string, status = 200) => withCors(textBase(body, status), request, env);
    const options = () => withCors(optionsBase(), request, env);

    try {
      if (request.method === "OPTIONS") return options();

      if (request.method !== "GET" && request.method !== "POST") {
        return json({ error: "Method not allowed" }, 405);
      }

    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
      return json({ ok: true, service: "seo-agent", date: new Date().toISOString() });
    }

      if (url.pathname.startsWith("/api/")) {
        if (!originIsAllowed(request, env)) {
          return json({ error: "Origin is not allowed." }, 403);
        }
        if (!authorizedForRequest(request, env)) {
          return json(
            {
              error: "Unauthorized",
              hint: "Set ADMIN_API_TOKEN and pass it via Authorization: Bearer <token> or ?api_key=<token>.",
          },
          401
        );
        }
        if (!siteIsAllowed(siteIdFromPath, env)) {
          return json({ error: "Forbidden for this site_id." }, 403);
        }
        if (isWriteMethod(request.method)) {
          const limit = checkRateLimit(request, env);
          if (!limit.allowed) {
            const resp = jsonBase({ error: "Rate limit exceeded", retry_after: limit.retryAfter }, 429);
            resp.headers.set("retry-after", String(limit.retryAfter));
            return withCors(resp, request, env);
          }
        }
      }

      if (url.pathname === "/api/auth/login") {
        return buildSsoLoginRedirect(request, env);
      }

      if (url.pathname === "/api/ops/metrics") {
        if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
        const now = Math.floor(Date.now() / 1000);
        const since24h = now - 86400;
        const [citationStats, followUpStats, audit24h, errors24h] = await Promise.all([
          env.DB.prepare(
            `SELECT COUNT(*) AS total, SUM(CASE WHEN listing_url IS NOT NULL AND listing_url <> '' THEN 1 ELSE 0 END) AS saved
             FROM citations`
          ).first<{ total: number; saved: number }>(),
          env.DB.prepare(
            `SELECT COUNT(*) AS due
             FROM citations
             WHERE follow_up_at IS NOT NULL AND follow_up_at <> '' AND follow_up_at <= ?1`
          )
            .bind(new Date().toISOString().slice(0, 10))
            .first<{ due: number }>(),
          env.DB.prepare(
            `SELECT COUNT(*) AS count
             FROM audit_logs
             WHERE created_at >= ?1`
          )
            .bind(since24h)
            .first<{ count: number }>(),
          env.DB.prepare(
            `SELECT COUNT(*) AS count
             FROM app_events
             WHERE level = 'error' AND created_at >= ?1`
          )
            .bind(since24h)
            .first<{ count: number }>(),
        ]);

        return json({
          ok: true,
          generated_at: new Date().toISOString(),
          uptime_ms: Date.now() - startedAt,
          citations_total: citationStats?.total ?? 0,
          citations_saved: citationStats?.saved ?? 0,
          followups_due_or_overdue: followUpStats?.due ?? 0,
          audit_events_last_24h: audit24h?.count ?? 0,
          error_events_last_24h: errors24h?.count ?? 0,
        });
      }

    if (url.pathname === "/api/listing-check") {
      if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
      const rawUrl = url.searchParams.get("url") || "";
      const businessName = (url.searchParams.get("business_name") || "").trim();
      const domain = normalizeDomain(rawUrl);
      if (!domain) {
        return json(
          {
            error: "Missing or invalid url query parameter.",
            example: "/api/listing-check?url=https://example.com",
          },
          400
        );
      }

      const result = await checkListings(env, domain, businessName);
      return json(result);
    }

    const match = url.pathname.match(/^\/api\/sites\/([^/]+)\/citations$/);
    if (match) {
      if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
      const siteId = decodeURIComponent(match[1]);
      const hasFollowUpAt = await citationsHasColumn(env, "follow_up_at");
      const { limit, offset } = parsePagination(url, { limit: 500, max: 1000 });

      const site = await env.DB.prepare(
        `SELECT id, url, domain, business_name, primary_city, primary_state, email, is_active, created_at
         FROM sites
         WHERE id = ?1`
      )
        .bind(siteId)
        .first();

      const rows = await env.DB.prepare(
        `SELECT
           cs.id AS source_id,
           cs.name,
           cs.category,
           cs.claim_url,
           c.status,
           c.listing_url,
           c.last_step,
           c.notes,
           ${hasFollowUpAt ? "c.follow_up_at" : "NULL AS follow_up_at"},
           c.evidence_json,
           c.updated_at,
           c.created_at
         FROM citation_sources cs
         LEFT JOIN citations c
           ON c.source_id = cs.id
          AND c.site_id = ?1
         ORDER BY cs.category, cs.name
         LIMIT ?2 OFFSET ?3`
      )
        .bind(siteId, limit, offset)
        .all<CitationRow>();
      const totalResult = await env.DB.prepare(`SELECT COUNT(*) AS total FROM citation_sources`).first<{ total: number }>();
      const total = totalResult?.total ?? 0;

      return json({
        site_id: siteId,
        site,
        page: {
          limit,
          offset,
          total,
          has_more: offset + limit < total,
        },
        citations: (rows.results ?? []).map((r) => ({
          ...r,
          status: r.status ?? "todo",
          evidence_json: r.evidence_json ?? "[]",
        })),
      });
    }

    const saveMatch = url.pathname.match(/^\/api\/sites\/([^/]+)\/listing-url$/);
    if (saveMatch) {
      if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
      const siteId = decodeURIComponent(saveMatch[1]);

      let body: { listing_url?: string } = {};
      try {
        body = (await request.json()) as { listing_url?: string };
      } catch {
        return json({ error: "Invalid JSON body." }, 400);
      }

      const listingUrl = normalizeListingUrl(body.listing_url || "");
      if (!listingUrl) {
        return json({ error: "listing_url must be a valid http/https URL." }, 400);
      }

      const sourceMatch = await findOrCreateSourceForListingUrl(env, listingUrl);
      if ("error" in sourceMatch) {
        return json({ error: sourceMatch.error }, 400);
      }

      const now = Math.floor(Date.now() / 1000);
      const sourceId = sourceMatch.source_id;
      const citationId = `cit_${siteId}_${sourceId}`;

      await env.DB.prepare(
        `INSERT INTO citations (
           id, site_id, source_id, status, listing_url, login_email, last_step, evidence_json, updated_at, created_at
         ) VALUES (?1, ?2, ?3, 'live', ?4, NULL, 'Listing URL saved from dashboard', '{}', ?5, ?5)
         ON CONFLICT(site_id, source_id) DO UPDATE SET
           listing_url = excluded.listing_url,
           last_step = 'Listing URL saved from dashboard',
           updated_at = excluded.updated_at,
           status = CASE WHEN citations.status = 'rejected' THEN citations.status ELSE 'live' END`
      )
        .bind(citationId, siteId, sourceId, listingUrl, now)
        .run();

      await writeAuditLog(env, request, siteId, "listing_url_saved", {
        source_id: sourceId,
        source_name: sourceMatch.source_name,
        listing_url: listingUrl,
      });

      return json({
        ok: true,
        site_id: siteId,
        source_id: sourceId,
        source_name: sourceMatch.source_name,
        inferred_domain: sourceMatch.inferred_domain,
        listing_url: listingUrl,
        updated_at: now,
      });
    }

    const customSaveMatch = url.pathname.match(/^\/api\/sites\/([^/]+)\/custom-citation$/);
    if (customSaveMatch) {
      if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
      const siteId = decodeURIComponent(customSaveMatch[1]);

      let body: { listing_url?: string; source_name?: string } = {};
      try {
        body = (await request.json()) as { listing_url?: string; source_name?: string };
      } catch {
        return json({ error: "Invalid JSON body." }, 400);
      }

      const listingUrl = normalizeListingUrl(body.listing_url || "");
      if (!listingUrl) {
        return json({ error: "listing_url must be a valid http/https URL." }, 400);
      }

      const domain = normalizeLookupDomain(listingUrl);
      if (!domain) {
        return json({ error: "Unable to derive domain from listing_url." }, 400);
      }

      const sourceId = makeCustomSourceIdFromDomain(domain);
      const sourceName = (body.source_name || "").trim() || `Custom: ${domain}`;
      const claimUrl = `https://${domain}`;
      const now = Math.floor(Date.now() / 1000);

      await env.DB.prepare(
        `INSERT OR REPLACE INTO citation_sources (id, name, category, claim_url, notes, created_at)
         VALUES (?1, ?2, 'directory', ?3, 'Custom citation source added from dashboard', ?4)`
      )
        .bind(sourceId, sourceName, claimUrl, now)
        .run();

      const citationId = `cit_${siteId}_${sourceId}`;
      await env.DB.prepare(
        `INSERT INTO citations (
           id, site_id, source_id, status, listing_url, login_email, last_step, evidence_json, updated_at, created_at
         ) VALUES (?1, ?2, ?3, 'live', ?4, NULL, 'Custom citation URL saved from dashboard', '{}', ?5, ?5)
         ON CONFLICT(site_id, source_id) DO UPDATE SET
           listing_url = excluded.listing_url,
           last_step = 'Custom citation URL saved from dashboard',
           updated_at = excluded.updated_at,
           status = CASE WHEN citations.status = 'rejected' THEN citations.status ELSE 'live' END`
      )
        .bind(citationId, siteId, sourceId, listingUrl, now)
        .run();

      await writeAuditLog(env, request, siteId, "custom_citation_saved", {
        source_id: sourceId,
        source_name: sourceName,
        listing_url: listingUrl,
      });

      return json({
        ok: true,
        site_id: siteId,
        source_id: sourceId,
        source_name: sourceName,
        listing_url: listingUrl,
        updated_at: now,
      });
    }

    const citationUpdateMatch = url.pathname.match(/^\/api\/sites\/([^/]+)\/citations\/([^/]+)$/);
    if (citationUpdateMatch) {
      if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
      const siteId = decodeURIComponent(citationUpdateMatch[1]);
      const sourceId = decodeURIComponent(citationUpdateMatch[2]);

      let body: { status?: string; notes?: string; follow_up_at?: string | null } = {};
      try {
        body = (await request.json()) as { status?: string; notes?: string; follow_up_at?: string | null };
      } catch {
        return json({ error: "Invalid JSON body." }, 400);
      }

      const status = normalizeStatus(body.status || "");
      const notes = normalizeNotes(body.notes || "");
      const rawFollowUpAt = body.follow_up_at == null ? "" : String(body.follow_up_at);
      const followUpAt = normalizeFollowUpAt(rawFollowUpAt);
      const hasFollowUpAt = await citationsHasColumn(env, "follow_up_at");
      if (!status) {
        return json({ error: "status is required and must be valid." }, 400);
      }
      if (rawFollowUpAt && !followUpAt) {
        return json({ error: "follow_up_at must be YYYY-MM-DD or empty." }, 400);
      }
      if (rawFollowUpAt && !hasFollowUpAt) {
        return json({ error: "Reminder dates are not available until the latest D1 migration is applied." }, 503);
      }

      const sourceExists = await env.DB.prepare(`SELECT id FROM citation_sources WHERE id = ?1`).bind(sourceId).first();
      if (!sourceExists) {
        return json({ error: "Unknown source_id." }, 404);
      }

      const existing = await env.DB.prepare(
        `SELECT id, listing_url, login_email, evidence_json, created_at
         FROM citations
         WHERE site_id = ?1 AND source_id = ?2
         LIMIT 1`
      )
        .bind(siteId, sourceId)
        .first<{
          id: string;
          listing_url: string | null;
          login_email: string | null;
          evidence_json: string | null;
          created_at: number | null;
        }>();

      const now = Math.floor(Date.now() / 1000);
      const citationId = existing?.id || `cit_${siteId}_${sourceId}`;
      if (hasFollowUpAt) {
        await env.DB.prepare(
          `INSERT INTO citations (
             id, site_id, source_id, status, listing_url, login_email, last_step, evidence_json, updated_at, created_at, notes, follow_up_at
           ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'Citation updated from dashboard', ?7, ?8, ?9, ?10, ?11)
           ON CONFLICT(site_id, source_id) DO UPDATE SET
             status = excluded.status,
             notes = excluded.notes,
             follow_up_at = excluded.follow_up_at,
             last_step = 'Citation updated from dashboard',
             updated_at = excluded.updated_at`
        )
          .bind(
            citationId,
            siteId,
            sourceId,
            status,
            existing?.listing_url ?? null,
            existing?.login_email ?? null,
            existing?.evidence_json ?? "{}",
            now,
            existing?.created_at ?? now,
            notes || null,
            followUpAt || null
          )
          .run();
      } else {
        await env.DB.prepare(
          `INSERT INTO citations (
             id, site_id, source_id, status, listing_url, login_email, last_step, evidence_json, updated_at, created_at, notes
           ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'Citation updated from dashboard', ?7, ?8, ?9, ?10)
           ON CONFLICT(site_id, source_id) DO UPDATE SET
             status = excluded.status,
             notes = excluded.notes,
             last_step = 'Citation updated from dashboard',
             updated_at = excluded.updated_at`
        )
          .bind(
            citationId,
            siteId,
            sourceId,
            status,
            existing?.listing_url ?? null,
            existing?.login_email ?? null,
            existing?.evidence_json ?? "{}",
            now,
            existing?.created_at ?? now,
            notes || null
          )
          .run();
      }

      await writeAuditLog(env, request, siteId, "citation_updated", {
        source_id: sourceId,
        status,
        has_notes: Boolean(notes),
        follow_up_at: followUpAt || null,
      });

      return json({
        ok: true,
        site_id: siteId,
        source_id: sourceId,
        status,
        notes,
        follow_up_at: followUpAt || null,
        updated_at: now,
      });
    }

    const auditMatch = url.pathname.match(/^\/api\/sites\/([^/]+)\/audit$/);
    if (auditMatch) {
      if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
      const siteId = decodeURIComponent(auditMatch[1]);
      const { limit, offset } = parsePagination(url, { limit: 100, max: 500 });
      const rows = await env.DB.prepare(
        `SELECT id, site_id, action, actor, request_path, payload_json, created_at
         FROM audit_logs
         WHERE site_id = ?1
         ORDER BY created_at DESC
         LIMIT ?2 OFFSET ?3`
      )
        .bind(siteId, limit, offset)
        .all<{
          id: string;
          site_id: string | null;
          action: string;
          actor: string | null;
          request_path: string | null;
          payload_json: string | null;
          created_at: number;
        }>();
      const totalResult = await env.DB.prepare(
        `SELECT COUNT(*) AS total FROM audit_logs WHERE site_id = ?1`
      )
        .bind(siteId)
        .first<{ total: number }>();
      const total = totalResult?.total ?? 0;
      return json({
        site_id: siteId,
        page: { limit, offset, total, has_more: offset + limit < total },
        logs: rows.results ?? [],
      });
    }

    const evidenceMatch = url.pathname.match(/^\/api\/sites\/([^/]+)\/citations\/([^/]+)\/evidence$/);
    if (evidenceMatch) {
      if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
      const siteId = decodeURIComponent(evidenceMatch[1]);
      const sourceId = decodeURIComponent(evidenceMatch[2]);

      let body: {
        evidence_url?: string;
        file_name?: string;
        content_type?: string;
        data_base64?: string;
        note?: string;
      } = {};
      try {
        body = (await request.json()) as {
          evidence_url?: string;
          file_name?: string;
          content_type?: string;
          data_base64?: string;
          note?: string;
        };
      } catch {
        return json({ error: "Invalid JSON body." }, 400);
      }

      const evidenceUrl = normalizeListingUrl(body.evidence_url || "");
      const fileName = (body.file_name || "").trim().slice(0, 200);
      const contentType = (body.content_type || "").trim().slice(0, 120);
      const dataBase64 = (body.data_base64 || "").trim();
      const note = normalizeEvidenceNote(body.note || "");
      const retentionSeconds = evidenceRetentionSeconds(env);

      const isLink = Boolean(evidenceUrl);
      const isFile = Boolean(fileName && contentType && dataBase64);
      if (!isLink && !isFile) {
        return json({ error: "Provide either evidence_url or file upload data." }, 400);
      }
      const fileBytes = isFile ? base64ToBytes(dataBase64) : null;
      if (isFile && fileBytes && fileBytes.byteLength > maxEvidenceBytes(env)) {
        return json({ error: `Uploaded file exceeds max size (${maxEvidenceBytes(env)} bytes).` }, 400);
      }

      const sourceExists = await env.DB.prepare(`SELECT id FROM citation_sources WHERE id = ?1`).bind(sourceId).first();
      if (!sourceExists) {
        return json({ error: "Unknown source_id." }, 404);
      }

      const existing = await env.DB.prepare(
        `SELECT id, status, listing_url, login_email, evidence_json, created_at, notes
         FROM citations
         WHERE site_id = ?1 AND source_id = ?2
         LIMIT 1`
      )
        .bind(siteId, sourceId)
        .first<{
          id: string;
          status: string | null;
          listing_url: string | null;
          login_email: string | null;
          evidence_json: string | null;
          created_at: number | null;
          notes: string | null;
        }>();

      const evidenceItems = parseEvidenceJson(existing?.evidence_json ?? "[]");
      const now = Math.floor(Date.now() / 1000);
      evidenceItems.unshift(
        isLink
          ? {
              id: makeEvidenceId(),
              kind: "link",
              url: evidenceUrl,
              note: note || undefined,
              created_at: now,
              expires_at: retentionSeconds ? now + retentionSeconds : undefined,
            }
          : {
              id: makeEvidenceId(),
              kind: "file",
              file_name: fileName,
              content_type: contentType,
              note: note || undefined,
              created_at: now,
              expires_at: retentionSeconds ? now + retentionSeconds : undefined,
            }
      );

      const newest = evidenceItems[0];
      if (newest.kind === "file") {
        const r2Key = makeEvidenceKey(siteId, sourceId, newest.id, fileName);
        await env.EVIDENCE_BUCKET.put(r2Key, fileBytes || new Uint8Array(), {
          httpMetadata: {
            contentType,
            contentDisposition: contentDispositionForEvidence(contentType, fileName),
          },
        });
        newest.r2_key = r2Key;
      }

      const citationId = existing?.id || `cit_${siteId}_${sourceId}`;
      await env.DB.prepare(
        `INSERT INTO citations (
           id, site_id, source_id, status, listing_url, login_email, last_step, evidence_json, updated_at, created_at, notes
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'Evidence uploaded from dashboard', ?7, ?8, ?9, ?10)
         ON CONFLICT(site_id, source_id) DO UPDATE SET
           evidence_json = excluded.evidence_json,
           last_step = 'Evidence uploaded from dashboard',
           updated_at = excluded.updated_at`
      )
        .bind(
          citationId,
          siteId,
          sourceId,
          existing?.status ?? "todo",
          existing?.listing_url ?? null,
          existing?.login_email ?? null,
          JSON.stringify(evidenceItems),
          now,
          existing?.created_at ?? now,
          existing?.notes ?? null
        )
        .run();

      await writeAuditLog(env, request, siteId, "evidence_uploaded", {
        source_id: sourceId,
        evidence_kind: newest.kind,
        evidence_count: evidenceItems.length,
      });

      return json({
        ok: true,
        site_id: siteId,
        source_id: sourceId,
        evidence_count: evidenceItems.length,
        uploaded_at: now,
      });
    }

    const evidenceDownloadMatch = url.pathname.match(/^\/api\/sites\/([^/]+)\/citations\/([^/]+)\/evidence\/([^/]+)$/);
    if (evidenceDownloadMatch) {
      if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
      const siteId = decodeURIComponent(evidenceDownloadMatch[1]);
      const sourceId = decodeURIComponent(evidenceDownloadMatch[2]);
      const evidenceId = decodeURIComponent(evidenceDownloadMatch[3]);

      const citation = await env.DB.prepare(
        `SELECT evidence_json
         FROM citations
         WHERE site_id = ?1 AND source_id = ?2
         LIMIT 1`
      )
        .bind(siteId, sourceId)
        .first<{ evidence_json: string | null }>();

      const item = parseEvidenceJson(citation?.evidence_json ?? "[]").find((entry) => entry.id === evidenceId);
      if (!item) return text("Not found", 404);
      if (isExpiredEvidence(item, Math.floor(Date.now() / 1000))) {
        return text("Evidence expired", 410);
      }

      if (item.kind === "link" && item.url) {
        return Response.redirect(item.url, 302);
      }

      if (item.kind === "file" && item.r2_key) {
        const object = await env.EVIDENCE_BUCKET.get(item.r2_key);
        if (!object) return text("Not found", 404);
        const headers = new Headers();
        object.writeHttpMetadata(headers);
        headers.set("etag", object.httpEtag);
        return withCors(new Response(object.body, { headers }), request, env);
      }

      if (item.kind === "file" && item.data_base64) {
        return withCors(
          new Response(base64ToBytes(item.data_base64), {
          headers: {
            "content-type": item.content_type || "application/octet-stream",
            "content-disposition": contentDispositionForEvidence(
              item.content_type || "application/octet-stream",
              item.file_name || "evidence"
            ),
          },
          }),
          request,
          env
        );
      }

      return text("Not found", 404);
    }

    if (url.pathname === "/api/listings/lookup") {
      if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
      const lookupDomain = normalizeLookupDomain(url.searchParams.get("domain") || "");
      const { limit, offset } = parsePagination(url, { limit: 100, max: 500 });
      if (!lookupDomain) {
        return json(
          {
            error: "Missing or invalid domain query parameter.",
            example: "/api/listings/lookup?domain=dnb.com",
          },
          400
        );
      }

      const rows = await env.DB.prepare(
        `SELECT
           c.site_id,
           s.domain,
           s.business_name,
           cs.id AS source_id,
           cs.name AS source_name,
           c.status,
           c.listing_url,
           cs.claim_url,
           c.updated_at
         FROM citations c
         LEFT JOIN sites s ON s.id = c.site_id
         LEFT JOIN citation_sources cs ON cs.id = c.source_id
         WHERE c.listing_url IS NOT NULL
         AND (c.listing_url LIKE ?1 OR cs.claim_url LIKE ?1)
         ORDER BY c.updated_at DESC
         LIMIT ?2 OFFSET ?3`
      )
        .bind(`%${lookupDomain}%`, limit, offset)
        .all<{
          site_id: string;
          domain: string | null;
          business_name: string | null;
          source_id: string | null;
          source_name: string | null;
          status: string | null;
          listing_url: string | null;
          claim_url: string | null;
          updated_at: number | null;
        }>();

      const matches = (rows.results ?? []).filter((r) => {
        const hosts: string[] = [];
        if (r.listing_url) {
          const h = normalizeLookupDomain(r.listing_url);
          if (h) hosts.push(h);
        }
        if (r.claim_url) {
          const h = normalizeLookupDomain(r.claim_url);
          if (h) hosts.push(h);
        }
        return hosts.some((h) => h === lookupDomain || h.endsWith(`.${lookupDomain}`) || lookupDomain.endsWith(`.${h}`));
      });
      const totalResult = await env.DB.prepare(
        `SELECT COUNT(*) AS total
         FROM citations c
         LEFT JOIN citation_sources cs ON cs.id = c.source_id
         WHERE c.listing_url IS NOT NULL
         AND (c.listing_url LIKE ?1 OR cs.claim_url LIKE ?1)`
      )
        .bind(`%${lookupDomain}%`)
        .first<{ total: number }>();
      const total = totalResult?.total ?? 0;

      return json({
        lookup_domain: lookupDomain,
        page: { limit, offset, total, has_more: offset + limit < total },
        matches,
      });
    }

    const exportMatch = url.pathname.match(/^\/api\/sites\/([^/]+)\/export$/);
    if (exportMatch) {
      if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
      const siteId = decodeURIComponent(exportMatch[1]);
      const site = await env.DB.prepare(
        `SELECT id, url, domain, business_name, primary_city, primary_state, email, is_active, created_at
         FROM sites WHERE id = ?1 LIMIT 1`
      )
        .bind(siteId)
        .first();
      if (!site) return json({ error: "site not found" }, 404);

      const citations = await env.DB.prepare(
        `SELECT id, site_id, source_id, status, listing_url, last_step, notes, follow_up_at, evidence_json, updated_at, created_at
         FROM citations
         WHERE site_id = ?1
         ORDER BY updated_at DESC`
      )
        .bind(siteId)
        .all();

      const audit = await env.DB.prepare(
        `SELECT id, action, actor, request_path, payload_json, created_at
         FROM audit_logs
         WHERE site_id = ?1
         ORDER BY created_at DESC`
      )
        .bind(siteId)
        .all();

      return json({
        exported_at: new Date().toISOString(),
        site,
        citations: citations.results ?? [],
        audit_logs: audit.results ?? [],
      });
    }

    if (url.pathname === "/api/sites/bootstrap") {
      if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
      let body: { domain?: string; business_name?: string; email?: string } = {};
      try {
        body = (await request.json()) as { domain?: string; business_name?: string; email?: string };
      } catch {
        return json({ error: "Invalid JSON body." }, 400);
      }

      const domain = normalizeLookupDomain(body.domain || "");
      const businessName = (body.business_name || "").trim().slice(0, 180);
      const email = normalizeEmail(body.email || "");
      if (!domain) {
        return json({ error: "Missing or invalid domain." }, 400);
      }
      if ((body.email || "").trim() && !email) {
        return json({ error: "Invalid email format." }, 400);
      }

      const existing = await env.DB.prepare(
        `SELECT id, url, domain, business_name, created_at
         FROM sites
         WHERE domain = ?1
         LIMIT 1`
      )
        .bind(domain)
        .first<{
          id: string;
          url: string | null;
          domain: string;
          business_name: string | null;
          created_at: number;
        }>();

      if (existing) {
        if (businessName || email) {
          await env.DB.prepare(
            `UPDATE sites
             SET business_name = COALESCE(?1, business_name),
                 email = COALESCE(?2, email)
             WHERE id = ?3`
          )
            .bind(businessName || null, email || null, existing.id)
            .run();
        }
        return json({ created: false, site: { ...existing, business_name: businessName || existing.business_name } });
      }

      const now = Math.floor(Date.now() / 1000);
      const siteId = makeSiteIdFromDomain(domain);
      const siteUrl = `https://${domain}`;
      await env.DB.prepare(
        `INSERT INTO sites (
           id, url, domain, business_name, primary_city, primary_state, email,
           baseline_start_date, baseline_end_date, is_active, created_at
         ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, NULL, NULL, 1, ?6)`
      )
        .bind(siteId, siteUrl, domain, businessName || null, email || null, now)
        .run();

      await writeAuditLog(env, request, siteId, "site_bootstrap_created", {
        domain,
        business_name: businessName || null,
        email: email || null,
      });

      return json({
        created: true,
        site: {
          id: siteId,
          url: siteUrl,
          domain,
          business_name: businessName || null,
          email: email || null,
          created_at: now,
        },
      });
    }

    if (url.pathname === "/api/sites/resolve") {
      if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
      const domain = normalizeLookupDomain(url.searchParams.get("domain") || "");
      if (!domain) {
        return json(
          {
            error: "Missing or invalid domain query parameter.",
            example: "/api/sites/resolve?domain=example.com",
          },
          400
        );
      }

      const existing = await env.DB.prepare(
        `SELECT id, url, domain, business_name, created_at FROM sites WHERE domain = ?1 LIMIT 1`
      )
        .bind(domain)
        .first<{
          id: string;
          url: string | null;
          domain: string;
          business_name: string | null;
          created_at: number;
        }>();

      if (existing) {
        return json({
          created: false,
          site: existing,
        });
      }

      const now = Math.floor(Date.now() / 1000);
      const siteId = makeSiteIdFromDomain(domain);
      const siteUrl = `https://${domain}`;

      await env.DB.prepare(
        `INSERT INTO sites (
           id, url, domain, business_name, primary_city, primary_state, email,
           baseline_start_date, baseline_end_date, is_active, created_at
         ) VALUES (?1, ?2, ?3, NULL, NULL, NULL, NULL, NULL, NULL, 1, ?4)`
      )
        .bind(siteId, siteUrl, domain, now)
        .run();

      return json({
        created: true,
        site: {
          id: siteId,
          url: siteUrl,
          domain,
          business_name: null,
          created_at: now,
        },
      });
    }

      return text("Not found", 404);
    } catch (err) {
      await writeAppEvent(env, "error", "unhandled_exception", String(err), url.pathname, {
        method: request.method,
        site_id: siteIdFromPath || null,
      });
      return json({ error: "Internal server error" }, 500);
    }
  },

  async scheduled(_event: ScheduledEvent, env: Env, _ctx: ExecutionContext): Promise<void> {
    await cleanupExpiredEvidence(env);
  },
};
