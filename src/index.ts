export interface Env {
  DB: D1Database;
  EVIDENCE_BUCKET: R2Bucket;
  ADMIN_API_TOKEN?: string;
  ALLOWED_SITE_IDS?: string;
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

const corsHeaders = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET,POST,OPTIONS",
  "access-control-allow-headers": "content-type",
};

const json = (data: unknown, status = 200) =>
  new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...corsHeaders,
    },
  });

const text = (body: string, status = 200) =>
  new Response(body, {
    status,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      ...corsHeaders,
    },
  });

const options = () =>
  new Response(null, {
    status: 204,
    headers: {
      ...corsHeaders,
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

function makeEvidenceId(): string {
  return `ev_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function base64ToBytes(base64: string): Uint8Array {
  const bin = atob(base64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) bytes[i] = bin.charCodeAt(i);
  return bytes;
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

function buildSsoLoginRedirect(request: Request, env: Env): Response {
  if (!env.OTRUST_SSO_LOGIN_URL) {
    return json(
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

    if (request.method === "OPTIONS") return options();

    if (request.method !== "GET" && request.method !== "POST") {
      return json({ error: "Method not allowed" }, 405);
    }

    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
      return json({ ok: true, service: "seo-agent", date: new Date().toISOString() });
    }

    if (url.pathname.startsWith("/api/")) {
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
    }

    if (url.pathname === "/api/auth/login") {
      return buildSsoLoginRedirect(request, env);
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
         ORDER BY cs.category, cs.name`
      )
        .bind(siteId)
        .all<CitationRow>();

      return json({
        site_id: siteId,
        site,
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
      const rows = await env.DB.prepare(
        `SELECT id, site_id, action, actor, request_path, payload_json, created_at
         FROM audit_logs
         WHERE site_id = ?1
         ORDER BY created_at DESC
         LIMIT 200`
      )
        .bind(siteId)
        .all<{
          id: string;
          site_id: string | null;
          action: string;
          actor: string | null;
          request_path: string | null;
          payload_json: string | null;
          created_at: number;
        }>();
      return json({ site_id: siteId, logs: rows.results ?? [] });
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

      const isLink = Boolean(evidenceUrl);
      const isFile = Boolean(fileName && contentType && dataBase64);
      if (!isLink && !isFile) {
        return json({ error: "Provide either evidence_url or file upload data." }, 400);
      }
      if (isFile && dataBase64.length > 700000) {
        return json({ error: "Uploaded file is too large for inline storage." }, 400);
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
            }
          : {
              id: makeEvidenceId(),
              kind: "file",
              file_name: fileName,
              content_type: contentType,
              note: note || undefined,
              created_at: now,
            }
      );

      const newest = evidenceItems[0];
      if (newest.kind === "file") {
        const r2Key = makeEvidenceKey(siteId, sourceId, newest.id, fileName);
        await env.EVIDENCE_BUCKET.put(r2Key, base64ToBytes(dataBase64), {
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

      if (item.kind === "link" && item.url) {
        return Response.redirect(item.url, 302);
      }

      if (item.kind === "file" && item.r2_key) {
        const object = await env.EVIDENCE_BUCKET.get(item.r2_key);
        if (!object) return text("Not found", 404);
        const headers = new Headers();
        object.writeHttpMetadata(headers);
        headers.set("etag", object.httpEtag);
        headers.set("access-control-allow-origin", "*");
        headers.set("access-control-allow-methods", "GET,POST,OPTIONS");
        headers.set("access-control-allow-headers", "content-type");
        return new Response(object.body, { headers });
      }

      if (item.kind === "file" && item.data_base64) {
        return new Response(base64ToBytes(item.data_base64), {
          headers: {
            "content-type": item.content_type || "application/octet-stream",
            "content-disposition": contentDispositionForEvidence(
              item.content_type || "application/octet-stream",
              item.file_name || "evidence"
            ),
            ...corsHeaders,
          },
        });
      }

      return text("Not found", 404);
    }

    if (url.pathname === "/api/listings/lookup") {
      if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
      const lookupDomain = normalizeLookupDomain(url.searchParams.get("domain") || "");
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
         ORDER BY c.updated_at DESC`
      )
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

      return json({
        lookup_domain: lookupDomain,
        matches,
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
  },

  async scheduled(_event: ScheduledEvent, _env: Env, _ctx: ExecutionContext): Promise<void> {
    // Placeholder for future daily jobs configured in wrangler.toml.
  },
};
