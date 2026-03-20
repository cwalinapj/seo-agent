import http from "node:http";
import { URL } from "node:url";

let chromium;

async function getChromium() {
  if (!chromium) {
    ({ chromium } = await import("playwright"));
  }
  return chromium;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1024 * 1024) {
        reject(new Error("Request too large"));
      }
    });
    req.on("end", () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function send(res, status, payload) {
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization",
  });
  res.end(JSON.stringify(payload));
}

async function runDnbCheck({ business_name, domain }) {
  if (!business_name) {
    return {
      result: "error",
      confidence: "low",
      matched_links: [],
      detail: "business_name is required for D&B check.",
    };
  }

  const chromiumLib = await getChromium();
  const browser = await chromiumLib.launch({ headless: true });

  try {
    const page = await browser.newPage();
    await page.goto("https://www.dnb.com/business-directory.html", {
      waitUntil: "domcontentloaded",
      timeout: 45000,
    });

    // Best-effort selectors for D&B search input/button.
    const inputSelectors = [
      'input[name*="search"]',
      'input[id*="search"]',
      'input[type="search"]',
      'input[placeholder*="Search"]',
    ];

    let inputFound = false;
    for (const sel of inputSelectors) {
      const el = page.locator(sel).first();
      if (await el.count()) {
        await el.fill(business_name);
        inputFound = true;
        break;
      }
    }

    if (!inputFound) {
      return {
        result: "error",
        confidence: "low",
        matched_links: [],
        detail: "Could not find D&B search input.",
      };
    }

    const buttonSelectors = [
      'button:has-text("Search")',
      'input[type="submit"]',
      'button[type="submit"]',
    ];

    let clicked = false;
    for (const sel of buttonSelectors) {
      const btn = page.locator(sel).first();
      if (await btn.count()) {
        await btn.click();
        clicked = true;
        break;
      }
    }

    if (!clicked) {
      await page.keyboard.press("Enter");
    }

    await page.waitForLoadState("domcontentloaded", { timeout: 30000 });
    await page.waitForTimeout(2500);

    const links = await page
      .locator('a[href*="dnb.com"]')
      .evaluateAll((anchors) => anchors.map((a) => a.href).filter(Boolean).slice(0, 8));

    const bodyText = (await page.locator("body").innerText()).toLowerCase();
    const domainMatched = domain ? bodyText.includes(domain.toLowerCase()) : false;
    const nameMatched = bodyText.includes(String(business_name).toLowerCase());

    if (domainMatched || nameMatched) {
      return {
        result: "listed",
        confidence: domainMatched ? "high" : "medium",
        matched_links: links,
        detail: "D&B browser check found matching listing text.",
      };
    }

    if (links.length > 0) {
      return {
        result: "possible",
        confidence: "low",
        matched_links: links,
        detail: "D&B browser check found candidate links without a strict text match.",
      };
    }

    return {
      result: "not_found",
      confidence: "low",
      matched_links: [],
      detail: "No obvious D&B listing match found.",
    };
  } finally {
    await browser.close();
  }
}

const server = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") {
    send(res, 204, {});
    return;
  }

  const url = new URL(req.url, "http://localhost");

  if (req.method === "GET" && url.pathname === "/health") {
    send(res, 200, { ok: true, service: "browser-checker" });
    return;
  }

  if (req.method === "POST" && url.pathname === "/check/dnb") {
    try {
      const expected = process.env.BROWSER_CHECKER_TOKEN || "";
      if (expected) {
        const auth = req.headers.authorization || "";
        if (auth !== `Bearer ${expected}`) {
          send(res, 401, { error: "Unauthorized" });
          return;
        }
      }

      const payload = await readJson(req);
      const result = await runDnbCheck(payload);
      send(res, 200, result);
      return;
    } catch (err) {
      send(res, 500, {
        result: "error",
        confidence: "low",
        matched_links: [],
        detail: `Browser checker exception: ${String(err)}`,
      });
      return;
    }
  }

  send(res, 404, { error: "Not found" });
});

const port = Number(process.env.PORT || 8788);
server.listen(port, () => {
  console.log(`browser-checker listening on :${port}`);
});
