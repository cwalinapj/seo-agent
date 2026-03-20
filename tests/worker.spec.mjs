import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const worker = fs.readFileSync(path.join(root, "src", "index.ts"), "utf8");

test("worker contains core API routes", () => {
  const expected = [
    "/api/health",
    "/api/sites/resolve",
    "/citations$/",
    "/listing-url$/",
    "/evidence$/",
    "/audit$/",
    "/api/ops/metrics",
  ];
  for (const route of expected) {
    assert.equal(worker.includes(route), true, `missing route marker: ${route}`);
  }
});

test("worker enforces supported citation statuses", () => {
  const requiredStatuses = ["todo", "in_progress", "needs_verification", "submitted", "live", "rejected"];
  for (const status of requiredStatuses) {
    assert.equal(worker.includes(`"${status}"`), true, `missing status: ${status}`);
  }
});

test("migrations include audit and observability tables", () => {
  const migrationDir = path.join(root, "migrations");
  const files = fs.readdirSync(migrationDir);
  assert.equal(files.includes("0006_audit_logs.sql"), true);
  assert.equal(files.includes("0008_app_events.sql"), true);
});
