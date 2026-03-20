const fs = require("fs");
const path = require("path");

function parseInlineScript(filePath) {
  const html = fs.readFileSync(filePath, "utf8");
  const match = html.match(/<script>([\s\S]*)<\/script>/);
  if (!match) {
    throw new Error(`No inline <script> found in ${filePath}`);
  }
  // eslint-disable-next-line no-new-func
  new Function(match[1]);
}

function assertExists(filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Missing required file: ${filePath}`);
  }
}

const root = path.resolve(__dirname, "..");
assertExists(path.join(root, "src", "index.ts"));
assertExists(path.join(root, "dashboard", "public", "index.html"));
parseInlineScript(path.join(root, "dashboard", "public", "index.html"));
console.log("Checks passed.");
