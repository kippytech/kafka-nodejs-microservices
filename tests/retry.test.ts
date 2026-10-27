import test from "node:test";
import assert from "node:assert/strict";
import { requestWithRetry } from "../packages/shared/src/http/requestWithRetry";

const originalFetch = globalThis.fetch;
test("idempotent HTTP requests retry transient upstream responses", async () => {
  let attempts = 0;
  globalThis.fetch = async () => {
    attempts++;
    return attempts === 1
      ? new Response("busy", { status: 503 })
      : new Response("ok", { status: 200 });
  };
  try {
    const response = await requestWithRetry("http://upstream", {}, { timeoutMs: 100, retries: 1, baseDelayMs: 0 });
    assert.equal(response.status, 200);
    assert.equal(attempts, 2);
  } finally { globalThis.fetch = originalFetch; }
});

test("non-idempotent HTTP requests are not retried", async () => {
  let attempts = 0;
  globalThis.fetch = async () => { attempts++; return new Response("busy", { status: 503 }); };
  try {
    const response = await requestWithRetry("http://upstream", { method: "POST" }, { timeoutMs: 100, retries: 3, baseDelayMs: 0 });
    assert.equal(response.status, 503);
    assert.equal(attempts, 1);
  } finally { globalThis.fetch = originalFetch; }
});
