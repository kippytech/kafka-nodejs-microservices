import test from "node:test";
import assert from "node:assert/strict";
import type { Request } from "express";
import { signToken } from "../packages/shared/src/auth/jwt";
import { requireAuthenticatedUser } from "../packages/shared/src/auth/requireAuthenticatedUser";
import { gatewayAuth } from "../apps/api-gateway/src/middleware/gatewayAuth";

test("gateway strips user-controlled identity and only adds the gateway marker", () => {
  process.env.GATEWAY_SECRET = "gateway-test";
  const req = { headers: { "x-user-id": "forged", "x-user-role": "ADMIN", authorization: "Bearer token" } };
  let nextCalled = false;
  gatewayAuth(req as unknown as Request, {} as never, (err?: unknown) => { assert.equal(err, undefined); nextCalled = true; });
  assert.equal(nextCalled, true);
  assert.equal(req.headers["x-user-id"], undefined);
  assert.equal(req.headers["x-user-role"], undefined);
  assert.equal(req.headers["x-gateway-secret"], "gateway-test");
  assert.equal(req.headers.authorization, "Bearer token");
});

test("downstream authentication derives identity from the signed token", () => {
  process.env.JWT_SECRET = "jwt-test";
  const token = signToken({ userId: "user-1", role: "USER" });
  const req = {
    headers: { "x-user-id": "forged", "x-user-role": "ADMIN", authorization: `Bearer ${token}` },
    header(name: string) { return this.headers[name.toLowerCase()]; },
  };
  let nextCalled = false;
  requireAuthenticatedUser(req as unknown as Request, {} as never, (err?: unknown) => { assert.equal(err, undefined); nextCalled = true; });
  assert.equal(nextCalled, true);
  assert.equal(req.headers["x-user-id"], "user-1");
  assert.equal(req.headers["x-user-role"], "USER");
});

