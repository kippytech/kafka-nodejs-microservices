import test from "node:test";
import assert from "node:assert/strict";
import { Server, ServerCredentials } from "@grpc/grpc-js";
import { CheckTaskAccessResponse, TaskAccessServiceService, type TaskAccessServiceServer } from "../packages/shared/src/grpc/generated/task_access";
import { checkTaskAccess } from "../apps/media-service/src/task-client";
import { checkTaskAccessGrpc, closeTaskAccessGrpcClient } from "../apps/workflow-service/src/task-client";

const originalFetch = globalThis.fetch;
test("HTTP Task Service client validates and returns an access decision", async () => {
  process.env.TASK_SERVICE_URL = "http://task-service";
  globalThis.fetch = async () => new Response(JSON.stringify({ exists: true, allowed: false }), { status: 200 });
  try { assert.deepEqual(await checkTaskAccess({ taskId: "t1", userId: "u1", role: "USER", action: "attachment" }), { exists: true, allowed: false }); }
  finally { globalThis.fetch = originalFetch; }
});

test("HTTP Task Service client rejects malformed responses", async () => {
  process.env.TASK_SERVICE_URL = "http://task-service";
  globalThis.fetch = async () => new Response(JSON.stringify({ allowed: true }), { status: 200 });
  try { await assert.rejects(checkTaskAccess({ taskId: "t1", userId: "u1", role: "USER", action: "attachment" })); }
  finally { globalThis.fetch = originalFetch; }
});

test("generated gRPC client calls the protobuf service and receives access decisions", async () => {
  process.env.GATEWAY_SECRET = "test-secret";
  const server = new Server();
  let attempts = 0;
  const service: TaskAccessServiceServer = {
    checkTaskAccess(call, callback) {
      attempts++;
      if (call.metadata.get("x-gateway-secret")[0] !== "test-secret") {
        callback({ code: 16, message: "unauthenticated" });
        return;
      }
      if (attempts === 1) {
        callback({ code: 14, message: "temporarily unavailable" });
        return;
      }
      callback(null, CheckTaskAccessResponse.create({ exists: true, allowed: call.request.userId === "owner" || call.request.role === "ADMIN" }));
    },
  };
  server.addService(TaskAccessServiceService, service);
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (err, boundPort) => err ? reject(err) : resolve(boundPort));
  });
  process.env.TASK_GRPC_URL = `127.0.0.1:${port}`;
  try {
    assert.deepEqual(await checkTaskAccessGrpc({ taskId: "t1", userId: "stranger", role: "USER", action: "workflow" }), { exists: true, allowed: false });
    assert.equal(attempts, 2);
  } finally {
    closeTaskAccessGrpcClient();
    await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
  }
});
