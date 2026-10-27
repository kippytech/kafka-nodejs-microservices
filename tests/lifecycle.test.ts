import test from "node:test";
import assert from "node:assert/strict";
import { registerShutdown } from "../packages/shared/src/lifecycle";

test("shutdown closes resources in order and runs only once", async () => {
  const calls: string[] = [];
  const shutdown = registerShutdown("lifecycle-test", [
    async () => { calls.push("http"); },
    async () => { calls.push("kafka"); },
    async () => { calls.push("database"); },
  ]);
  await shutdown("test");
  await shutdown("again");
  assert.deepEqual(calls, ["http", "kafka", "database"]);
});
