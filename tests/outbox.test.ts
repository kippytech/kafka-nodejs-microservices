import test from "node:test";
import assert from "node:assert/strict";
import type { Pool } from "pg";
import type { Producer } from "kafkajs";
import { processOutboxBatch } from "../packages/shared/src/kafka/outbox";

type Event = { id: string; event_type: string; payload: Record<string, unknown>; retry_count: number };
function setup(events: Event[], send: () => Promise<unknown>) {
  const statements: Array<{ sql: string; values?: unknown[] }> = [];
  const pool = { query: async (sql: string, values?: unknown[]) => {
    statements.push({ sql, values });
    if (sql.includes("WITH eligible")) return { rows: events, rowCount: events.length };
    return { rows: [], rowCount: 1 };
  } } as unknown as Pool;
  const producer = { send: send } as unknown as Producer;
  return { pool, producer, statements };
}

test("outbox publishes claimed events and marks them published", async () => {
  const { pool, producer, statements } = setup([{ id: "event-1", event_type: "task.created", payload: { taskId: "task-1" }, retry_count: 0 }], async () => []);
  assert.equal(await processOutboxBatch(pool, producer, "task.events"), 1);
  assert.match(statements[0].sql, /FOR UPDATE SKIP LOCKED/);
  assert.match(statements[1].sql, /status='published'/);
});

test("outbox publish failures schedule a retry with backoff", async () => {
  const { pool, producer, statements } = setup([{ id: "event-2", event_type: "task.created", payload: {}, retry_count: 1 }], async () => { throw new Error("broker unavailable"); });
  await processOutboxBatch(pool, producer, "task.events", { maxRetries: 4 });
  assert.match(statements[1].sql, /status=\$2/);
  assert.deepEqual(statements[1].values?.slice(1, 4), ["pending", 2, "broker unavailable"]);
  assert.ok(Number(statements[1].values?.[4]) >= 1_500);
  assert.ok(Number(statements[1].values?.[4]) <= 2_500);
});

test("outbox marks an event failed after the configured retry maximum", async () => {
  const { pool, producer, statements } = setup([{ id: "event-3", event_type: "task.created", payload: {}, retry_count: 2 }], async () => { throw new Error("broker unavailable"); });
  await processOutboxBatch(pool, producer, "task.events", { maxRetries: 3 });
  assert.equal(statements[1].values?.[1], "failed");
  assert.equal(statements[1].values?.[2], 3);
});

test("outbox does not publish when no event was claimed", async () => {
  const { pool, producer } = setup([], async () => { throw new Error("should not publish"); });
  assert.equal(await processOutboxBatch(pool, producer, "task.events"), 0);
});
