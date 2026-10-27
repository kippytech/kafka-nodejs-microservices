import type { Pool } from "pg";
import type { Producer } from "kafkajs";
import { publishEvent } from "./producer";
import { logger } from "../logger/logger";

export async function processOutboxBatch(pool: Pool, producer: Producer, topic: string, options: { maxRetries?: number; batchSize?: number; signal?: AbortSignal } = {}): Promise<number> {
  const maxRetries = options.maxRetries ?? Number(process.env.OUTBOX_MAX_RETRIES ?? 8);
  const batchSize = options.batchSize ?? 20;
  const staleLockMs = Number(process.env.OUTBOX_LOCK_TIMEOUT_MS ?? 120_000);
  const claimed = await pool.query<{ id: string; event_type: string; payload: Record<string, unknown>; retry_count: number }>(`
      WITH eligible AS (
        SELECT id FROM outbox_events
        WHERE (status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= now()))
          OR (status = 'processing' AND locked_at < now() - ($2 * interval '1 millisecond'))
        ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED
      )
      UPDATE outbox_events o SET status = 'processing', locked_at = now(), updated_at = now()
      FROM eligible WHERE o.id = eligible.id
      RETURNING o.id, o.event_type, o.payload, o.retry_count`, [batchSize, staleLockMs]);
  if (claimed.rowCount === 0) return 0;
  for (let index = 0; index < claimed.rows.length; index++) {
    if (options.signal?.aborted) {
      const unprocessedIds = claimed.rows.slice(index).map((event) => event.id);
      await pool.query("UPDATE outbox_events SET status='pending', locked_at=NULL, updated_at=now() WHERE id=ANY($1::uuid[])", [unprocessedIds]);
      break;
    }
    const event = claimed.rows[index];
      try {
        await publishEvent(producer, topic, { ...event.payload, eventId: event.id, eventType: event.event_type }, String(event.payload.taskId ?? event.id));
        await pool.query("UPDATE outbox_events SET status='published', published_at=now(), locked_at=NULL, updated_at=now() WHERE id=$1", [event.id]);
      } catch (err) {
        const retryCount = event.retry_count + 1;
        const status = retryCount >= maxRetries ? "failed" : "pending";
        const baseDelayMs = Number(process.env.OUTBOX_RETRY_BASE_MS ?? 1_000);
        const maxDelayMs = Number(process.env.OUTBOX_RETRY_MAX_MS ?? 3_600_000);
        const exponentialDelay = Math.min(maxDelayMs, baseDelayMs * 2 ** Math.min(retryCount - 1, 12));
        const retryDelayMs = Math.floor(exponentialDelay * (0.75 + Math.random() * 0.5));
        await pool.query("UPDATE outbox_events SET status=$2, retry_count=$3, last_error=$4, next_retry_at=CASE WHEN $2='pending' THEN now() + ($5 * interval '1 millisecond') ELSE NULL END, locked_at=NULL, updated_at=now() WHERE id=$1", [event.id, status, retryCount, err instanceof Error ? err.message : String(err), retryDelayMs]);
        logger.error({ err, eventId: event.id, status, retryCount }, "Outbox event publish failed");
      }
  }
  return claimed.rows.length;
}

export async function runOutboxWorker(pool: Pool, producer: Producer, topic: string, options: { maxRetries?: number; pollMs?: number; batchSize?: number; signal?: AbortSignal } = {}) {
  const pollMs = options.pollMs ?? 1000;
  while (!options.signal?.aborted) {
    const claimedCount = await processOutboxBatch(pool, producer, topic, options);
    if (claimedCount === 0 && !options.signal?.aborted) {
      await new Promise<void>((resolve) => {
        const cleanup = () => options.signal?.removeEventListener("abort", onAbort);
        const timer = setTimeout(() => { cleanup(); resolve(); }, pollMs);
        const onAbort = () => { clearTimeout(timer); cleanup(); resolve(); };
        options.signal?.addEventListener("abort", onAbort, { once: true });
      });
    }
  }
}


// runOutboxWorker()
//        │
//        ↓
// processOutboxBatch()
//        │
//        ↓
// claim eligible rows
//        │
//        ├── none ──→ return 0
//        │              ↓
//        │           sleep 1s
//        │              ↓
//        │           try again
//        │
//        └── rows
//             ↓
//        for each event
//             │
//             ├── cancelled → release remaining → stop batch
//             │
//             ├── publish succeeds
//             │       ↓
//             │    published
//             │
//             └── publish fails
//                     ↓
//               increment retry
//                     ↓
//               pending or failed
//                     ↓
//               continue next event