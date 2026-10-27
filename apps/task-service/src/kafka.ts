import { createProducer, getPool, logger, runOutboxWorker, TOPICS } from "shared";

export async function initKafka(): Promise<() => Promise<void>> {
  const pool = getPool("TASK_DATABASE_URL");
  const producer = await createProducer("task-service-outbox");
  const controller = new AbortController();
  const worker = runOutboxWorker(pool, producer, TOPICS.TASK_EVENTS, { signal: controller.signal })
    .catch((err) => {
      logger.fatal({ err }, "Task outbox worker stopped unexpectedly");
      process.kill(process.pid, "SIGTERM");
    });
  return async () => { controller.abort(); await worker; await producer.disconnect(); };
}
