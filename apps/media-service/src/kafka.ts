import { createProducer, getPool, logger, runOutboxWorker, TOPICS } from "shared";

export async function initKafka(): Promise<() => Promise<void>> {
  const pool = getPool("MEDIA_DATABASE_URL");
  const producer = await createProducer("media-service-outbox");
  const controller = new AbortController();
  const worker = runOutboxWorker(pool, producer, TOPICS.MEDIA_EVENTS, { signal: controller.signal })
    .catch((err) => {
      logger.fatal({ err }, "Media outbox worker stopped unexpectedly");
      process.kill(process.pid, "SIGTERM");
    });
  return async () => { controller.abort(); await worker; await producer.disconnect(); };
}
