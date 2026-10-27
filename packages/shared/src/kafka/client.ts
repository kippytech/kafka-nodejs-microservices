import { Kafka, logLevel, type KafkaConfig } from "kafkajs";

export function createKafkaClient(
  clientId: string,
  config: Partial<KafkaConfig> = {},
) {
  const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0) //.filter(Boolean);

  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS are empty");
  }

  const defaultRetry = {
    retries: Number(process.env.KAFKA_RETRY_ATTEMPTS ?? 8),
    initialRetryTime: 300,
    maxRetryTime: 10_000,
  };

  return new Kafka({
    clientId,
    brokers,
    logLevel: logLevel.ERROR,
    connectionTimeout: Number(process.env.KAFKA_CONNECTION_TIMEOUT_MS ?? 5_000),
    requestTimeout: Number(process.env.KAFKA_REQUEST_TIMEOUT_MS ?? 30_000),
    enforceRequestTimeout: true,
    ...config,
    retry: { ...defaultRetry, ...config.retry },
  });
}
