import type { Producer, RecordMetadata } from "kafkajs";
import { createKafkaClient } from "./client";
import { logger } from "../logger/logger";

export async function createProducer(clientId: string): Promise<Producer> {
  const kafka = createKafkaClient(clientId);

  // responsible for sending records/data to the kafka topics
  const producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 5,
  });

  try {
    await producer.connect();
  } catch (err) {
    await producer.disconnect().catch(() => undefined);
    throw err;
  }

  logger.info({ clientId }, "kafka producer connected");

  return producer;
}

export async function publishJson(
  producer: Producer,
  topic: string,
  payload: Record<string, unknown>,
  key?: string,
): Promise<RecordMetadata[]> {
  const result = await producer.send({
    topic,
    acks: -1, // Wait for all replicas to acknowledge
    messages: [
      {
        key: key ?? null,
        value: JSON.stringify(payload),
      },
    ],
  });

  logger.info({ topic, payload }, "Kafka event published");

  return result;
}

export async function publishEvent(
  producer: Producer,
  topic: string,
  payload: Record<string, unknown>,
  key?: string,
): Promise<void> {
  await publishJson(producer, topic, payload, key);
}
