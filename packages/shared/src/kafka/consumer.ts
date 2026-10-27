import type { Consumer } from "kafkajs";
import { createKafkaClient } from "./client";
import { logger } from "../logger/logger";

export async function createConsumer(
  clientId: string,
  groupId: string,
): Promise<Consumer> {
  const kafka = createKafkaClient(clientId);

  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: Number(process.env.KAFKA_SESSION_TIMEOUT_MS ?? 30_000),
    heartbeatInterval: Number(process.env.KAFKA_HEARTBEAT_INTERVAL_MS ?? 3_000),
    rebalanceTimeout: Number(process.env.KAFKA_REBALANCE_TIMEOUT_MS ?? 60_000),
  });

  try {
    await consumer.connect();
  } catch (err) {
    await consumer.disconnect().catch(() => undefined);
    throw err;
  }

  logger.info({ clientId, groupId }, "kafka consumer connected");

  return consumer;
}

export type KafkaEvent = {
    topic: string;
    key: string | null;
    value: string | null;
    partition: number;
    offset: string;
  };

export async function runConsumer(
  consumer: Consumer,
  topics: string[],
  handler: (event: KafkaEvent) => Promise<void>,
  options?: { fromBeginning?: boolean },
): Promise<void> {
  await consumer.subscribe({
    topics,
    fromBeginning: options?.fromBeginning ?? false,
  });

  await consumer.run({
    eachMessage: async (payload) => {
      const { topic, partition, message } = payload;

      logger.info(
        {
          topic,
          partition,
          offset: message.offset,
          key: message?.key?.toString(),
        },
        "Kafka messages received",
      );

      await handler({
        topic,
        partition,
        key: message.key?.toString() ?? null,
        value: message.value?.toString() ?? null,
        offset: message.offset,
      });

      // await consumer.run({
      //   eachMessage: async ({ topic, partition, message }) => {
      //     await handler({
      //       topic,
      //       partition,
      //       key: message.key?.toString() ?? null,
      //       value: message.value
      //         ? JSON.parse(message.value.toString())
      //         : null,
      //       offset: message.offset,
      //     });
      //   },
      // });
    },
  });
}
