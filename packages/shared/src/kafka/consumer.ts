import type { Consumer, EachMessagePayload } from "kafkajs";
import { createKafkaClient } from "./client";
import { logger } from "../logger/logger";

export async function createConsumer(
  clientId: string,
  groupId: string,
): Promise<Consumer> {
  const kafka = createKafkaClient(clientId);

  const consumer = kafka.consumer({ groupId });

  await consumer.connect();

  logger.info({ clientId, groupId }, "kafka consumer connected");

  return consumer;
}

export type KafkaEvent = {
    topic: string;
    key: string | null;
    value: unknown;
    partition: number;
    offset: string;
  };

export async function runConsumer(
  consumer: Consumer,
  topics: string[],
  handler: (payload: EachMessagePayload) => Promise<void>, //payload: KafkaEvent
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

      await handler(payload);

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
