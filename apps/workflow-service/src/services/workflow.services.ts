import { AppError, createConsumer, logger, runConsumer, TOPICS } from "shared";
import { DomainEvent } from "../utils/types";
import * as worflowRepo from "../repository/workflow.repositories";
import { convertToPublicWorkflow } from "../utils/workflow.utils";
import { checkTaskAccessGrpc } from "../task-client";

async function handleDomainEvent(rawData: DomainEvent) {
  if (!rawData.eventType || !rawData.taskId || !rawData.userId) {
    logger.warn({ rawData }, "invalid domain event");
    return;
  }

  const workflow = await worflowRepo.createWorkflow({
    sourceEventId: rawData.eventId,
    taskId: rawData.taskId,
    eventType: rawData.eventType,
    message: rawData.message || rawData.eventType,
    createdBy: rawData.userId,
  });

  logger.info(
    {
      workflowId: workflow.id,
      eventType: workflow.event_type,
    },
    "workflow row created",
  );
}

export async function startKafka(): Promise<() => Promise<void>> {
  const consumer = await createConsumer(
    "workflow-service",
    "workflow-service-group",
  );
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    if (payload.restart) {
      logger.warn({ err: payload.error }, "Kafka consumer crashed; KafkaJS will retry");
    } else {
      logger.fatal({ err: payload.error }, "Kafka consumer cannot restart");
      process.kill(process.pid, "SIGTERM");
    }
  });

  // run consumer
  const runTask = runConsumer(
    consumer,
    [TOPICS.TASK_EVENTS, TOPICS.MEDIA_EVENTS],
    async (message) => {
      const value = message.value;
      if (!value) return;

      let parsed: unknown;
      try {
        parsed = JSON.parse(value);
      } catch (err) {
        logger.warn({ err, topic: message.topic, offset: message.offset }, "malformed domain event discarded");
        return;
      }
      if (!isDomainEvent(parsed)) { logger.warn({ topic: message.topic, offset: message.offset }, "invalid domain event discarded"); return; }
      await handleDomainEvent(parsed);
    },
  ).catch((err: Error) => {
    logger.fatal({ err }, "Kafka consumer stopped unexpectedly");
    process.kill(process.pid, "SIGTERM");
  });
  return async () => {
    try { await consumer.stop(); }
    finally {
      await consumer.disconnect();
      await runTask;
    }
  };
}

function isDomainEvent(value: unknown): value is DomainEvent {
  if (!value || typeof value !== "object") return false;
  const event = value as Record<string, unknown>;
  return typeof event.eventType === "string" && typeof event.taskId === "string" && typeof event.userId === "string" &&
    (event.eventId === undefined || (typeof event.eventId === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(event.eventId)));
}

export async function listWorkflowsByTask(
  taskId: string,
  userId: string,
  role: string,
) {
  const access = await checkTaskAccessGrpc({ taskId, userId, role, action: "workflow" });
  if (!access.exists) throw new AppError(404, "Task not found");
  if (!access.allowed) throw new AppError(403, "forbidden");

  const rows = await worflowRepo.listWorkflowsByTaskId(taskId);

  return rows.map(convertToPublicWorkflow);
}
