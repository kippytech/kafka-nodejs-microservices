import { credentials, Metadata, status } from "@grpc/grpc-js";
import { CircuitBreaker, CircuitOpenError, logger, TaskAccessServiceClient } from "shared";

let client: InstanceType<typeof TaskAccessServiceClient> | undefined;
let taskServiceBreaker: CircuitBreaker | undefined;

function getTaskServiceBreaker() {
  taskServiceBreaker ??= new CircuitBreaker({
    name: "workflow-to-task-grpc",
    failureThreshold: Number(process.env.CIRCUIT_BREAKER_FAILURE_THRESHOLD ?? 5),
    openMs: Number(process.env.CIRCUIT_BREAKER_OPEN_MS ?? 10_000),
    onStateChange: (previous, state) => (state === "open" ? logger.warn : logger.info)({ dependency: "task-service-grpc", previous, state }, "Service circuit breaker state changed"),
  });
  return taskServiceBreaker;
}

function getClient() {
  if (!client) {
    const address = process.env.TASK_GRPC_URL;
    if (!address) throw new Error("TASK_GRPC_URL is not set");
    client = new TaskAccessServiceClient(address, credentials.createInsecure());
  }
  return client;
}

export function closeTaskAccessGrpcClient() {
  client?.close();
  client = undefined;
}

export async function checkTaskAccessGrpc(input: { taskId: string; userId: string; role: string; action: string }): Promise<{ exists: boolean; allowed: boolean }> {
  try {
    return await getTaskServiceBreaker().execute(() => checkTaskAccessGrpcWithRetry(input), {
      shouldTrip: (error) => typeof error !== "object" || error === null || !("code" in error) ||
        error.code === status.UNAVAILABLE || error.code === status.DEADLINE_EXCEEDED || error.code === status.INTERNAL,
    });
  } catch (err) {
    if (!(err instanceof CircuitOpenError)) logger.error({ err, taskId: input.taskId }, "Task Service gRPC authorization failed");
    throw err;
  }
}

async function checkTaskAccessGrpcWithRetry(input: { taskId: string; userId: string; role: string; action: string }): Promise<{ exists: boolean; allowed: boolean }> {
  const metadata = new Metadata();
  metadata.set("x-gateway-secret", process.env.GATEWAY_SECRET ?? "");
  const configuredTimeoutMs = Number(process.env.TASK_GRPC_TIMEOUT_MS ?? 3_000);
  const timeoutMs = Number.isFinite(configuredTimeoutMs) && configuredTimeoutMs > 0 ? configuredTimeoutMs : 3_000;
  const configuredRetries = Number(process.env.TASK_GRPC_RETRIES ?? 1);
  const retries = Number.isFinite(configuredRetries) ? Math.max(0, Math.floor(configuredRetries)) : 1;
  const maxAttempts = 1 + retries;
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const response = await new Promise<{ exists: boolean; allowed: boolean }>((resolve, reject) => {
        getClient().checkTaskAccess(
          { taskId: input.taskId, userId: input.userId, role: input.role, action: input.action },
          metadata,
          { deadline: Math.min(deadline, Date.now() + Math.ceil(timeoutMs / maxAttempts)) },
          (err, result) => {
            if (err) { reject(err); return; }
            if (typeof result?.exists !== "boolean" || typeof result?.allowed !== "boolean") {
              reject(new Error("Task Service gRPC returned an invalid response"));
              return;
            }
            resolve({ exists: result.exists, allowed: result.allowed });
          },
        );
      });
      return response;
    } catch (err) {
      lastError = err;
      const retryable = typeof err === "object" && err !== null && "code" in err &&
        (err.code === status.UNAVAILABLE || err.code === status.DEADLINE_EXCEEDED);
      if (!retryable || attempt + 1 >= maxAttempts || Date.now() >= deadline) break;
      await new Promise((resolve) => setTimeout(resolve, Math.min(100 + Math.floor(Math.random() * 50), Math.max(0, deadline - Date.now()))));
    }
  }
  throw lastError ?? new Error("Task Service gRPC authorization failed");
}
