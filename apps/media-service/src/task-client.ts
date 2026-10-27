import { CircuitBreaker, CircuitOpenError, logger, requestWithRetry } from "shared";

type AccessInput = { taskId: string; userId: string; role: string; action: string };
type Access = { exists: boolean; allowed: boolean };
let taskServiceBreaker: CircuitBreaker | undefined;

function getTaskServiceBreaker() {
  taskServiceBreaker ??= new CircuitBreaker({
    name: "media-to-task-http",
    failureThreshold: Number(process.env.CIRCUIT_BREAKER_FAILURE_THRESHOLD ?? 5),
    openMs: Number(process.env.CIRCUIT_BREAKER_OPEN_MS ?? 10_000),
    onStateChange: (previous, state) => (state === "open" ? logger.warn : logger.info)({ dependency: "task-service-http", previous, state }, "Service circuit breaker state changed"),
  });
  return taskServiceBreaker;
}

class UpstreamHttpError extends Error {
  constructor(readonly status: number) { super(`Task access API returned invalid response (${status})`); }
}

export async function checkTaskAccess(input: AccessInput): Promise<Access> {
  const baseUrl = process.env.TASK_SERVICE_URL;
  if (!baseUrl) throw new Error("TASK_SERVICE_URL is not set");
  try {
    return await getTaskServiceBreaker().execute(async () => {
      const response = await requestWithRetry(`${baseUrl}/internal/tasks/${encodeURIComponent(input.taskId)}/access?action=${encodeURIComponent(input.action)}`, {
        headers: { "x-user-id": input.userId, "x-user-role": input.role, "x-gateway-secret": process.env.GATEWAY_SECRET ?? "" },
      }, { timeoutMs: Number(process.env.HTTP_CLIENT_TIMEOUT_MS ?? 1_500), retries: 1 });
      const body: unknown = await response.json();
      if (!body || typeof body !== "object" || !("exists" in body) || !("allowed" in body) || typeof body.exists !== "boolean" || typeof body.allowed !== "boolean") {
        throw new Error(`Task access API returned invalid response (${response.status})`);
      }
      if (!response.ok) throw new UpstreamHttpError(response.status);
      return { exists: body.exists, allowed: body.allowed };
    }, {
      shouldTrip: (error) => !(error instanceof UpstreamHttpError && error.status < 500),
    });
  } catch (err) {
    if (!(err instanceof CircuitOpenError)) logger.error({ err, taskId: input.taskId }, "Task Service authorization request failed");
    throw err;
  }
}
