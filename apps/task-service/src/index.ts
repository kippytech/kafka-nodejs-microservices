import { config } from "dotenv";
import express from "express";
import { resolve } from "node:path";
import {
  AppError,
  closeHttpServer,
  closePool,
  errorHandler,
  httpLogger,
  listenHttp,
  logger,
  registerShutdown,
  getPool,
  verifyDatabase,
  requireGatewaySecret,
  requireAuthenticatedUser,
  successResponse,
} from "shared";
import taskRoutes from "./routes/task.routes";
import { initKafka } from "./kafka";
import { checkTaskAccess } from "./services/task.services";
import { closeTaskAccessGrpcServer, startTaskAccessGrpcServer } from "./grpc";

config({ path: resolve(process.cwd(), ".env") });
config({ path: resolve(process.cwd(), "../../.env") });

const PORT = process.env.TASK_PORT || 3002;

const app = express();

app.use(httpLogger);
app.use(express.json());

app.get("/health", (_req, res) => {
  successResponse(res, { service: "task-service" });
});

// mount routes here

app.use("/tasks", requireGatewaySecret, requireAuthenticatedUser, taskRoutes);

app.get("/internal/tasks/:id/access", requireGatewaySecret, async (req, res, next) => {
  try {
    const userId = req.header("x-user-id");
    const role = req.header("x-user-role");
    if (!userId || (role !== "USER" && role !== "ADMIN")) throw new AppError(401, "Missing or invalid user identity");
    res.json(await checkTaskAccess(String(req.params.id), userId, role));
  } catch (err) { next(err); }
});

app.use((_req, _res, next) => {
  next(new AppError(404, "Route not found"));
});

app.use(errorHandler);

async function initStartUp() {
  let grpcServer: Awaited<ReturnType<typeof startTaskAccessGrpcServer>> | undefined;
  let closeKafka: (() => Promise<void>) | undefined;
  try {
    await verifyDatabase(getPool("TASK_DATABASE_URL"), "task-service");
    grpcServer = await startTaskAccessGrpcServer();
    closeKafka = await initKafka();
    const server = listenHttp(app, PORT, "task-service");
    registerShutdown("task-service", [
      () => closeHttpServer(server),
      () => closeTaskAccessGrpcServer(grpcServer!),
      closeKafka,
      closePool,
    ]);
  } catch (err) {
    logger.fatal({ err }, "Task service startup failed");
    if (closeKafka) await closeKafka().catch((closeErr) => logger.error({ err: closeErr }, "Failed to close Kafka during startup cleanup"));
    if (grpcServer) await closeTaskAccessGrpcServer(grpcServer).catch((closeErr) => logger.error({ err: closeErr }, "Failed to close gRPC server during startup cleanup"));
    await closePool().catch((closeErr) => logger.error({ err: closeErr }, "Failed to close database pool during startup cleanup"));
    process.exitCode = 1;
  }
}

void initStartUp();
