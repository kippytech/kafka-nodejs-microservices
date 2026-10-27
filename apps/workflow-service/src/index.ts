import { config } from "dotenv";
import { resolve } from "node:path";
import express from "express";
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
import { startKafka } from "./services/workflow.services";
import workflowRoutes from "./routes/workflow.routes";
import { closeTaskAccessGrpcClient } from "./task-client";

config({ path: resolve(process.cwd(), ".env") });
config({ path: resolve(process.cwd(), "../../.env") });

const PORT = process.env.WORKFLOW_PORT || 3004;

const app = express();

app.use(httpLogger);
app.use(express.json());

app.get("/health", (_req, res) => {
  successResponse(res, { service: "workflow-service" });
});

// mount routes here

app.use(requireGatewaySecret, requireAuthenticatedUser, workflowRoutes);

app.use((_req, _res, next) => {
  next(new AppError(404, "Route not found"));
});

app.use(errorHandler);

async function initStart() {
  let closeKafka: (() => Promise<void>) | undefined;
  try {
    await verifyDatabase(getPool("WORKFLOW_DATABASE_URL"), "workflow-service");
    closeKafka = await startKafka();
    const server = listenHttp(app, PORT, "workflow-service");
    registerShutdown("workflow-service", [
      () => closeHttpServer(server),
      closeKafka,
      async () => closeTaskAccessGrpcClient(),
      closePool,
    ]);
  } catch (err) {
    logger.fatal({ err }, "Workflow service startup failed");
    if (closeKafka) await closeKafka().catch((closeErr) => logger.error({ err: closeErr }, "Failed to close Kafka during startup cleanup"));
    closeTaskAccessGrpcClient();
    await closePool().catch((closeErr) => logger.error({ err: closeErr }, "Failed to close database pool during startup cleanup"));
    process.exitCode = 1;
  }
}

void initStart();
