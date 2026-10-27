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
import attachmentRoutes from "./routes/media.routes";
import { initKafka } from "./kafka";

config({ path: resolve(process.cwd(), ".env") });
config({ path: resolve(process.cwd(), "../../.env") });

const PORT = process.env.MEDIA_PORT || 3003;

const app = express();

app.use(httpLogger);

app.get("/health", (_req, res) => {
  successResponse(res, { service: "media-service" });
});

app.use("/tasks", requireGatewaySecret, requireAuthenticatedUser, attachmentRoutes);

app.use((_req, _res, next) => {
  next(new AppError(404, "Route not found"));
});

app.use(errorHandler);

async function initStartUp() {
  let closeKafka: (() => Promise<void>) | undefined;
  try {
    await verifyDatabase(getPool("MEDIA_DATABASE_URL"), "media-service");
    closeKafka = await initKafka();
    const server = listenHttp(app, PORT, "media-service");
    registerShutdown("media-service", [() => closeHttpServer(server), closeKafka, closePool]);
  } catch (err) {
    logger.fatal({ err }, "Media service startup failed");
    if (closeKafka) await closeKafka().catch((closeErr) => logger.error({ err: closeErr }, "Failed to close Kafka during startup cleanup"));
    await closePool().catch((closeErr) => logger.error({ err: closeErr }, "Failed to close database pool during startup cleanup"));
    process.exitCode = 1;
  }
}

void initStartUp();
