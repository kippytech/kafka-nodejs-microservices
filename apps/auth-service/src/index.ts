import { config } from "dotenv";
import express from "express";
import { resolve } from "node:path";
import {
  AppError,
  errorHandler,
  httpLogger,
  closeHttpServer,
  closePool,
  listenHttp,
  logger,
  registerShutdown,
  getPool,
  verifyDatabase,
  requireGatewaySecret,
  successResponse,
} from "shared";
import authRoutes from "./routes/auth.routes";

config({ path: resolve(process.cwd(), ".env") });
config({ path: resolve(process.cwd(), "../../.env") });

const PORT = process.env.AUTH_PORT || 3001;

const app = express();

app.use(httpLogger);
app.use(express.json());

app.get("/health", (_req, res) => {
  successResponse(res, { service: "auth-service" });
});

app.use("/auth", requireGatewaySecret, authRoutes);

app.use((_req, _res, next) => {
  next(new AppError(404, "Route not found"));
});

app.use(errorHandler);

async function start() {
  try {
    await verifyDatabase(getPool("USER_DATABASE_URL"), "auth-service");
    const server = listenHttp(app, PORT, "auth-service");
    registerShutdown("auth-service", [() => closeHttpServer(server), closePool]);
  } catch (err) {
    logger.fatal({ err }, "Auth service startup failed");
    await closePool().catch((closeErr) => logger.error({ err: closeErr }, "Failed to close database pool during startup cleanup"));
    process.exitCode = 1;
  }
}

void start();
