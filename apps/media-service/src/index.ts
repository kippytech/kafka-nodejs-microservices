import { config } from "dotenv";
import express from "express";
import { resolve } from "node:path";
import {
  AppError,
  errorHandler,
  httpLogger,
  logger,
  requireGatewaySecret,
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

app.use("/tasks", requireGatewaySecret, attachmentRoutes);

app.use((_req, _res, next) => {
  next(new AppError(404, "Route not found"));
});

app.use(errorHandler);

async function initStartUp() {
  try {
    await initKafka();

    app.listen(PORT, () => {
      logger.info(`Media service is now running on port ${PORT}`);
    });
    
  } catch (err) {
    logger.error({ err }, "Media service startup failed");
    process.exit(1);
  }
}

initStartUp();
