import { pinoHttp } from "pino-http";
import { logger } from "./logger";

export const httpLogger = pinoHttp({
  logger,
  redact: {
    paths: ["req.headers.authorization", "req.headers.cookie", "req.headers.x-gateway-secret"],
    censor: "[REDACTED]",
  },
});
