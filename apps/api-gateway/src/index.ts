import { config } from "dotenv";
import { resolve } from "node:path";
import express from "express";
import { ServerResponse, type IncomingMessage } from "node:http";
import cors from "cors";
import helmet from "helmet";
import rateLimit, { MINUTE } from "express-rate-limit";
import {
  AppError,
  errorHandler,
  httpLogger,
  logger,
  closeHttpServer,
  listenHttp,
  registerShutdown,
  successResponse,
  CircuitBreaker,
  type CircuitPermit,
} from "shared";
import { createProxyMiddleware } from "http-proxy-middleware";
import { gatewayAuth } from "./middleware/gatewayAuth";

config({ path: resolve(process.cwd(), ".env") });
config({ path: resolve(process.cwd(), "../../.env") });

const PORT = process.env.PORT || 3000;
const AUTH_SERVICE_URL =
  process.env.AUTH_SERVICE_URL || "http://localhost:3001";

const TASK_SERVICE_URL =
  process.env.TASK_SERVICE_URL || "http://localhost:3002";

const MEDIA_SERVICE_URL =
  process.env.MEDIA_SERVICE_URL || "http://localhost:3003";

const WORKFLOW_SERVICE_URL =
  process.env.WORKFLOW_SERVICE_URL || "http://localhost:3004";

const app = express();
app.disable("x-powered-by");

//secure default http headers
app.use(helmet());
app.use(cors());
app.use(
  rateLimit({
    windowMs: 15 * MINUTE, // SECOND, MINUTE, HOUR, and DAY constants are available, or a use bare number for milliseconds
    limit: 100, // Limit each IP to 100 requests per `window` (here, per 15 minutes).
    standardHeaders: true, // draft-6: `RateLimit-*` headers; draft-7 & draft-8: combined `RateLimit` header
    legacyHeaders: false, // Disable the `X-RateLimit-*` headers.
  }),
);

app.use(httpLogger);

app.use("/health", (_req, res) => {
  successResponse(res, { service: "api-gateway" });
});

function proxy(name: string, target: string, pathRewrite: (path: string) => string) {
  const breaker = new CircuitBreaker({
    name,
    failureThreshold: Number(process.env.CIRCUIT_BREAKER_FAILURE_THRESHOLD ?? 5),
    openMs: Number(process.env.CIRCUIT_BREAKER_OPEN_MS ?? 10_000),
    onStateChange: (previous, state) => (state === "open" ? logger.warn : logger.info)({ dependency: name, previous, state }, "Gateway circuit breaker state changed"),
  });
  const permits = new WeakMap<IncomingMessage, CircuitPermit>();
  const proxyMiddleware = createProxyMiddleware({
    target,
    changeOrigin: true,
    pathRewrite,
    timeout: Number(process.env.HTTP_REQUEST_TIMEOUT_MS ?? 30_000),
    proxyTimeout: Number(process.env.HTTP_PROXY_TIMEOUT_MS ?? 5_000),
    on: {
      proxyRes(proxyRes, req) {
        const permit = permits.get(req);
        if (!permit) return;
        permits.delete(req);
        if ((proxyRes.statusCode ?? 500) >= 500) permit.failure();
        else permit.success();
      },
      error(err, req, res) {
        logger.error({ err, target, path: req.url }, "Upstream service proxy failed");
        const permit = permits.get(req);
        if (permit) {
          permits.delete(req);
          permit.failure();
        }
        if (res instanceof ServerResponse && !res.headersSent) {
          res.writeHead(502, { "content-type": "application/json" });
          res.end(JSON.stringify({ message: "Upstream service unavailable" }));
        } else if (res instanceof ServerResponse && !res.writableEnded) {
          res.end();
        }
      },
    },
  });

  return (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const permit = breaker.acquire();
    if (!permit) {
      const retryAfterMs = breaker.getRetryAfterMs();
      res.setHeader("Retry-After", String(Math.max(1, Math.ceil(retryAfterMs / 1_000))));
      res.status(503).json({ message: "Upstream service temporarily unavailable" });
      return;
    }
    permits.set(req, permit);
    res.once("close", () => {
      if (!res.writableEnded) {
        permits.get(req)?.ignore();
        permits.delete(req);
      }
    });
    proxyMiddleware(req, res, next);
  };
}

app.use("/auth", gatewayAuth, proxy("auth-service", AUTH_SERVICE_URL, (path) => `/auth${path}`));

const taskProxy = proxy("task-service", TASK_SERVICE_URL, (path) => `/tasks${path}`);
const mediaProxy = proxy("media-service", MEDIA_SERVICE_URL, (path) => `/tasks${path}`);
const workflowProxy = proxy("workflow-service", WORKFLOW_SERVICE_URL, (path) => `/tasks${path}`);

app.use("/tasks", gatewayAuth, (req, res, next) => {
  if (req.path.includes("/attachments")) {
    return mediaProxy(req, res, next);
  }

  if (req.path.includes("/workflows")) {
    return workflowProxy(req, res, next);
  }

  return taskProxy(req, res, next);
});

app.use((_req, _res, next) => {
  next(new AppError(404, "Route not found"));
});

app.use(errorHandler);

const server = listenHttp(app, PORT, "api-gateway");
registerShutdown("api-gateway", [() => closeHttpServer(server)]);
