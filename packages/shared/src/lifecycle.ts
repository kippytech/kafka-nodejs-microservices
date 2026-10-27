import { createServer, type Server } from "node:http";
import type { Express } from "express";
import { logger } from "./logger/logger";

export function listenHttp(app: Express, port: string | number, service: string): Server {
  const server = createServer(app);
  server.requestTimeout = Number(process.env.HTTP_REQUEST_TIMEOUT_MS ?? 30_000);
  server.headersTimeout = Number(process.env.HTTP_HEADERS_TIMEOUT_MS ?? 15_000);
  server.keepAliveTimeout = Number(process.env.HTTP_KEEPALIVE_TIMEOUT_MS ?? 5_000);
  server.maxRequestsPerSocket = 1_000;
  server.listen(port, () => logger.info({ service, port }, "HTTP server listening"));
  return server;
}

export function closeHttpServer(server: Server, graceMs = 10_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const forceClose = setTimeout(() => server.closeAllConnections(), graceMs);
    forceClose.unref();
    server.close((err) => {
      clearTimeout(forceClose);
      if (err) reject(err);
      else resolve();
    });
    server.closeIdleConnections();
  });
}

export function registerShutdown(service: string, closeSteps: Array<() => Promise<void>>): (signal?: string) => Promise<void> {
  let shuttingDown: Promise<void> | undefined;
  const shutdown = (signal = "manual") => {
    if (shuttingDown) return shuttingDown;
    shuttingDown = (async () => {
      logger.info({ service, signal }, "Graceful shutdown started");
      const deadline = setTimeout(() => {
        logger.fatal({ service }, "Graceful shutdown timed out");
        process.exit(1);
      }, Number(process.env.SHUTDOWN_TIMEOUT_MS ?? 60_000));
      deadline.unref();
      for (const close of closeSteps) {
        try { await close(); }
        catch (err) {
          process.exitCode = 1;
          logger.error({ err, service }, "Shutdown step failed");
        }
      }
      clearTimeout(deadline);
      logger.info({ service }, "Graceful shutdown complete");
    })();
    return shuttingDown;
  };

  process.once("SIGTERM", () => { void shutdown("SIGTERM"); });
  process.once("SIGINT", () => { void shutdown("SIGINT"); });
  return shutdown;
}
