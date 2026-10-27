import { Request, Response, NextFunction } from "express";
import { AppError } from "./AppError";
import { logger } from "../logger/logger";
import { CircuitOpenError } from "../resilience/circuitBreaker";

export function errorHandler(
  err: unknown,
  req: Request,
  res: Response,
  _next: NextFunction,
) {
  if (err instanceof AppError) {
    return res.status(err.statusCode).json({
      //success: false,
      message: err.message,
    });
  }

  if (err instanceof CircuitOpenError) {
    res.setHeader("Retry-After", String(Math.max(1, Math.ceil(err.retryAfterMs / 1_000))));
    return res.status(503).json({ message: "A required service is temporarily unavailable" });
  }

  logger.error({ err, method: req.method, path: req.originalUrl }, "Unhandled request error");

  return res.status(500).json({
    //success: false,
    message: "Internal server error",
  });
}
