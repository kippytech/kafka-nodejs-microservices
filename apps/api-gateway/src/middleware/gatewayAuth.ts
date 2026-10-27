import type { Request, NextFunction, Response } from "express";
import { AppError } from "shared";

const IDENTITY_HEADERS = ["x-user-id", "x-user-role", "x-gateway-secret"] as const;

export function gatewayAuth(req: Request, _res: Response, next: NextFunction) {
  for (const header of IDENTITY_HEADERS) delete req.headers[header];

  const secret = process.env.GATEWAY_SECRET;
  if (!secret) return next(new AppError(500, "GATEWAY_SECRET is not configured"));

  req.headers["x-gateway-secret"] = secret;
  next();
}
