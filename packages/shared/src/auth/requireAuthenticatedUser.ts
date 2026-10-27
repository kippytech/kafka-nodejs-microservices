import type { NextFunction, Request, Response } from "express";
import { AppError } from "../errors/AppError";
import { verifyToken } from "./jwt";

export function requireAuthenticatedUser(req: Request, _res: Response, next: NextFunction) {
  const authorization = req.header("authorization");
  if (!authorization?.startsWith("Bearer ")) return next(new AppError(401, "Missing or invalid auth token"));

  try {
    const identity = verifyToken(authorization.slice("Bearer ".length).trim());
    req.headers["x-user-id"] = identity.userId;
    req.headers["x-user-role"] = identity.role;
    next();
  } catch {
    next(new AppError(401, "Invalid or expired auth token"));
  }
}
