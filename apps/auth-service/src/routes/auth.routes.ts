import { Router } from "express";
import { requireAuthenticatedUser, validateBody } from "shared";
import { loginSchema, registerSchema } from "../schemas/auth.schemas";
import * as authController from "../controllers/auth.controller";

const router = Router();

router.post("/register", validateBody(registerSchema), authController.register);
router.post("/login", validateBody(loginSchema), authController.login);
router.get("/me", requireAuthenticatedUser, authController.getMe);

export default router;
