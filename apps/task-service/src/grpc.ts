import { Server, ServerCredentials, status, type ServerUnaryCall, type sendUnaryData } from "@grpc/grpc-js";
import {
  logger,
  TaskAccessServiceService,
  type GrpcCheckTaskAccessRequest,
  type GrpcCheckTaskAccessResponse,
  type TaskAccessServiceServer,
} from "shared";
import { checkTaskAccess } from "./services/task.services";

const service: TaskAccessServiceServer = {
  async checkTaskAccess(
    call: ServerUnaryCall<GrpcCheckTaskAccessRequest, GrpcCheckTaskAccessResponse>,
    callback: sendUnaryData<GrpcCheckTaskAccessResponse>,
  ) {
    const expectedSecret = process.env.GATEWAY_SECRET;
    if (!expectedSecret || call.metadata.get("x-gateway-secret")[0] !== expectedSecret) {
      callback({ code: status.UNAUTHENTICATED, message: "Invalid service credentials" });
      return;
    }
    const { taskId, userId, role } = call.request;
    if (!taskId || !userId || (role !== "USER" && role !== "ADMIN")) {
      callback({ code: status.INVALID_ARGUMENT, message: "taskId, userId, and a valid role are required" });
      return;
    }
    try {
      callback(null, await checkTaskAccess(taskId, userId, role));
    } catch (err) {
      logger.error({ err, taskId }, "gRPC task authorization failed");
      callback({ code: status.INTERNAL, message: "Task access check failed" });
    }
  },
};

export async function startTaskAccessGrpcServer(): Promise<Server> {
  const server = new Server();
  server.addService(TaskAccessServiceService, service);
  const address = process.env.TASK_GRPC_BIND_ADDRESS ?? "0.0.0.0:50051";
  await new Promise<void>((resolve, reject) => {
    server.bindAsync(address, ServerCredentials.createInsecure(), (err) => err ? reject(err) : resolve());
  });
  logger.info({ address }, "Task Access gRPC server listening");
  return server;
}

export function closeTaskAccessGrpcServer(server: Server): Promise<void> {
  return new Promise((resolve) => {
    const forceClose = setTimeout(() => server.forceShutdown(), 10_000);
    forceClose.unref();
    server.tryShutdown(() => { clearTimeout(forceClose); resolve(); });
  });
}
