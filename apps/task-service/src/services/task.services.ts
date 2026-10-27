import { CreateTaskInput } from "../schemas/task.schemas";
import * as taskRepo from "../repositories/task.repository";
import { convertToPublicTask } from "../utils/task.utils";
import { AppError } from "shared";

export async function createTask(input: CreateTaskInput, userId: string) {
  const newlyCreatedTask = await taskRepo.createTaskWithEvent({
    title: input.title,
    createdBy: userId,
  });

  return convertToPublicTask(newlyCreatedTask);
}

export async function listTasks(userId: string, role: string) {
  if (!userId || !role) {
    throw new AppError(401, "Missing user identity");
  }

  const tasks = await taskRepo.listTasks({ userId, role });

  return tasks.map(convertToPublicTask);
}

export async function getSingleTask(id: string, userId: string, role: string) {
  const task = await taskRepo.findSingleTaskById(id);

  if (!task) {
    throw new AppError(404, "Task not found");
  }

  if (role !== "ADMIN" && task.created_by !== userId) {
    throw new AppError(403, "forbidden");
  }

  return convertToPublicTask(task);
}

export async function checkTaskAccess(id: string, userId: string, role: string) {
  const task = await taskRepo.findSingleTaskById(id);
  return { exists: Boolean(task), allowed: Boolean(task && (role === "ADMIN" || task.created_by === userId)) };
}

export async function deleteSingleTask(id: string, role: string) {
  if (role !== "ADMIN") {
    throw new AppError(403, "Forbidden");
  }

  // const task = await taskRepo.findSingleTaskById(id);

  // if (!task) {
  //   throw new AppError(404, "Task not found");
  // }

  const deletedId = await taskRepo.deleteSingleTaskById(id);

  if (!deletedId) {
    throw new AppError(404, "Task not found");
  }

  return { id: deletedId };
}

// async function assertTaskAccess(
//   taskId: string,
//   userId: string,
//   role: string,
// ) {
//   const access = await taskServiceClient.checkTaskAccess({
//     taskId,
//     userId,
//     role,
//   });

//   if (!access.exists) {
//     throw new AppError(404, "Task not found");
//   }

//   if (!access.allowed) {
//     throw new AppError(403, "Forbidden");
//   }
// }
