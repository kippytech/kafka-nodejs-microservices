import { AppError } from "shared";
import * as attachmentRepo from "../repositories/media.repositories";
import { uploadBuffer } from "../utils/stroage";
import { convertToPublicMediaAttachment } from "../utils/media.utils";
import { checkTaskAccess } from "../task-client";

async function assertTaskAccess(taskId: string, userId: string, role: string) {
  const access = await checkTaskAccess({ taskId, userId, role, action: "attachment" });
  if (!access.exists) throw new AppError(404, "Task not found");
  if (!access.allowed) throw new AppError(403, "Forbidden");
}

export async function uploadAttachment(input: {
  taskId: string;
  userId: string;
  role: string;
  file?: Express.Multer.File;
}) {
  if (!input.file) {
    throw new AppError(400, "Image file is required");
  }

  await assertTaskAccess(input.taskId, input.userId, input.role);

  const uploaded = await uploadBuffer(
    input.file.buffer,
    input.file.mimetype || "image/jpeg",
  );

  const attachment = await attachmentRepo.createAttachmentWithEvent({
    taskId: input.taskId,
    imageUrl: uploaded.imageUrl,
    publicId: uploaded.publicId,
    uploadedBy: input.userId,
  });

  return convertToPublicMediaAttachment(attachment);
}

export async function listAttachments(
  taskId: string,
  userId: string,
  role: string,
) {
  await assertTaskAccess(taskId, userId, role);
  const rows = await attachmentRepo.listByTaskId(taskId);

  return rows.map(convertToPublicMediaAttachment);
}
