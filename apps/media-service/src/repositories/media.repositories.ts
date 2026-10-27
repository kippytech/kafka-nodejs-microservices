import { getPool } from "shared";
import { Attachment } from "../utils/types";

export async function createAttachment(input: {
  taskId: string;
  imageUrl: string;
  publicId: string;
  uploadedBy: string;
}): Promise<Attachment> {
  const result = await getPool("MEDIA_DATABASE_URL").query<Attachment>(
    `
     INSERT INTO attachments (task_id, image_url, public_id, uploaded_by)
     VALUES ($1, $2, $3, $4)
     RETURNING id, task_id, image_url, public_id, uploaded_by, created_at
     `,
    [input.taskId, input.imageUrl, input.publicId, input.uploadedBy],
  );

  return result.rows[0];
}

export async function createAttachmentWithEvent(input: {
  taskId: string;
  imageUrl: string;
  publicId: string;
  uploadedBy: string;
}): Promise<Attachment> {
  const pool = getPool("MEDIA_DATABASE_URL");
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const attachmentResult = await client.query<Attachment>(
      `
        INSERT INTO attachments (task_id, image_url, public_id, uploaded_by)
        VALUES ($1, $2, $3, $4)
        RETURNING id, task_id, image_url, public_id, uploaded_by, created_at
      `,
      [input.taskId, input.imageUrl, input.publicId, input.uploadedBy],
    );

    const attachment = attachmentResult.rows[0];

    await client.query(
      `
        INSERT INTO outbox_events (
          event_type,
          payload
        )
        VALUES ($1, $2)
      `,
      ["attachment.uploaded", JSON.stringify({ ...attachment, eventType: "attachment.uploaded" })],
    );

    await client.query("COMMIT");

    return attachment;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function listByTaskId(taskId: string): Promise<Attachment[]> {
  const result = await getPool("MEDIA_DATABASE_URL").query<Attachment>(
    `
        SELECT id, task_id, image_url, public_id, uploaded_by, created_at
        FROM attachments
        WHERE task_id = $1
        ORDER BY created_at DESC
        
        `,
    [taskId],
  );

  return result.rows;
}
