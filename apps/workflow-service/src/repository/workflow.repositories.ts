import { getPool } from "shared";
import { Workflow } from "../utils/types";

export async function createWorkflow(input: {
  sourceEventId?: string;
  taskId: string;
  eventType: string;
  message: string;
  createdBy: string;
}): Promise<Workflow> {
  const result = await getPool("WORKFLOW_DATABASE_URL").query<Workflow>(
    `
        INSERT INTO task_workflows (task_id, event_type, message, created_by, source_event_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (source_event_id) WHERE source_event_id IS NOT NULL DO UPDATE SET source_event_id = EXCLUDED.source_event_id
        RETURNING id, task_id, event_type, message, created_by, created_at
        
        `,
    [input.taskId, input.eventType, input.message, input.createdBy, input.sourceEventId ?? null],
  );

  return result.rows[0];
}
export async function listWorkflowsByTaskId(
  taskId: string,
): Promise<Workflow[]> {
  const result = await getPool("WORKFLOW_DATABASE_URL").query<Workflow>(
    `
    SELECT id, task_id, event_type, message, created_by, created_at
    FROM task_workflows
    WHERE task_id = $1
    ORDER BY created_at DESC
    
    `,
    [taskId],
  );

  return result.rows;
}
