ALTER TABLE task_workflows ADD COLUMN IF NOT EXISTS source_event_id UUID;
CREATE UNIQUE INDEX IF NOT EXISTS idx_task_workflows_source_event ON task_workflows (source_event_id) WHERE source_event_id IS NOT NULL;
