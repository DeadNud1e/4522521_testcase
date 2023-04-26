-- Update last_processed_timestamps
WITH updated_timestamps_cte AS (
  SELECT 'stream' AS table_name, MAX(updated_at) AS last_processed_at FROM source.stream
  UNION ALL
  SELECT 'course' AS table_name, MAX(updated_at) AS last_processed_at FROM source.course
  UNION ALL
  SELECT 'stream_module' AS table_name, MAX(updated_at) AS last_processed_at FROM source.stream_module
  UNION ALL
  SELECT 'stream_module_lesson' AS table_name, MAX(updated_at) AS last_processed_at FROM source.stream_module_lesson
)
UPDATE last_processed_timestamps lpt
SET last_processed_at = utc.updated_timestamps_cte
FROM updated_timestamps_cte utc
WHERE lpt.table_name = utc.table_name;
