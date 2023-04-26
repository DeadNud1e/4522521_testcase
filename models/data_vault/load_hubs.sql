WITH last_processed_timestamps_cte AS (
  SELECT COALESCE(last_processed_at, '1970-01-01 00:00:00'::timestamp) AS last_processed_at,
         table_name
  FROM last_processed_timestamps
  WHERE table_name = 'stream'
  UNION ALL
  SELECT COALESCE(last_processed_at, '1970-01-01 00:00:00'::timestamp) AS last_processed_at,
         table_name
  FROM last_processed_timestamps
  WHERE table_name = 'course'
  UNION ALL
  SELECT COALESCE(last_processed_at, '1970-01-01 00:00:00'::timestamp) AS last_processed_at,
         table_name
  FROM last_processed_timestamps
  WHERE table_name = 'stream_module'
  UNION ALL
  SELECT COALESCE(last_processed_at, '1970-01-01 00:00:00'::timestamp) AS last_processed_at,
         table_name
  FROM last_processed_timestamps
  WHERE table_name = 'stream_module_lesson'
)

-- Load Hubs
-- Load H_stream
INSERT INTO H_stream (stream_id)
SELECT id FROM source.stream
WHERE updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream')
ON CONFLICT (stream_id) DO NOTHING;

-- Load H_course
INSERT INTO H_course (course_id)
SELECT id FROM source.course
WHERE updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'course')
ON CONFLICT (course_id) DO NOTHING;

-- Load H_stream_module
INSERT INTO H_stream_module (stream_module_id)
SELECT id FROM source.stream_module
WHERE updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream_module')
ON CONFLICT (stream_module_id) DO NOTHING;

-- Load H_stream_module_lesson
INSERT INTO H_stream_module_lesson (stream_module_lesson_id)
SELECT id FROM source.stream_module_lesson
WHERE updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream_module_lesson')
ON CONFLICT (stream_module_lesson_id) DO NOTHING;
