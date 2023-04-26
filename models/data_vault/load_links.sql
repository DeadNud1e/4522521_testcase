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

-- Load Links

-- Load L_stream_course
INSERT INTO L_stream_course (H_stream_id, H_course_id)
SELECT hs.id, hc.id
FROM source.stream s
JOIN H_stream hs ON s.id = hs.stream_id
JOIN H_course hc ON s.course_id = hc.course_id
WHERE s.updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream')
ON CONFLICT (H_stream_id, H_course_id) DO NOTHING;

-- Load L_stream_module
INSERT INTO L_stream_module (H_stream_id, H_stream_module_id)
SELECT hs.id, hm.id
FROM source.stream_module sm
JOIN H_stream hs ON sm.stream_id = hs.stream_id
JOIN H_stream_module hm ON sm.id = hm.stream_module_id
WHERE sm.updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream_module')
ON CONFLICT (H_stream_id, H_stream_module_id) DO NOTHING;

-- Load L_stream_module_lesson
INSERT INTO L_stream_module_lesson (H_stream_module_id, H_stream_module_lesson_id)
SELECT hm.id, hl.id
FROM source.stream_module_lesson sml
JOIN H_stream_module hm ON sml.stream_module_id = hm.stream_module_id
JOIN H_stream_module_lesson hl ON sml.id = hl.stream_module_lesson_id
WHERE sml.updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream_module_lesson')
ON CONFLICT (H_stream_module_id, H_stream_module_lesson_id) DO NOTHING;