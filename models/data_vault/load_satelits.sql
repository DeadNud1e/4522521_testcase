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

-- Load Satelits
-- Load SAT_stream_details
INSERT INTO SAT_stream_details (H_stream_id, start_at, end_at, created_at, updated_at, deleted_at, is_open, name, homework_deadline_days)
SELECT hs.id, s.start_at, s.end_at, s.created_at, s.updated_at, s.deleted_at, s.is_open, s.name, s.homework_deadline_days
FROM source.stream s
JOIN H_stream hs ON s.id = hs.stream_id
WHERE s.updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream')
ON CONFLICT (H_stream_id) DO UPDATE SET start_at = EXCLUDED.start_at, end_at = EXCLUDED.end_at, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, deleted_at = EXCLUDED.deleted_at, is_open = EXCLUDED.is_open, name = EXCLUDED.name, homework_deadline_days = EXCLUDED.homework_deadline_days;

-- Load SAT_course_details
INSERT INTO SAT_course_details (H_course_id, title, created_at, updated_at, deleted_at, icon_url, is_auto_course_enroll, is_demo_enroll)
SELECT hc.id, c.title, c.created_at, c.updated_at, c.deleted_at, c.icon_url, c.is_auto_course_enroll, c.is_demo_enroll
FROM source.course c
JOIN H_course hc ON c.id = hc.course_id
WHERE c.updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'course')
ON CONFLICT (H_course_id) DO UPDATE SET title = EXCLUDED.title, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, deleted_at = EXCLUDED.deleted_at, icon_url = EXCLUDED.icon_url, is_auto_course_enroll = EXCLUDED.is_auto_course_enroll, is_demo_enroll = EXCLUDED.is_demo_enroll;

-- Load SAT_stream_module_details
INSERT INTO SAT_stream_module_details (H_stream_module_id, title, created_at, updated_at, order_in_stream, deleted_at)
SELECT hm.id, sm.title, sm.created_at, sm.updated_at, sm.order_in_stream, sm.deleted_at
FROM source.stream_module sm
JOIN H_stream_module hm ON sm.id = hm.stream_module_id
WHERE sm.updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream_module')
ON CONFLICT (H_stream_module_id) DO UPDATE SET title = EXCLUDED.title, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, order_in_stream = EXCLUDED.order_in_stream, deleted_at = EXCLUDED.deleted_at;

-- Load SAT_stream_module_lesson_details
INSERT INTO SAT_stream_module_lesson_details (H_stream_module_lesson_id, title, description, start_at, end_at, homework_url, teacher_id, deleted_at, online_lesson_join_url, online_lesson_recording_url)
SELECT hl.id, sml.title, sml.description, sml.start_at, sml.end_at, sml.homework_url, sml.teacher_id, sml.deleted_at, sml.online_lesson_join_url, sml.online_lesson_recording_url
FROM source.stream_module_lesson sml
JOIN H_stream_module_lesson hl ON sml.id = hl.stream_module_lesson_id
WHERE sml.updated_at > (SELECT last_processed_at FROM last_processed_timestamps_cte WHERE table_name = 'stream_module_lesson')
ON CONFLICT (H_stream_module_lesson_id) DO UPDATE SET title = EXCLUDED.title, description = EXCLUDED.description, start_at = EXCLUDED.start_at, end_at = EXCLUDED.end_at, homework_url = EXCLUDED.homework_url, teacher_id = EXCLUDED.teacher_id, deleted_at = EXCLUDED.deleted_at, online_lesson_join_url = EXCLUDED.online_lesson_join_url, online_lesson_recording_url = EXCLUDED.online_lesson_recording_url;
