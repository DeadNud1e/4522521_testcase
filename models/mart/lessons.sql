WITH streams AS (
  SELECT
    H_stream.id AS stream_id,
    SAT_stream_details.name AS stream_name,
    SAT_stream_details.start_at AS stream_start_at,
    SAT_stream_details.end_at AS stream_end_at
  FROM H_stream
  JOIN SAT_stream_details ON H_stream.id = SAT_stream_details.H_stream_id
),

courses AS (
  SELECT
    H_course.id AS course_id,
    SAT_course_details.title AS course_title
  FROM H_course
  JOIN SAT_course_details ON H_course.id = SAT_course_details.H_course_id
),

stream_modules AS (
  SELECT
    H_stream_module.id AS stream_module_id,
    SAT_stream_module_details.title AS module_title
  FROM H_stream_module
  JOIN SAT_stream_module_details ON H_stream_module.id = SAT_stream_module_details.H_stream_module_id
),

stream_module_lessons AS (
  SELECT
    H_stream_module_lesson.id AS stream_module_lesson_id,
    SAT_stream_module_lesson_details.title AS lesson_title,
    SAT_stream_module_lesson_details.description AS lesson_description,
    SAT_stream_module_lesson_details.start_at AS lesson_start_at,
    SAT_stream_module_lesson_details.end_at AS lesson_end_at,
    SAT_stream_module_lesson_details.homework_url AS lesson_homework_url,
    SAT_stream_module_lesson_details.teacher_id AS lesson_teacher_id,
    SAT_stream_module_lesson_details.online_lesson_join_url AS lesson_online_lesson_join_url,
    SAT_stream_module_lesson_details.online_lesson_recording_url AS lesson_online_lesson_recording_url
  FROM H_stream_module_lesson
  JOIN SAT_stream_module_lesson_details ON H_stream_module_lesson.id = SAT_stream_module_lesson_details.H_stream_module_lesson_id
)
CREATE VIEW lessons_view AS
SELECT
  streams.stream_id,
  streams.stream_name,
  streams.stream_start_at,
  streams.stream_end_at,
  courses.course_id,
  courses.course_title,
  stream_modules.stream_module_id,
  stream_modules.module_title,
  stream_module_lessons.stream_module_lesson_id,
  stream_module_lessons.lesson_title,
  stream_module_lessons.lesson_description,
  stream_module_lessons.lesson_start_at,
  stream_module_lessons.lesson_end_at,
  stream_module_lessons.lesson_homework_url,
  stream_module_lessons.lesson_teacher_id,
  stream_module_lessons.lesson_online_lesson_join_url,
  stream_module_lessons.lesson_online_lesson_recording_url
FROM streams
JOIN L_stream_course ON streams.stream_id = L_stream_course.H_stream_id
JOIN courses ON L_stream_course.H_course_id = courses.course_id
JOIN L_stream_module ON streams.stream_id = L_stream_module.H_stream_id
JOIN stream_modules ON L_stream_module.H_stream_module_id = stream_modules.stream_module_id
JOIN L_stream_module_lesson ON stream_modules.stream_module_id = L_stream_module_lesson.H_stream_module_id
JOIN stream_module_lessons ON L_stream_module_lesson.H_stream_module_lesson_id = stream_module_lessons.stream_module_lesson_id;
