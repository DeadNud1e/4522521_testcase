-- Hubs
CREATE TABLE H_stream (
    id SERIAL PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    UNIQUE (stream_id)
);

CREATE TABLE H_course (
    id SERIAL PRIMARY KEY,
    course_id INTEGER NOT NULL,
    UNIQUE (course_id)
);

CREATE TABLE H_stream_module (
    id SERIAL PRIMARY KEY,
    stream_module_id INTEGER NOT NULL,
    UNIQUE (stream_module_id)
);

CREATE TABLE H_stream_module_lesson (
    id SERIAL PRIMARY KEY,
    stream_module_lesson_id INTEGER NOT NULL,
    UNIQUE (stream_module_lesson_id)
);

-- Links
CREATE TABLE L_stream_course (
    id SERIAL PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES H_stream (stream_id),
    FOREIGN KEY (course_id) REFERENCES H_course (course_id),
    UNIQUE (stream_id, course_id)
);

CREATE TABLE L_stream_stream_module (
    id SERIAL PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    stream_module_id INTEGER NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES H_stream (stream_id),
    FOREIGN KEY (stream_module_id) REFERENCES H_stream_module (stream_module_id),
    UNIQUE (stream_id, stream_module_id)
);

CREATE TABLE L_stream_module_stream_module_lesson (
    id SERIAL PRIMARY KEY,
    stream_module_id INTEGER NOT NULL,
    stream_module_lesson_id INTEGER NOT NULL,
    FOREIGN KEY (stream_module_id) REFERENCES H_stream_module (stream_module_id),
    FOREIGN KEY (stream_module_lesson_id) REFERENCES H_stream_module_lesson (stream_module_lesson_id),
    UNIQUE (stream_module_id, stream_module_lesson_id)
);

-- Satellites
CREATE TABLE S_stream (
    id SERIAL PRIMARY KEY,
    H_stream_id INTEGER NOT NULL,
    name VARCHAR(255),
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    is_open BOOLEAN,
    homework_deadline_days INTEGER,
    FOREIGN KEY (H_stream_id) REFERENCES H_stream (id),
    UNIQUE (H_stream_id)
);

CREATE TABLE S_course (
    id SERIAL PRIMARY KEY,
    H_course_id INTEGER NOT NULL,
    title VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP(0),
    icon_url VARCHAR(255),
    is_auto_course_enroll BOOLEAN,
    is_demo_enroll BOOLEAN,
    FOREIGN KEY (H_course_id) REFERENCES H_course (id),
    UNIQUE (H_course_id)
);

CREATE TABLE S_stream_module (
    id SERIAL PRIMARY KEY,
    H_stream_module_id INTEGER NOT NULL,
    title VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    order_in_stream INTEGER,
    deleted_at TIMESTAMP,
    FOREIGN KEY (H_stream_module_id) REFERENCES H_stream_module (id),
    UNIQUE (H_stream_module_id)
);

CREATE TABLE S_stream_module_lesson (
    id SERIAL PRIMARY KEY,
    H_stream_module_lesson_id INTEGER NOT NULL,
    title VARCHAR(255),
    description TEXT,
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    homework_url VARCHAR(500),
    teacher_id INTEGER,
    deleted_at TIMESTAMP(0),
    online_lesson_join_url VARCHAR(255),
    online_lesson_recording_url VARCHAR(255),
    FOREIGN KEY (H_stream_module_lesson_id) REFERENCES H_stream_module_lesson (id),
    UNIQUE (H_stream_module_lesson_id)
);


CREATE TABLE last_processed_timestamps (
    table_name VARCHAR(255) PRIMARY KEY,
    last_processed_at TIMESTAMP
);
