CREATE TABLE IF NOT EXISTS job_posts (
                id TEXT  primary key,
                title TEXT,
                description TEXT NOT NULL,
                via TEXT,
                role TEXT NOT NULL
)