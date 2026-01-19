CREATE TABLE IF NOT EXISTS public.email_thread_summary (
    thread_id INTEGER NOT NULL,
    summary TEXT NULL,
    PRIMARY KEY (thread_id)
);
