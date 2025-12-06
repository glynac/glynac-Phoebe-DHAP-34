CREATE TABLE IF NOT EXISTS email_thread_details (
    thread_id INTEGER,
    sender TEXT,
    receiver TEXT,
    timestamp TIMESTAMP,
    message TEXT
);

CREATE TABLE IF NOT EXISTS email_thread_summaries (
    thread_id INTEGER,
    subject TEXT,
    category TEXT
);
