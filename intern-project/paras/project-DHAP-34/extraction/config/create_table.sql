CREATE TABLE IF NOT EXISTS public.customer_care_emails (
    subject TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    message_body TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    email_types JSONB NULL,
    email_status TEXT NOT NULL,
    email_criticality TEXT NOT NULL,
    product_types JSONB NULL,
    agent_effectivity TEXT NULL,
    agent_efficiency TEXT NULL,
    customer_satisfaction NUMERIC NULL,
    PRIMARY KEY (thread_id)
);
