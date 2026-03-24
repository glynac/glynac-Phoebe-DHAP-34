CREATE TABLE IF NOT EXISTS public.customer_care_emails (
    subject TEXT,
    sender TEXT,
    receiver TEXT,
    timestamp TIMESTAMP,
    message_body TEXT,
    thread_id TEXT,
    email_types TEXT,
    email_status TEXT,
    email_criticality TEXT,
    product_types TEXT,
    agent_effectivity TEXT,
    agent_efficiency TEXT,
    customer_satisfaction TEXT
);