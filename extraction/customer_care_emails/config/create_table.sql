CREATE TABLE IF NOT EXISTS public.customer_care_emails (
  email_id TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  subject TEXT NOT NULL,
  sender TEXT NOT NULL,
  receiver TEXT NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  message_body TEXT NOT NULL,
  email_types TEXT[] NOT NULL,
  email_status TEXT NOT NULL,
  email_criticality TEXT NOT NULL,
  product_types TEXT[] NOT NULL,
  agent_effectivity TEXT NULL,
  agent_efficiency TEXT NULL,
  customer_satisfaction FLOAT NULL,
  PRIMARY KEY (email_id)
);
